import asyncio
import logging
import json
import time
from datetime import datetime
from typing import List, Dict, Set, Optional

from config import SUPABASE_URL, SUPABASE_KEY, CATEGORIES, SOURCE, EMBEDDING_MODEL
from src.scraper.category_scraper import CategoryScraper
from src.scraper.product_scraper import ProductScraper
from src.embeddings.embedding_generator import SigLIPEmbeddingGenerator
from src.database.supabase_client import SupabaseClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SmartScraper:
    def __init__(self, max_concurrent: int = 5, batch_size: int = 50):
        self.category_scraper = None
        self.product_scrapers: List[ProductScraper] = []
        self.embedding_generator = None
        self.supabase_client = None
        self.max_concurrent = max_concurrent
        self.batch_size = batch_size
        
        self.stats = {
            'new': 0,
            'updated': 0,
            'unchanged': 0,
            'deleted': 0,
            'errors': 0
        }

    async def initialize(self):
        logger.info("Initializing scraper components...")
        
        self.category_scraper = CategoryScraper()
        await self.category_scraper.init_browser()
        
        logger.info(f"Creating {self.max_concurrent} product scrapers for parallel processing...")
        for i in range(self.max_concurrent):
            scraper = ProductScraper()
            await scraper.init_browser()
            self.product_scrapers.append(scraper)
        
        self.embedding_generator = SigLIPEmbeddingGenerator(EMBEDDING_MODEL)
        
        self.supabase_client = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        
        if not self.supabase_client.check_connection():
            raise Exception("Failed to connect to Supabase")
        
        logger.info("All components initialized successfully")

    async def cleanup(self):
        logger.info("Cleaning up resources...")
        if self.category_scraper:
            await self.category_scraper.close()
        for scraper in self.product_scrapers:
            await scraper.close()

    async def scrape_product_worker(self, url: str, scraper: ProductScraper, index: int, total: int) -> dict:
        try:
            product = await scraper.scrape_product(url)
            if index % 20 == 0:
                logger.info(f"Scraped product {index}/{total}")
            return product
        except Exception as e:
            logger.error(f"Error scraping {url}: {e}")
            return None

    async def scrape_products_parallel(self, urls: List[str]) -> List[dict]:
        products = []
        total = len(urls)
        
        for i in range(0, total, self.max_concurrent):
            batch = urls[i:i + self.max_concurrent]
            scrapers = self.product_scrapers[:len(batch)]
            
            tasks = [
                self.scrape_product_worker(url, scraper, i + j + 1, total)
                for j, (url, scraper) in enumerate(zip(batch, scrapers))
            ]
            
            results = await asyncio.gather(*tasks)
            products.extend([r for r in results if r])
            
            logger.info(f"Progress: {min(i + self.max_concurrent, total)}/{total} products scraped")
        
        return products

    def get_existing_products(self, products: List[dict]) -> Dict[str, dict]:
        logger.info("Fetching existing products from database...")
        existing = {}
        
        product_urls = [p.get('product_url') for p in products if p.get('product_url')]
        
        for url in product_urls:
            product = self.supabase_client.get_product_by_url(url)
            if product:
                existing[url] = product
        
        logger.info(f"Found {len(existing)} existing products")
        return existing

    def has_product_changed(self, scraped: dict, existing: dict) -> bool:
        fields_to_check = ['title', 'price', 'sale', 'image_url', 'additional_images', 'description', 'sizes']
        
        for field in fields_to_check:
            scraped_val = scraped.get(field)
            existing_val = existing.get(field)
            
            if str(scraped_val) != str(existing_val):
                return True
        
        return False

    def should_regenerate_embeddings(self, scraped: dict, existing: dict) -> bool:
        if not existing:
            return True
        
        scraped_image = scraped.get('image_url', '')
        existing_image = existing.get('image_url', '')
        
        return scraped_image != existing_image

    def generate_embeddings_staggered(self, products: List[dict]) -> List[dict]:
        logger.info(f"Generating embeddings for {len(products)} products (with 0.5s stagger)...")
        
        for i, product in enumerate(products):
            info_emb = self.embedding_generator.generate_info_embedding(product)
            product['info_embedding'] = info_emb
            
            if product.get('image_url'):
                img_emb = self.embedding_generator.generate_image_embedding(product['image_url'])
                product['image_embedding'] = img_emb
            
            if i % 50 == 0:
                logger.info(f"Embedding progress: {i+1}/{len(products)}")
            
            time.sleep(0.5)
        
        logger.info(f"Completed embeddings for {len(products)} products")
        return products

    def insert_products_batch(self, products: List[dict]) -> int:
        logger.info(f"Inserting {len(products)} products into database (batch size: {self.batch_size})...")
        
        total_inserted = 0
        for i in range(0, len(products), self.batch_size):
            batch = products[i:i + self.batch_size]
            
            success = self.supabase_client.upsert_products_batch(batch, self.stats)
            total_inserted += success if success else 0
            
            logger.info(f"Inserted batch {i//self.batch_size + 1}/{(len(products) + self.batch_size - 1) // self.batch_size}")
        
        return total_inserted

    def cleanup_stale_products(self, current_urls: Set[str]) -> int:
        logger.info("Cleaning up stale products...")
        
        all_products = self.supabase_client.get_all_products_by_source()
        
        stale_count = 0
        for product in all_products:
            product_url = product.get('product_url')
            
            if product_url not in current_urls:
                stale_count += 1
                self.supabase_client.mark_product_seen(product_url)
        
        two_run_stale = self.supabase_client.get_products_not_seen_in_runs(2)
        
        for product in two_run_stale:
            self.supabase_client.delete_product(product['id'])
            stale_count += 1
        
        logger.info(f"Deleted {stale_count} stale products")
        return stale_count

    def print_summary(self):
        logger.info("=" * 60)
        logger.info("SCRAPER RUN SUMMARY")
        logger.info("=" * 60)
        logger.info(f"  New products added:    {self.stats['new']}")
        logger.info(f"  Products updated:     {self.stats['updated']}")
        logger.info(f"  Products unchanged:   {self.stats['unchanged']}")
        logger.info(f"  Stale products deleted: {self.stats['deleted']}")
        logger.info(f"  Errors:               {self.stats['errors']}")
        logger.info("=" * 60)

    async def run(self):
        try:
            await self.initialize()
            
            logger.info("=" * 60)
            logger.info("Starting Footshop Scraper (SMART MODE)")
            logger.info("=" * 60)
            
            logger.info(f"Categories to scrape: {CATEGORIES}")
            
            logger.info("STEP 1: Collecting all product URLs from categories...")
            all_product_urls = []
            
            for category_url in CATEGORIES:
                logger.info(f"Scraping category: {category_url}")
                
                total_pages = await self.category_scraper.get_total_pages(category_url)
                logger.info(f"Category {category_url} has {total_pages} pages")
                
                for page_num in range(1, total_pages + 1):
                    if page_num == 1:
                        page_url = category_url
                    else:
                        page_url = f"{category_url}/page-{page_num}"
                    
                    logger.info(f"Scraping page {page_num}/{total_pages}: {page_url}")
                    
                    try:
                        urls = await self.category_scraper.get_product_urls_from_page(page_url)
                        if not urls:
                            logger.info(f"No products found on page {page_num}, stopping")
                            break
                        all_product_urls.extend(urls)
                        logger.info(f"Found {len(urls)} products on page {page_num}")
                    except Exception as e:
                        logger.error(f"Error scraping page {page_num}: {e}")
                        break
                
                logger.info(f"Found {len(all_product_urls)} products in {category_url}")
            
            unique_urls = set(all_product_urls)
            logger.info(f"Total unique product URLs: {len(unique_urls)}")
            
            with open('product_urls.json', 'w') as f:
                json.dump(list(unique_urls), f)
            
            logger.info("STEP 2: Scraping product details (parallel)...")
            products = await self.scrape_products_parallel(list(unique_urls))
            logger.info(f"Successfully scraped {len(products)} products")
            
            with open('scraped_products.json', 'w') as f:
                json.dump(products, f, indent=2)
            
            logger.info("STEP 3: Smart embedding generation...")
            existing_products = self.get_existing_products(products)
            
            products_to_embed = []
            for product in products:
                url = product.get('product_url')
                existing = existing_products.get(url)
                
                if not existing:
                    products_to_embed.append(product)
                elif self.should_regenerate_embeddings(product, existing):
                    products_to_embed.append(product)
                else:
                    product['info_embedding'] = existing.get('info_embedding')
                    product['image_embedding'] = existing.get('image_embedding')
                    self.stats['unchanged'] += 1
            
            logger.info(f"Products needing new embeddings: {len(products_to_embed)}")
            logger.info(f"Products with unchanged embeddings: {self.stats['unchanged']}")
            
            products_to_embed = self.generate_embeddings_staggered(products_to_embed)
            
            for product in products_to_embed:
                url = product.get('product_url')
                existing = existing_products.get(url)
                if existing:
                    self.stats['updated'] += 1
                else:
                    self.stats['new'] += 1
            
            for p in products_to_embed:
                for i, product in enumerate(products):
                    if product.get('product_url') == p.get('product_url'):
                        products[i] = p
                        break
            
            with open('products_with_embeddings.json', 'w') as f:
                json.dump(products, f, indent=2)
            
            logger.info("STEP 4: Inserting products into Supabase (batch)...")
            self.insert_products_batch(products)
            
            logger.info("STEP 5: Cleaning up stale products...")
            deleted = self.cleanup_stale_products(unique_urls)
            self.stats['deleted'] = deleted
            
            self.print_summary()
            
        except Exception as e:
            logger.error(f"Scraper failed: {e}")
            raise
        finally:
            await self.cleanup()


async def main():
    scraper = SmartScraper(max_concurrent=5, batch_size=50)
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())

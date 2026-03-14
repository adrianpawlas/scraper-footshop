import asyncio
import logging
import json
from datetime import datetime

from config import SUPABASE_URL, SUPABASE_KEY, CATEGORIES, SOURCE, BRAND, SECOND_HAND, EMBEDDING_MODEL
from src.scraper.category_scraper import CategoryScraper
from src.scraper.product_scraper import ProductScraper
from src.embeddings.embedding_generator import SigLIPEmbeddingGenerator
from src.database.supabase_client import SupabaseClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class FootshopScraper:
    def __init__(self):
        self.category_scraper = None
        self.product_scraper = None
        self.embedding_generator = None
        self.supabase_client = None
        
    async def initialize(self):
        logger.info("Initializing scraper components...")
        
        self.category_scraper = CategoryScraper()
        await self.category_scraper.init_browser()
        
        self.product_scraper = ProductScraper()
        await self.product_scraper.init_browser()
        
        self.embedding_generator = SigLIPEmbeddingGenerator(EMBEDDING_MODEL)
        
        self.supabase_client = SupabaseClient(SUPABASE_URL, SUPABASE_KEY)
        
        if not self.supabase_client.check_connection():
            raise Exception("Failed to connect to Supabase")
        
        logger.info("All components initialized successfully")

    async def cleanup(self):
        logger.info("Cleaning up resources...")
        if self.category_scraper:
            await self.category_scraper.close()
        if self.product_scraper:
            await self.product_scraper.close()

    async def run(self):
        try:
            await self.initialize()
            
            logger.info("=" * 50)
            logger.info("Starting Footshop scraper")
            logger.info("=" * 50)
            
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
            
            unique_urls = list(set(all_product_urls))
            logger.info(f"Total unique product URLs: {len(unique_urls)}")
            
            with open('product_urls.json', 'w') as f:
                json.dump(unique_urls, f)
            
            logger.info("STEP 2: Scraping product details...")
            products = []
            for i, url in enumerate(unique_urls):
                logger.info(f"Scraping product {i+1}/{len(unique_urls)}: {url}")
                product = await self.product_scraper.scrape_product(url)
                if product:
                    products.append(product)
                await asyncio.sleep(0.3)
            
            logger.info(f"Successfully scraped {len(products)} products")
            
            with open('scraped_products.json', 'w') as f:
                json.dump(products, f, indent=2)
            
            logger.info("STEP 3: Generating embeddings...")
            for i, product in enumerate(products):
                logger.info(f"Generating embeddings for product {i+1}/{len(products)}")
                
                info_emb = self.embedding_generator.generate_info_embedding(product)
                product['info_embedding'] = info_emb
                
                if product.get('image_url'):
                    img_emb = self.embedding_generator.generate_image_embedding(product['image_url'])
                    product['image_embedding'] = img_emb
                
                if i % 10 == 0:
                    logger.info(f"Progress: {i+1}/{len(products)} products processed")
            
            with open('products_with_embeddings.json', 'w') as f:
                json.dump(products, f, indent=2)
            
            logger.info("STEP 4: Inserting products into Supabase...")
            success_count = 0
            for i, product in enumerate(products):
                logger.info(f"Inserting product {i+1}/{len(products)}")
                if self.supabase_client.insert_product(product):
                    success_count += 1
            
            logger.info("=" * 50)
            logger.info(f"Scraping completed successfully!")
            logger.info(f"Total products scraped: {len(products)}")
            logger.info(f"Products with embeddings: {len(products)}")
            logger.info(f"Products inserted: {success_count}")
            logger.info("=" * 50)
            
        except Exception as e:
            logger.error(f"Scraper failed: {e}")
            raise
        finally:
            await self.cleanup()


async def main():
    scraper = FootshopScraper()
    await scraper.run()


if __name__ == "__main__":
    asyncio.run(main())

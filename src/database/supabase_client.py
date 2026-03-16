from supabase import create_client, Client
from typing import Dict, List, Optional
import logging
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class SupabaseClient:
    def __init__(self, url: str, key: str):
        self.client: Client = create_client(url, key)
        logger.info(f"Supabase client initialized for {url}")

    def check_connection(self) -> bool:
        try:
            self.client.table("products").select("id").limit(1).execute()
            return True
        except Exception:
            return False

    def get_product_by_url(self, product_url: str) -> Optional[Dict]:
        try:
            result = self.client.table("products").select("*").eq("product_url", product_url).execute()
            if result.data and len(result.data) > 0:
                return result.data[0]
            return None
        except Exception as e:
            logger.error(f"Error getting product by URL: {e}")
            return None

    def get_all_products_by_source(self, source: str = "scraper-footshop") -> List[Dict]:
        try:
            result = self.client.table("products").select("*").eq("source", source).execute()
            return result.data if result.data else []
        except Exception as e:
            logger.error(f"Error getting products by source: {e}")
            return []

    def mark_product_seen(self, product_url: str) -> bool:
        try:
            product = self.get_product_by_url(product_url)
            current_run = 1
            if product:
                current_run = (product.get('run_count', 0) % 2) + 1
            
            self.client.table("products").update({
                "run_count": current_run,
                "last_seen": datetime.now().isoformat()
            }).eq("product_url", product_url).execute()
            return True
        except Exception as e:
            logger.error(f"Error marking product seen: {e}")
            return False

    def get_products_not_seen_in_runs(self, runs: int = 2) -> List[Dict]:
        try:
            result = self.client.table("products").select("*").eq("source", "scraper-footshop").execute()
            stale = []
            for p in result.data:
                run_count = p.get('run_count', 0)
                if run_count < runs:
                    stale.append(p)
            return stale
        except Exception as e:
            logger.error(f"Error getting stale products: {e}")
            return []

    def delete_product(self, product_id: str) -> bool:
        try:
            self.client.table("products").delete().eq("id", product_id).execute()
            logger.info(f"Deleted stale product: {product_id}")
            return True
        except Exception as e:
            logger.error(f"Error deleting product: {e}")
            return False

    def insert_product(self, product_data: Dict) -> bool:
        try:
            data = {
                "id": product_data.get("id"),
                "source": product_data.get("source", "scraper-footshop"),
                "product_url": product_data.get("product_url"),
                "brand": product_data.get("brand", "Footshop"),
                "title": product_data.get("title", "Unknown"),
                "description": product_data.get("description"),
                "category": product_data.get("category"),
                "gender": product_data.get("gender"),
                "image_url": product_data.get("image_url", ""),
                "price": product_data.get("price"),
                "sale": product_data.get("sale"),
                "metadata": product_data.get("metadata"),
                "size": product_data.get("sizes"),
                "second_hand": False,
                "country": product_data.get("country"),
                "additional_images": product_data.get("additional_images"),
                "image_embedding": product_data.get("image_embedding"),
                "info_embedding": product_data.get("info_embedding"),
                "updated_at": datetime.now().isoformat(),
                "run_count": 1,
                "last_seen": datetime.now().isoformat(),
            }
            
            result = self.client.table("products").upsert(data, on_conflict="source,product_url").execute()
            
            if result.data:
                return True
            return False
                
        except Exception as e:
            logger.error(f"Error inserting product {product_data.get('title')}: {e}")
            return False

    def upsert_products_batch(self, products: List[Dict], stats: Dict = None) -> int:
        if not products:
            return 0
        
        success_count = 0
        max_retries = 3
        
        for attempt in range(max_retries):
            try:
                records = []
                for product in products:
                    record = {
                        "id": product.get("id"),
                        "source": product.get("source", "scraper-footshop"),
                        "product_url": product.get("product_url"),
                        "brand": product.get("brand", "Footshop"),
                        "title": product.get("title", "Unknown"),
                        "description": product.get("description"),
                        "category": product.get("category"),
                        "gender": product.get("gender"),
                        "image_url": product.get("image_url", ""),
                        "price": product.get("price"),
                        "sale": product.get("sale"),
                        "metadata": product.get("metadata"),
                        "size": product.get("sizes"),
                        "second_hand": False,
                        "country": product.get("country"),
                        "additional_images": product.get("additional_images"),
                        "image_embedding": product.get("image_embedding"),
                        "info_embedding": product.get("info_embedding"),
                        "updated_at": datetime.now().isoformat(),
                        "run_count": 1,
                        "last_seen": datetime.now().isoformat(),
                    }
                    records.append(record)
                
                result = self.client.table("products").upsert(records, on_conflict="source,product_url").execute()
                
                if result.data:
                    success_count = len(records)
                    logger.info(f"Successfully upserted batch of {len(records)} products")
                    return success_count
                    
            except Exception as e:
                logger.error(f"Batch insert attempt {attempt + 1} failed: {e}")
                if attempt == max_retries - 1:
                    self._log_failed_products(products, e)
                    if stats:
                        stats['errors'] += len(products)
        
        return success_count

    def _log_failed_products(self, products: List[Dict], error: Exception):
        try:
            failed_log = {
                "timestamp": datetime.now().isoformat(),
                "error": str(error),
                "products": [{"id": p.get("id"), "url": p.get("product_url")} for p in products]
            }
            with open("failed_products.json", "a") as f:
                f.write(json.dumps(failed_log) + "\n")
            logger.info(f"Logged {len(products)} failed products to failed_products.json")
        except Exception as e:
            logger.error(f"Failed to log error: {e}")

    def get_product_count(self) -> int:
        try:
            result = self.client.table("products").select("id", count="exact").execute()
            return result.count or 0
        except Exception as e:
            logger.error(f"Error getting product count: {e}")
            return 0

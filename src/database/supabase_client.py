from supabase import create_client, Client
from typing import Dict, List, Optional
import logging
import json

logger = logging.getLogger(__name__)


class SupabaseClient:
    def __init__(self, url: str, key: str):
        self.client: Client = create_client(url, key)
        logger.info(f"Supabase client initialized for {url}")

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
            }
            
            result = self.client.table("products").upsert(data).execute()
            
            if result.data:
                logger.info(f"Successfully inserted/updated product: {product_data.get('title')}")
                return True
            else:
                logger.warning(f"No data returned for product: {product_data.get('title')}")
                return False
                
        except Exception as e:
            logger.error(f"Error inserting product {product_data.get('title')}: {e}")
            return False

    def insert_products_batch(self, products: List[Dict], batch_size: int = 10) -> int:
        success_count = 0
        
        for i in range(0, len(products), batch_size):
            batch = products[i:i + batch_size]
            
            try:
                records = []
                for product in batch:
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
                        "country": product.get("country", "Belgium"),
                        "additional_images": product.get("additional_images"),
                        "image_embedding": product.get("image_embedding"),
                        "info_embedding": product.get("info_embedding"),
                    }
                    records.append(record)
                
                result = self.client.table("products").upsert(records).execute()
                
                success_count += len(records)
                logger.info(f"Inserted batch {i//batch_size + 1}: {len(records)} products")
                
            except Exception as e:
                logger.error(f"Error inserting batch: {e}")
                for product in batch:
                    self.insert_product(product)
                    success_count += 1
        
        return success_count

    def check_connection(self) -> bool:
        try:
            result = self.client.table("products").select("id").limit(1).execute()
            logger.info("Supabase connection successful")
            return True
        except Exception as e:
            logger.error(f"Supabase connection failed: {e}")
            return False

    def get_product_count(self) -> int:
        try:
            result = self.client.table("products").select("id", count="exact").execute()
            return result.count or 0
        except Exception as e:
            logger.error(f"Error getting product count: {e}")
            return 0

    def product_exists(self, product_id: str) -> bool:
        try:
            result = self.client.table("products").select("id").eq("id", product_id).execute()
            return len(result.data) > 0
        except Exception:
            return False

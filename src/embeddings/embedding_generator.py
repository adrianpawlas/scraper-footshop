import torch
from transformers import AutoModel, AutoProcessor
from PIL import Image
import requests
from io import BytesIO
import numpy as np
from typing import Optional, List, Union
import logging
import hashlib

logger = logging.getLogger(__name__)


class SigLIPEmbeddingGenerator:
    def __init__(self, model_name: str = "google/siglip-base-patch16-384"):
        self.model_name = model_name
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        logger.info(f"Loading SigLIP model: {model_name} on {self.device}")
        
        self.model = AutoModel.from_pretrained(model_name)
        self.processor = AutoProcessor.from_pretrained(model_name)
        self.model.to(self.device)
        self.model.eval()
        
        logger.info(f"SigLIP model loaded successfully. Embedding dimension: 768")

    def _download_image(self, url: str) -> Optional[Image.Image]:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            img = Image.open(BytesIO(response.content)).convert('RGB')
            return img
        except Exception as e:
            logger.warning(f"Failed to download image from {url}: {e}")
            return None

    def generate_image_embedding(self, image_url: str) -> Optional[List[float]]:
        try:
            img = self._download_image(image_url)
            if img is None:
                return None
            
            inputs = self.processor(images=img, return_tensors="pt")
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            with torch.no_grad():
                outputs = self.model.get_image_features(**inputs)
            
            embedding = outputs.pooler_output.cpu().numpy().flatten().tolist()
            
            return embedding
            
        except Exception as e:
            logger.error(f"Failed to generate image embedding for {image_url}: {e}")
            return None

    def generate_text_embedding(self, text: str) -> Optional[List[float]]:
        try:
            inputs = self.processor(text=text, return_tensors="pt")
            inputs = {k: v.to(self.device) for k, v in inputs.items()}
            
            with torch.no_grad():
                outputs = self.model.get_text_features(**inputs)
            
            embedding = outputs.pooler_output.cpu().numpy().flatten().tolist()
            
            return embedding
            
        except Exception as e:
            logger.error(f"Failed to generate text embedding: {e}")
            return None

    def prepare_product_text_for_embedding(self, product_data: dict) -> str:
        text_parts = []
        
        if product_data.get('title'):
            text_parts.append(f"Title: {product_data['title']}")
        
        if product_data.get('brand'):
            text_parts.append(f"Brand: {product_data['brand']}")
        
        if product_data.get('description'):
            text_parts.append(f"Description: {product_data['description']}")
        
        if product_data.get('category'):
            text_parts.append(f"Category: {product_data['category']}")
        
        if product_data.get('gender'):
            text_parts.append(f"Gender: {product_data['gender']}")
        
        if product_data.get('price'):
            text_parts.append(f"Price: {product_data['price']}")
        
        if product_data.get('sizes'):
            text_parts.append(f"Sizes: {product_data['sizes']}")
        
        if product_data.get('metadata'):
            import json
            try:
                metadata = json.loads(product_data['metadata'])
                if metadata.get('colors'):
                    text_parts.append(f"Colors: {', '.join(metadata['colors'])}")
                if metadata.get('breadcrumbs'):
                    text_parts.append(f"Path: {metadata['breadcrumbs']}")
            except:
                pass
        
        return " | ".join(text_parts)

    def generate_info_embedding(self, product_data: dict) -> Optional[List[float]]:
        text = self.prepare_product_text_for_embedding(product_data)
        return self.generate_text_embedding(text)

    def batch_process_products(self, products: List[dict], batch_size: int = 10) -> List[dict]:
        results = []
        
        for i, product in enumerate(products):
            logger.info(f"Generating embeddings for product {i+1}/{len(products)}: {product.get('title', 'Unknown')}")
            
            image_embedding = None
            if product.get('image_url'):
                image_embedding = self.generate_image_embedding(product['image_url'])
            
            info_embedding = self.generate_info_embedding(product)
            
            product['image_embedding'] = image_embedding
            product['info_embedding'] = info_embedding
            
            results.append(product)
        
        return results

    def get_embedding_dimension(self) -> int:
        return 768

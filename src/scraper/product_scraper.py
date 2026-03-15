import asyncio
import re
import json
from playwright.async_api import async_playwright, Page, Browser
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import Dict, Optional, List
import logging

logger = logging.getLogger(__name__)


class ProductScraper:
    def __init__(self):
        self.browser: Optional[Browser] = None
        self.page: Optional[Page] = None
        self.playwright = None

    async def init_browser(self):
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=True,
            args=['--no-sandbox', '--disable-setuid-sandbox']
        )
        context = await self.browser.new_context(
            user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
        )
        self.page = await context.new_page()

    async def close(self):
        if self.browser:
            await self.browser.close()
        if self.playwright:
            await self.playwright.stop()

    def _extract_product_id(self, url: str) -> str:
        match = re.search(r'/(\d+)-', url)
        if match:
            return match.group(1)
        return url

    def _determine_gender(self, url: str, category: str) -> str:
        url_lower = url.lower()
        category_lower = category.lower()
        
        if '/men' in url_lower or 'mens' in url_lower or 'man' in category_lower:
            return "man"
        elif '/women' in url_lower or 'womens' in url_lower or 'woman' in category_lower:
            return "woman"
        
        return "unknown"

    def _parse_prices(self, price_text: str, sale_text: Optional[str] = None) -> tuple:
        prices = {}
        
        price_pattern = r'([\d\s.,]+)\s*(USD|EUR|CZK|PLN|GBP|SEK|NOK|DKK|HUF|RON|BGN|CHF)?'
        matches = re.findall(price_pattern, price_text.upper())
        
        for amount, currency in matches:
            amount = amount.strip().replace(' ', '').replace(',', '.')
            if currency:
                prices[currency] = f"{amount}{currency}"
            elif amount:
                prices['EUR'] = f"{amount}EUR"
        
        sale_prices = {}
        if sale_text:
            sale_matches = re.findall(price_pattern, sale_text.upper())
            for amount, currency in sale_matches:
                amount = amount.strip().replace(' ', '').replace(',', '.')
                if currency:
                    sale_prices[currency] = f"{amount}{currency}"
                elif amount:
                    sale_prices['EUR'] = f"{amount}EUR"
        
        return prices, sale_prices

    def _format_price_string(self, prices: Dict[str, str]) -> str:
        if not prices:
            return ""
        return ", ".join(prices.values())

    def _parse_category(self, breadcrumb: str) -> str:
        categories = []
        
        parts = [p.strip() for p in breadcrumb.split('/') if p.strip()]
        
        exclude_words = ['home', 'shoes', 'men', 'women', 'sale', 'new']
        
        for part in parts:
            part_lower = part.lower()
            if part_lower not in exclude_words and not part_lower.isdigit():
                if any(word in part_lower for word in ['shoe', 'sneaker', 'boot', 'sandal', 'heel', 'flat', 'loafer', 'slide', 'sleeper', 'pant', 'shirt', 'jacket', 'dress', 'skirt', 'hoodie', 'sweater', 'cap', 'bag', 'accessories']):
                    categories.append(part)
        
        return ", ".join(categories) if categories else "Shoes"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def scrape_product(self, url: str, category_hint: str = "") -> Optional[Dict]:
        try:
            await self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
            
            await self.page.wait_for_timeout(2000)
            
            product_id = self._extract_product_id(url)
            
            title = await self._get_title()
            
            brand = await self._get_brand()
            
            description = await self._get_description()
            
            image_url = await self._get_main_image()
            
            additional_images = await self._get_additional_images()
            
            price_text, sale_text = await self._get_prices()
            
            prices, sale_prices = self._parse_prices(price_text, sale_text)
            
            price_str = self._format_price_string(prices)
            sale_str = self._format_price_string(sale_prices) if sale_prices else price_str
            
            sizes = await self._get_sizes()
            
            colors = await self._get_colors()
            
            breadcrumbs = await self._get_breadcrumbs()
            category = self._parse_category(breadcrumbs)
            
            gender = self._determine_gender(url, category)
            
            metadata = {
                "title": title,
                "brand": brand,
                "description": description,
                "sizes": sizes,
                "colors": colors,
                "breadcrumbs": breadcrumbs,
                "url": url
            }
            
            product_data = {
                "id": product_id,
                "product_url": url,
                "title": title,
                "brand": "Footshop",
                "description": description,
                "image_url": image_url,
                "additional_images": additional_images,
                "price": price_str,
                "sale": sale_str,
                "category": category,
                "gender": gender,
                "metadata": json.dumps(metadata),
                "sizes": ", ".join(sizes) if sizes else None,
                "source": "scraper-footshop",
                "country": "Belgium"
            }
            
            return product_data
            
        except Exception as e:
            logger.error(f"Error scraping product {url}: {e}")
            return None

    async def _get_title(self) -> str:
        try:
            title_elem = await self.page.query_selector('h1')
            if title_elem:
                return await title_elem.inner_text()
        except Exception:
            pass
        return "Unknown"

    async def _get_brand(self) -> str:
        try:
            title = await self._get_title()
            if title:
                parts = title.split()
                if parts:
                    return parts[0].capitalize()
            
            brand_elem = await self.page.query_selector('[itemprop="brand"]')
            if brand_elem:
                brand_text = await brand_elem.inner_text()
                if brand_text:
                    return brand_text
            
            brand_elem = await self.page.query_selector('.product-brand a')
            if brand_elem:
                return await brand_elem.inner_text()
            brand_elem = await self.page.query_selector('.product-manufacturer a')
            if brand_elem:
                return await brand_elem.inner_text()
        except Exception:
            pass
        return "Footshop"

    async def _get_description(self) -> str:
        try:
            desc_elem = await self.page.query_selector('.product-description')
            if desc_elem:
                return await desc_elem.inner_text()
        except Exception:
            pass
        return ""

    async def _get_main_image(self) -> str:
        try:
            img_elem = await self.page.query_selector('[itemprop="image"]')
            if img_elem:
                src = await img_elem.get_attribute('src')
                if src:
                    return src
                data_src = await img_elem.get_attribute('data-src')
                if data_src:
                    return data_src
            img_elem = await self.page.query_selector('.product-image img')
            if img_elem:
                src = await img_elem.get_attribute('src')
                if src:
                    return src
        except Exception:
            pass
        return ""

    async def _get_additional_images(self) -> str:
        try:
            thumbs = await self.page.query_selector_all('.product-images-thumbs .thumb-img')
            urls = []
            for thumb in thumbs:
                img = await thumb.query_selector('img')
                if img:
                    src = await img.get_attribute('src')
                    if src:
                        urls.append(src)
                    else:
                        data_src = await img.get_attribute('data-src')
                        if data_src:
                            urls.append(data_src)
            return " , ".join(urls)
        except Exception:
            pass
        return ""

    async def _get_prices(self) -> tuple:
        price_text = ""
        sale_text = ""
        
        try:
            price_elem = await self.page.query_selector('[itemprop="price"]')
            if price_elem:
                price_text = await price_elem.get_attribute('content') or ""
                if not price_text:
                    price_text = await price_elem.inner_text()
        except Exception:
            pass
        
        try:
            sale_elem = await self.page.query_selector('.sale-price')
            if sale_elem:
                sale_text = await sale_elem.inner_text()
        except Exception:
            pass
        
        return price_text, sale_text

    async def _get_sizes(self) -> List[str]:
        sizes = []
        try:
            size_elements = await self.page.query_selector_all('.product-variants .size-option, .product-sizes .size')
            for size in size_elements:
                size_text = await size.inner_text()
                disabled = await size.get_attribute('disabled')
                if disabled is None:
                    sizes.append(size_text.strip())
        except Exception:
            pass
        return sizes

    async def _get_colors(self) -> List[str]:
        colors = []
        try:
            color_elements = await self.page.query_selector_all('.product-colors .color-option, .color-picker .color')
            for color in color_elements:
                color_text = await color.get_attribute('title') or await color.inner_text()
                if color_text:
                    colors.append(color_text.strip())
        except Exception:
            pass
        return colors

    async def _get_breadcrumbs(self) -> str:
        try:
            breadcrumbs = await self.page.query_selector_all('.breadcrumb a, .breadcrumb li')
            breadcrumb_text = []
            for crumb in breadcrumbs:
                text = await crumb.inner_text()
                if text and text.strip():
                    breadcrumb_text.append(text.strip())
            return " > ".join(breadcrumb_text)
        except Exception:
            pass
        return ""

    async def scrape_products_batch(self, urls: List[str], category_hint: str = "") -> List[Dict]:
        results = []
        for i, url in enumerate(urls):
            logger.info(f"Scraping product {i+1}/{len(urls)}: {url}")
            product = await self.scrape_product(url, category_hint)
            if product:
                results.append(product)
            await asyncio.sleep(0.5)
        return results

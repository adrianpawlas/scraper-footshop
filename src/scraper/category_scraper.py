import asyncio
import re
from playwright.async_api import async_playwright, Page, Browser
from tenacity import retry, stop_after_attempt, wait_exponential
from typing import List, Dict, Optional
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class CategoryScraper:
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

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=2, max=10))
    async def get_product_urls_from_page(self, url: str) -> List[str]:
        await self.page.goto(url, wait_until="domcontentloaded")
        
        await self.page.wait_for_timeout(5000)
        
        products = await self.page.query_selector_all('a[href*="/mens-shoes/"], a[href*="/womens-shoes/"]')
        urls = []
        seen = set()
        
        for product in products:
            href = await product.get_attribute('href')
            if href and href not in seen:
                seen.add(href)
                urls.append(href)
        
        logger.info(f"Found {len(urls)} products on {url}")
        return urls

    async def get_total_pages(self, category_url: str) -> int:
        await self.page.goto(category_url, wait_until="domcontentloaded")
        await self.page.wait_for_timeout(3000)
        
        try:
            pagination = await self.page.query_selector('.pagination')
            if pagination:
                page_links = await pagination.query_selector_all('a')
                if page_links:
                    pages = []
                    for link in page_links:
                        text = await link.inner_text()
                        if text.strip().isdigit():
                            pages.append(int(text.strip()))
                    if pages:
                        return max(pages)
        except Exception:
            pass
        
        product_count = await self._check_page_product_count(category_url)
        if product_count == 0:
            return 1
        
        test_url = f"{category_url}/page-2"
        count = await self._check_page_product_count(test_url)
        if count > 0:
            return 100
        
        return 1

    async def _check_page_product_count(self, url: str) -> int:
        try:
            await self.page.goto(url, wait_until="domcontentloaded")
            await self.page.wait_for_timeout(3000)
            products = await self.page.query_selector_all('a[href*="/mens-shoes/"], a[href*="/womens-shoes/"]')
            seen = set()
            for p in products:
                href = await p.get_attribute('href')
                if href:
                    seen.add(href)
            return len(seen)
        except Exception:
            return 0

    async def scrape_category(self, category_url: str) -> List[str]:
        all_urls = []
        
        total_pages = await self.get_total_pages(category_url)
        logger.info(f"Category {category_url} has {total_pages} pages")
        
        for page_num in range(1, total_pages + 1):
            if page_num == 1:
                page_url = category_url
            else:
                page_url = f"{category_url}/page-{page_num}"
            
            logger.info(f"Scraping page {page_num}/{total_pages}: {page_url}")
            
            try:
                urls = await self.get_product_urls_from_page(page_url)
                if not urls:
                    logger.info(f"No products found on page {page_num}, stopping")
                    break
                all_urls.extend(urls)
            except Exception as e:
                logger.error(f"Error scraping page {page_num}: {e}")
                break
        
        return all_urls

    async def scrape_all_categories(self, category_urls: List[str]) -> List[str]:
        all_product_urls = []
        
        for category_url in category_urls:
            logger.info(f"Scraping category: {category_url}")
            urls = await self.scrape_category(category_url)
            all_product_urls.extend(urls)
            logger.info(f"Found {len(urls)} products in {category_url}")
        
        unique_urls = list(set(all_product_urls))
        logger.info(f"Total unique product URLs: {len(unique_urls)}")
        
        return unique_urls

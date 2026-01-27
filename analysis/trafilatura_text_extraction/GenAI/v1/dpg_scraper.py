#!/usr/bin/env python3
"""
Specialized Scrapers for Problematic Dutch News Sites

Handles sites that failed with standard methods:
- AD.nl
- ED.nl
- BD.nl
- Gelderlander.nl
- Destentor.nl

These are all DPG Media sites that use heavy JavaScript and
require special handling.
"""

from typing import Optional
import time
import json
from urllib.parse import urlparse

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Import from main scraper
from news_scraper import Article, ArticleScraper


class DPGMediaScraper(ArticleScraper):
    """
    Specialized scraper for DPG Media sites.
    
    DPG Media owns: AD, Volkskrant, Trouw, Parool, BD, ED, 
    Destentor, Gelderlander, and others.
    
    These sites share similar HTML structure but require Selenium
    and careful handling of their JavaScript rendering.
    """
    
    SUPPORTED_DOMAINS = [
        'ad.nl',
        'ed.nl', 
        'bd.nl',
        'destentor.nl',
        'gelderlander.nl',
        'tubantia.nl',
        'pzc.nl',
        'bndestem.nl'
    ]
    
    def __init__(self, headless: bool = True, wait_time: int = 5):
        """
        Args:
            headless: Run browser without GUI
            wait_time: Seconds to wait for JavaScript to render
        """
        self.headless = headless
        self.wait_time = wait_time
        self._driver = None
    
    @property
    def driver(self):
        """Lazy driver initialization"""
        if self._driver is None:
            self._driver = self._create_driver()
        return self._driver
    
    def _create_driver(self):
        """Create optimized Chrome driver for DPG sites"""
        options = Options()
        
        if self.headless:
            options.add_argument('--headless=new')
        
        # Performance options
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--disable-software-rasterizer')
        options.add_argument('--window-size=1920,1080')
        
        # Anti-detection
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        # Dutch locale and headers
        options.add_argument('--lang=nl-NL')
        options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36')
        
        # Disable images to speed up loading
        prefs = {
            'profile.managed_default_content_settings.images': 2,
            'profile.default_content_setting_values.notifications': 2,
        }
        options.add_experimental_option('prefs', prefs)
        
        driver = webdriver.Chrome(options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return driver
    
    def can_scrape(self, url: str) -> bool:
        domain = self._get_domain(url)
        return any(d in domain for d in self.SUPPORTED_DOMAINS)
    
    def scrape(self, url: str) -> Optional[Article]:
        """
        Scrape DPG Media article with multiple strategies
        """
        try:
            # Load page
            self.driver.get(url)
            
            # Wait for article element
            self._wait_for_article()
            
            # Additional wait for JavaScript
            time.sleep(self.wait_time)
            
            # Progressive loading: scroll to trigger lazy content
            self._trigger_lazy_loading()
            
            # Get rendered HTML
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Strategy 1: JSON-LD (most reliable for DPG)
            article = self._extract_json_ld(url, soup)
            if article:
                return article
            
            # Strategy 2: DPG-specific HTML selectors
            article = self._extract_dpg_html(url, soup)
            if article:
                return article
            
            # Strategy 3: Generic article extraction
            return self._extract_generic(url, soup)
            
        except Exception as e:
            print(f"DPG scraper error for {url}: {e}")
            return None
    
    def _wait_for_article(self):
        """Wait for article content to appear"""
        selectors_to_try = [
            (By.TAG_NAME, "article"),
            (By.CSS_SELECTOR, "div[class*='article']"),
            (By.CSS_SELECTOR, "div[class*='artstory']"),
            (By.TAG_NAME, "h1"),
        ]
        
        for by, selector in selectors_to_try:
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((by, selector))
                )
                return
            except:
                continue
    
    def _trigger_lazy_loading(self):
        """Scroll page to trigger lazy-loaded content"""
        try:
            # Scroll to 25%, 50%, 75%, and bottom
            for percentage in [0.25, 0.5, 0.75, 1.0]:
                self.driver.execute_script(
                    f"window.scrollTo(0, document.body.scrollHeight * {percentage});"
                )
                time.sleep(0.5)
            
            # Scroll back to top
            self.driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(0.5)
        except:
            pass
    
    def _extract_json_ld(self, url: str, soup: BeautifulSoup) -> Optional[Article]:
        """
        Extract from JSON-LD structured data.
        
        DPG Media sites often include full article text in JSON-LD
        for SEO purposes, even when it's hidden in the HTML.
        """
        scripts = soup.find_all('script', type='application/ld+json')
        
        for script in scripts:
            try:
                data = json.loads(script.string)
                
                # Handle array of JSON-LD objects
                if isinstance(data, list):
                    # Look for NewsArticle
                    for item in data:
                        if isinstance(item, dict) and item.get('@type') == 'NewsArticle':
                            data = item
                            break
                    else:
                        continue
                
                # Must be NewsArticle
                if not isinstance(data, dict) or data.get('@type') != 'NewsArticle':
                    continue
                
                # Extract content
                content = data.get('articleBody', '')
                
                # Some sites put content in description
                if not content or len(content) < 100:
                    content = data.get('description', '')
                
                if not content or len(content) < 100:
                    continue
                
                # Extract author
                author = self._extract_author_from_json(data.get('author'))
                
                return Article(
                    url=url,
                    title=data.get('headline'),
                    content=content,
                    summary=data.get('description'),
                    author=author,
                    date=data.get('datePublished'),
                    method='dpg_json_ld'
                )
                
            except (json.JSONDecodeError, ValueError, KeyError):
                continue
        
        return None
    
    def _extract_dpg_html(self, url: str, soup: BeautifulSoup) -> Optional[Article]:
        """
        Extract using DPG Media specific HTML structure.
        
        DPG sites use consistent class naming:
        - article__header / artheader
        - article__intro / intro
        - article__body / artstory
        """
        # Title
        title = self._find_element_text(soup, [
            ('h1', {'class': lambda x: x and 'article__header' in x}),
            ('h1', {'class': lambda x: x and 'artheader' in x}),
            ('h1', {}),
        ])
        
        # Intro/Summary
        summary = self._find_element_text(soup, [
            ('p', {'class': lambda x: x and 'article__intro' in x}),
            ('p', {'class': lambda x: x and 'intro' in x}),
            ('div', {'class': lambda x: x and 'intro' in x}),
        ])
        
        # Main content
        content_div = self._find_element(soup, [
            ('div', {'class': lambda x: x and 'article__body' in x}),
            ('div', {'class': lambda x: x and 'artstory' in x}),
            ('div', {'class': lambda x: x and 'article-body' in x}),
        ])
        
        if not content_div:
            return None
        
        # Clean up unwanted elements
        for tag in content_div.find_all(['script', 'style', 'nav', 'aside', 'header', 'footer']):
            tag.decompose()
        
        # Remove ads and figures
        for tag in content_div.find_all(['div', 'figure'], class_=lambda x: x and ('ad' in str(x).lower() or 'image' in str(x).lower())):
            tag.decompose()
        
        # Extract paragraphs
        paragraphs = content_div.find_all('p')
        content_parts = []
        
        # Add intro if we have it
        if summary:
            content_parts.append(summary)
        
        # Add article paragraphs
        for p in paragraphs:
            text = p.get_text(strip=True)
            if len(text) > 30 and text not in content_parts:
                content_parts.append(text)
        
        if not content_parts:
            return None
        
        return Article(
            url=url,
            title=title,
            content='\n\n'.join(content_parts),
            summary=summary,
            method='dpg_html'
        )
    
    def _extract_generic(self, url: str, soup: BeautifulSoup) -> Optional[Article]:
        """Last resort: generic article extraction"""
        # Find title
        title_elem = soup.find('h1')
        title = title_elem.get_text(strip=True) if title_elem else None
        
        # Find article
        article_elem = soup.find('article')
        if not article_elem:
            return None
        
        # Clean
        for tag in article_elem.find_all(['script', 'style', 'nav', 'aside']):
            tag.decompose()
        
        # Get all paragraphs
        paragraphs = article_elem.find_all('p')
        content_parts = [
            p.get_text(strip=True) 
            for p in paragraphs 
            if len(p.get_text(strip=True)) > 30
        ]
        
        if not content_parts:
            return None
        
        return Article(
            url=url,
            title=title,
            content='\n\n'.join(content_parts),
            method='dpg_generic'
        )
    
    def _find_element(self, soup: BeautifulSoup, selectors: list):
        """Try multiple selectors, return first match"""
        for tag, attrs in selectors:
            elem = soup.find(tag, attrs)
            if elem:
                return elem
        return None
    
    def _find_element_text(self, soup: BeautifulSoup, selectors: list) -> Optional[str]:
        """Find element and return its text"""
        elem = self._find_element(soup, selectors)
        return elem.get_text(strip=True) if elem else None
    
    @staticmethod
    def _extract_author_from_json(author_data) -> Optional[str]:
        """Extract author from various JSON-LD formats"""
        if isinstance(author_data, dict):
            return author_data.get('name')
        elif isinstance(author_data, list):
            names = [a.get('name', '') for a in author_data if isinstance(a, dict)]
            return ', '.join(names) if names else None
        elif isinstance(author_data, str):
            return author_data
        return None
    
    @staticmethod
    def _get_domain(url: str) -> str:
        return urlparse(url).netloc.replace('www.', '')
    
    def cleanup(self):
        if self._driver:
            self._driver.quit()
            self._driver = None


# ============================================================================
# Testing
# ============================================================================

def test_dpg_scraper():
    """Test DPG Media scraper on problematic sites"""
    
    test_urls = {
        'AD': 'https://www.ad.nl/buitenland/witte-huis-sluit-inzet-leger-niet-uit-bij-verkrijgen-groenland-al-houdt-minister-het-op-kopen~a7975ae2/',
        'ED': 'https://www.ed.nl/buitenland/sydney-sluit-stranden-na-derde-haaienaanval-in-twee-dagen-tijd~a290cf50/',
        'BD': 'https://www.bd.nl/buitenland/oostenrijk-in-rep-en-roer-kalfsvlees-voor-wienerschnitzel-komt-uit-nederland-zonder-dat-consument-dat-weet~a4a1c70a/',
        'Gelderlander': 'https://www.gelderlander.nl/economie/topbestuurders-eisen-miljardenfonds-vertraging-nieuwe-energie-infrastructuur-bedreigt-banen~a7c40088/',
        'Destentor': 'https://www.destentor.nl/buitenland/kijk-britse-bestuurder-is-zo-dronken-dat-hij-acht-pogingen-nodig-heeft-om-te-blazen~acf05a2d/',
    }
    
    print("Testing DPG Media Scraper")
    print("=" * 80)
    print("\nThis scraper handles AD, ED, BD, and other DPG Media sites")
    print("Using enhanced Selenium with progressive loading...\n")
    
    scraper = DPGMediaScraper(headless=True, wait_time=5)
    
    try:
        results = {}
        
        for name, url in test_urls.items():
            print(f"\n{'='*80}")
            print(f"Testing: {name}")
            print(f"URL: {url}")
            print(f"{'='*80}")
            
            start = time.time()
            article = scraper.scrape(url)
            elapsed = time.time() - start
            
            results[name] = article
            
            if article:
                print(f"✓ SUCCESS via {article.method} ({elapsed:.1f}s)")
                print(f"\nTitle: {article.title[:80] if article.title else 'N/A'}...")
                if article.summary:
                    print(f"Summary: {article.summary[:100]}...")
                print(f"Content: {len(article.content)} characters")
                print(f"\nPreview:\n{article.content[:300]}...")
            else:
                print(f"✗ FAILED ({elapsed:.1f}s)")
            
            time.sleep(2)
        
        # Summary
        print(f"\n\n{'='*80}")
        print("SUMMARY")
        print(f"{'='*80}")
        
        successful = sum(1 for a in results.values() if a is not None)
        print(f"\nTotal: {len(results)}")
        print(f"Success: {successful}")
        print(f"Failed: {len(results) - successful}")
        
        print("\n\nResults by site:")
        for name, article in results.items():
            status = "✓" if article else "✗"
            chars = len(article.content) if article else 0
            method = article.method if article else "N/A"
            print(f"  {status} {name:15} - {chars:5} chars - {method}")
        
    finally:
        scraper.cleanup()


if __name__ == '__main__':
    test_dpg_scraper()

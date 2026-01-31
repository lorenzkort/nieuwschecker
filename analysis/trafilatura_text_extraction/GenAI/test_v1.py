from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Dict, List
from urllib.parse import urlparse
import time
import json

# Third-party imports
import trafilatura
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


# ============================================================================
# Data Models
# ============================================================================

@dataclass
class Article:
    """Immutable article data structure"""
    url: str
    title: Optional[str]
    content: str
    summary: Optional[str] = None
    author: Optional[str] = None
    date: Optional[str] = None
    method: str = 'unknown'
    
    def __post_init__(self):
        """Validate required fields"""
        if not self.content:
            raise ValueError("Article content cannot be empty")
        if not self.url:
            raise ValueError("Article URL cannot be empty")


@dataclass
class ScrapeResult:
    """Result of a scraping operation"""
    success: bool
    article: Optional[Article] = None
    error: Optional[str] = None
    time_taken: float = 0.0


# ============================================================================
# Abstract Base Classes (Interfaces)
# ============================================================================

class ArticleScraper(ABC):
    """Abstract base class for all article scrapers"""
    
    @abstractmethod
    def can_scrape(self, url: str) -> bool:
        """Check if this scraper can handle the given URL"""
        pass
    
    @abstractmethod
    def scrape(self, url: str) -> Optional[Article]:
        """Scrape article from URL, return None if failed"""
        pass
    
    @abstractmethod
    def cleanup(self):
        """Clean up any resources"""
        pass


# ============================================================================
# Concrete Scraper Implementations
# ============================================================================

class TrafilaturaScraper(ArticleScraper):
    """Fast scraper using trafilatura library"""
    
    SUPPORTED_DOMAINS = ['nos.nl', 'volkskrant.nl', 'nrc.nl', 'trouw.nl', 'parool.nl']
    
    def can_scrape(self, url: str) -> bool:
        domain = self._get_domain(url)
        return any(d in domain for d in self.SUPPORTED_DOMAINS)
    
    def scrape(self, url: str) -> Optional[Article]:
        try:
            downloaded = trafilatura.fetch_url(url)
            if not downloaded:
                return None
            
            content = trafilatura.extract(downloaded)
            if not content or len(content) < 100:
                return None
            
            metadata = trafilatura.extract_metadata(downloaded)
            
            return Article(
                url=url,
                title=metadata.title if metadata else None,
                content=content,
                author=metadata.author if metadata else None,
                date=metadata.date if metadata else None,
                method='trafilatura'
            )
        except Exception:
            return None
    
    def cleanup(self):
        pass
    
    @staticmethod
    def _get_domain(url: str) -> str:
        return urlparse(url).netloc.replace('www.', '')


class SeleniumScraper(ArticleScraper):
    """Scraper using Selenium for JavaScript-rendered sites"""
    
    SUPPORTED_DOMAINS = ['nu.nl', 'telegraaf.nl', 'bnr.nl']
    
    def __init__(self, headless: bool = True, wait_time: int = 3):
        self.headless = headless
        self.wait_time = wait_time
        self._driver = None
    
    @property
    def driver(self):
        """Lazy initialization of Selenium driver"""
        if self._driver is None:
            self._driver = self._create_driver()
        return self._driver
    
    def _create_driver(self):
        """Create and configure Chrome driver"""
        options = Options()
        
        if self.headless:
            options.add_argument('--headless=new')
        
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')
        options.add_argument('--window-size=1920,1080')
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36')
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)
        
        driver = webdriver.Chrome(options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        return driver
    
    def can_scrape(self, url: str) -> bool:
        domain = self._get_domain(url)
        return any(d in domain for d in self.SUPPORTED_DOMAINS)
    
    def scrape(self, url: str) -> Optional[Article]:
        try:
            self.driver.get(url)
            
            # Wait for content
            try:
                WebDriverWait(self.driver, 10).until(
                    EC.presence_of_element_located((By.TAG_NAME, "article"))
                )
            except:
                pass
            
            time.sleep(self.wait_time)
            
            # Scroll to trigger lazy loading
            self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight/2);")
            time.sleep(1)
            
            soup = BeautifulSoup(self.driver.page_source, 'html.parser')
            
            # Try JSON-LD first
            article = self._extract_json_ld(url, soup)
            if article:
                return article
            
            # Fallback to HTML parsing
            return self._extract_html(url, soup)
            
        except Exception:
            return None
    
    def _extract_json_ld(self, url: str, soup: BeautifulSoup) -> Optional[Article]:
        """Extract article from JSON-LD structured data"""
        scripts = soup.find_all('script', type='application/ld+json')
        
        for script in scripts:
            try:
                data = json.loads(script.string)
                
                if isinstance(data, list):
                    data = next((d for d in data if d.get('@type') == 'NewsArticle'), None)
                
                if not data or data.get('@type') != 'NewsArticle':
                    continue
                
                content = data.get('articleBody', '')
                if not content or len(content) < 100:
                    continue
                
                author = self._extract_author(data.get('author'))
                
                return Article(
                    url=url,
                    title=data.get('headline'),
                    content=content,
                    summary=data.get('description'),
                    author=author,
                    date=data.get('datePublished'),
                    method='selenium_json_ld'
                )
            except (json.JSONDecodeError, ValueError):
                continue
        
        return None
    
    def _extract_html(self, url: str, soup: BeautifulSoup) -> Optional[Article]:
        """Extract article from HTML elements"""
        # Find title
        title_elem = soup.find('h1')
        title = title_elem.get_text(strip=True) if title_elem else None
        
        # Find article content
        article_elem = soup.find('article')
        if not article_elem:
            return None
        
        # Remove unwanted elements
        for tag in article_elem.find_all(['script', 'style', 'nav', 'aside', 'header', 'footer']):
            tag.decompose()
        
        # Extract paragraphs
        paragraphs = article_elem.find_all('p')
        content_parts = [
            p.get_text(strip=True) 
            for p in paragraphs 
            if len(p.get_text(strip=True)) > 30
        ]
        
        if not content_parts:
            return None
        
        content = '\n\n'.join(content_parts)
        
        return Article(
            url=url,
            title=title,
            content=content,
            method='selenium_html'
        )
    
    @staticmethod
    def _extract_author(author_data) -> Optional[str]:
        """Extract author name from various JSON-LD formats"""
        if isinstance(author_data, dict):
            return author_data.get('name')
        elif isinstance(author_data, list):
            return ', '.join(a.get('name', '') for a in author_data if isinstance(a, dict))
        elif isinstance(author_data, str):
            return author_data
        return None
    
    @staticmethod
    def _get_domain(url: str) -> str:
        return urlparse(url).netloc.replace('www.', '')
    
    def cleanup(self):
        """Close browser"""
        if self._driver:
            self._driver.quit()
            self._driver = None


# ============================================================================
# Main Scraper Orchestrator
# ============================================================================

class DutchNewsScraperService:
    """
    Main service that orchestrates multiple scrapers.
    
    Follows Single Responsibility Principle:
    - Manages scraper lifecycle
    - Routes URLs to appropriate scrapers
    - Provides clean API for clients
    """
    
    def __init__(self, scrapers: Optional[List[ArticleScraper]] = None):
        """
        Initialize with list of scrapers
        
        Args:
            scrapers: List of scraper instances. If None, uses defaults.
        """
        if scrapers is None:
            scrapers = [
                TrafilaturaScraper(),
                SeleniumScraper(headless=True, wait_time=3)
            ]
        
        self.scrapers = scrapers
    
    def scrape(self, url: str) -> ScrapeResult:
        """
        Scrape article from URL using appropriate scraper
        
        Args:
            url: Article URL
        
        Returns:
            ScrapeResult with article data or error
        """
        start_time = time.time()
        
        # Find appropriate scraper
        scraper = self._find_scraper(url)
        
        if not scraper:
            return ScrapeResult(
                success=False,
                error=f"No scraper available for {urlparse(url).netloc}",
                time_taken=time.time() - start_time
            )
        
        # Attempt scraping
        try:
            article = scraper.scrape(url)
            
            if article:
                return ScrapeResult(
                    success=True,
                    article=article,
                    time_taken=time.time() - start_time
                )
            else:
                return ScrapeResult(
                    success=False,
                    error="Failed to extract article content",
                    time_taken=time.time() - start_time
                )
        
        except Exception as e:
            return ScrapeResult(
                success=False,
                error=str(e),
                time_taken=time.time() - start_time
            )
    
    def scrape_batch(self, urls: Dict[str, str], 
                     progress_callback=None) -> Dict[str, ScrapeResult]:
        """
        Scrape multiple URLs
        
        Args:
            urls: Dict mapping names to URLs
            progress_callback: Optional callback(current, total, name, result)
        
        Returns:
            Dict mapping names to ScrapeResults
        """
        results = {}
        total = len(urls)
        
        for i, (name, url) in enumerate(urls.items(), 1):
            result = self.scrape(url)
            results[name] = result
            
            if progress_callback:
                progress_callback(i, total, name, result)
            
            # Be polite to servers
            time.sleep(1)
        
        return results
    
    def _find_scraper(self, url: str) -> Optional[ArticleScraper]:
        """Find the first scraper that can handle this URL"""
        for scraper in self.scrapers:
            if scraper.can_scrape(url):
                return scraper
        return None
    
    def cleanup(self):
        """Clean up all scrapers"""
        for scraper in self.scrapers:
            scraper.cleanup()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.cleanup()


# ============================================================================
# Usage Example & Testing
# ============================================================================

def main():
    """Example usage and testing"""
    
    # Test URLs
    test_urls = {
        'NOS': 'https://nos.nl/l/2597904',
        'Volkskrant': 'https://www.volkskrant.nl/columns-van-de-dag/aan-de-meubels-kleeft-een-klinische-gedwongen-vtwonen-achtige-spanning~b9829656/',
        'NRC': 'https://www.nrc.nl/nieuws/2026/01/09/de-grote-afbrokkeling-is-begonnen-a4917208',
        'NU.nl': 'https://www.nu.nl/binnenland/6383127/lichaam-tijn-25-volgens-om-verbrand-in-olievat-bruut-en-onmenselijk.html',
        'Telegraaf': 'https://www.telegraaf.nl/binnenland/live-marokko-fans-in-amsterdam-geloven-nog-in-de-overwinning-in-afrika-cup/124602227.html',
        'BNR': 'https://www.bnr.nl/nieuws/internationaal/10591824/iraanse-oud-kroonprins-roept-op-bereid-je-voor-om-steden-in-te-nemen',
        'AD': "https://www.ad.nl/buitenland/witte-huis-sluit-inzet-leger-niet-uit-bij-verkrijgen-groenland-al-houdt-minister-het-op-kopen~a7975ae2/",
    }
    
    print("Dutch News Scraper - Production Test")
    print("=" * 80)
    
    def progress_callback(current, total, name, result):
        status = "✓" if result.success else "✗"
        print(f"\n[{current}/{total}] {status} {name}")
        
        if result.success:
            article = result.article
            print(f"  Method: {article.method}")
            print(f"  Title: {article.title[:70] if article.title else 'N/A'}...")
            print(f"  Content: {len(article.content)} chars")
            print(f"  Time: {result.time_taken:.1f}s")
        else:
            print(f"  Error: {result.error}")
            print(f"  Time: {result.time_taken:.1f}s")
    
    # Run scraper
    with DutchNewsScraperService() as scraper:
        results = scraper.scrape_batch(test_urls, progress_callback)
    
    # Summary
    print("\n" + "=" * 80)
    print("SUMMARY")
    print("=" * 80)
    
    successful = sum(1 for r in results.values() if r.success)
    total_time = sum(r.time_taken for r in results.values())
    
    print(f"\nTotal: {len(results)}")
    print(f"Success: {successful}")
    print(f"Failed: {len(results) - successful}")
    print(f"Total time: {total_time:.1f}s")
    print(f"Avg time: {total_time/len(results):.1f}s per article")
    
    # Method breakdown
    methods = {}
    for result in results.values():
        if result.success:
            method = result.article.method
            methods[method] = methods.get(method, 0) + 1
    
    print("\nMethods used:")
    for method, count in sorted(methods.items()):
        print(f"  {method}: {count}")


if __name__ == '__main__':
    main()

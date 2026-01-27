#!/usr/bin/env python3
"""
Simple Integration Example

Shows how to use the news scraper with all supported domains.
"""

from news_scraper import DutchNewsScraperService, TrafilaturaScraper, SeleniumScraper
from dpg_scraper import DPGMediaScraper


# ============================================================================
# Quick Start Example
# ============================================================================

def quick_start():
    """Simplest possible usage"""
    
    # Create scraper with all scrapers
    scrapers = [
        TrafilaturaScraper(),           # Fast: NOS, NRC, Volkskrant, Trouw, Parool
        SeleniumScraper(),              # Medium: NU.nl, Telegraaf, BNR
        DPGMediaScraper(wait_time=5),   # Slow: AD, ED, BD, Gelderlander, Destentor
    ]
    
    service = DutchNewsScraperService(scrapers=scrapers)
    
    # Scrape a single article
    url = "https://nos.nl/l/2597904"
    result = service.scrape(url)
    
    if result.success:
        print(f"Title: {result.article.title}")
        print(f"Content: {result.article.content[:200]}...")
    else:
        print(f"Error: {result.error}")
    
    # Clean up
    service.cleanup()


# ============================================================================
# Batch Processing Example
# ============================================================================

def batch_example():
    """Scrape multiple articles"""
    
    urls = {
        # Fast sites
        'NOS': 'https://nos.nl/l/2597904',
        'NRC': 'https://www.nrc.nl/nieuws/2026/01/09/de-grote-afbrokkeling-is-begonnen-a4917208',
        
        # Medium sites
        'NU.nl': 'https://www.nu.nl/binnenland/6383127/lichaam-tijn-25-volgens-om-verbrand-in-olievat-bruut-en-onmenselijk.html',
        'Telegraaf': 'https://www.telegraaf.nl/binnenland/live-marokko-fans-in-amsterdam-geloven-nog-in-de-overwinning-in-afrika-cup/124602227.html',
        
        # Slow sites (DPG Media)
        'AD': 'https://www.ad.nl/buitenland/witte-huis-sluit-inzet-leger-niet-uit-bij-verkrijgen-groenland-al-houdt-minister-het-op-kopen~a7975ae2/',
        'ED': 'https://www.ed.nl/buitenland/sydney-sluit-stranden-na-derde-haaienaanval-in-twee-dagen-tijd~a290cf50/',
    }
    
    scrapers = [
        TrafilaturaScraper(),
        SeleniumScraper(),
        DPGMediaScraper(wait_time=5),
    ]
    
    with DutchNewsScraperService(scrapers=scrapers) as service:
        results = service.scrape_batch(urls, progress_callback=print_progress)
    
    # Process results
    for name, result in results.items():
        if result.success:
            save_article(name, result.article)


def print_progress(current, total, name, result):
    """Progress callback"""
    status = "✓" if result.success else "✗"
    print(f"[{current}/{total}] {status} {name} - {result.time_taken:.1f}s")


def save_article(name, article):
    """Save article to database or file"""
    # Your code here
    print(f"Saved: {name} - {len(article.content)} chars")


# ============================================================================
# Supported Domains Reference
# ============================================================================

SUPPORTED_DOMAINS = {
    'Fast (Trafilatura)': [
        'nos.nl',
        'nrc.nl',
        'volkskrant.nl',
        'trouw.nl',
        'parool.nl',
    ],
    'Medium (Selenium)': [
        'nu.nl',
        'telegraaf.nl',
        'bnr.nl',
    ],
    'Slow (DPG Media Selenium)': [
        'ad.nl',
        'ed.nl',
        'bd.nl',
        'destentor.nl',
        'gelderlander.nl',
        'tubantia.nl',
        'pzc.nl',
        'bndestem.nl',
    ]
}


def print_supported_domains():
    """Print all supported domains"""
    print("Supported Dutch News Domains")
    print("=" * 80)
    
    for category, domains in SUPPORTED_DOMAINS.items():
        print(f"\n{category}:")
        for domain in domains:
            print(f"  - {domain}")
    
    total = sum(len(domains) for domains in SUPPORTED_DOMAINS.values())
    print(f"\nTotal: {total} domains")


# ============================================================================
# Production Usage Example
# ============================================================================

def production_example():
    """
    Production-ready example with error handling and logging
    """
    import logging
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    logger = logging.getLogger(__name__)
    
    # URLs to scrape
    urls = {
        'Article 1': 'https://nos.nl/l/2597904',
        'Article 2': 'https://www.nu.nl/binnenland/6383127/lichaam-tijn-25-volgens-om-verbrand-in-olievat-bruut-en-onmenselijk.html',
        'Article 3': 'https://www.ad.nl/buitenland/witte-huis-sluit-inzet-leger-niet-uit-bij-verkrijgen-groenland-al-houdt-minister-het-op-kopen~a7975ae2/',
    }
    
    # Initialize scrapers
    scrapers = [
        TrafilaturaScraper(),
        SeleniumScraper(headless=True, wait_time=3),
        DPGMediaScraper(headless=True, wait_time=5),
    ]
    
    # Scrape
    successful = []
    failed = []
    
    try:
        with DutchNewsScraperService(scrapers=scrapers) as service:
            for name, url in urls.items():
                logger.info(f"Scraping {name}: {url}")
                
                result = service.scrape(url)
                
                if result.success:
                    logger.info(f"✓ {name} - {len(result.article.content)} chars")
                    successful.append({
                        'name': name,
                        'article': result.article,
                        'time': result.time_taken
                    })
                else:
                    logger.error(f"✗ {name} - {result.error}")
                    failed.append({
                        'name': name,
                        'url': url,
                        'error': result.error
                    })
    
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        return
    
    # Report
    logger.info(f"\n{'='*80}")
    logger.info(f"Scraping complete")
    logger.info(f"Success: {len(successful)}/{len(urls)}")
    logger.info(f"Failed: {len(failed)}/{len(urls)}")
    
    if failed:
        logger.warning(f"\nFailed articles:")
        for item in failed:
            logger.warning(f"  - {item['name']}: {item['error']}")
    
    return successful, failed


# ============================================================================
# Main
# ============================================================================

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1:
        if sys.argv[1] == 'quick':
            quick_start()
        elif sys.argv[1] == 'batch':
            batch_example()
        elif sys.argv[1] == 'domains':
            print_supported_domains()
        elif sys.argv[1] == 'production':
            production_example()
        else:
            print(f"Unknown command: {sys.argv[1]}")
            print("Usage: python integration_example.py [quick|batch|domains|production]")
    else:
        print("Dutch News Scraper - Integration Examples")
        print("=" * 80)
        print("\nRun examples:")
        print("  python integration_example.py quick       - Quick start")
        print("  python integration_example.py batch       - Batch processing")
        print("  python integration_example.py domains     - Show supported domains")
        print("  python integration_example.py production  - Production example")

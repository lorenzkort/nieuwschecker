#!/usr/bin/env python3
"""
Comprehensive Test Script for All Dutch News Domains

Tests all provided URLs across different scraper strategies.
"""

import time
from typing import Dict

from dpg_scraper import DPGMediaScraper
from news_scraper import DutchNewsScraperService, SeleniumScraper, TrafilaturaScraper

# ============================================================================
# Test URLs - All Domains
# ============================================================================

TEST_URLS = {
    # Fast sites (should work with Trafilatura)
    "NOS": "https://nos.nl/l/2597904",
    "Volkskrant": "https://www.volkskrant.nl/columns-van-de-dag/aan-de-meubels-kleeft-een-klinische-gedwongen-vtwonen-achtige-spanning~b9829656/",
    "NRC": "https://www.nrc.nl/nieuws/2026/01/09/de-grote-afbrokkeling-is-begonnen-a4917208",
    "Trouw": "https://www.trouw.nl/duurzaamheid-economie/hoe-de-duurzame-boer-in-2025-juist-niet-beloond-werd-of-toch-wel-geluid-maken-helpt~b6094577/",
    "Parool": "https://www.parool.nl/nederland/oud-en-nieuw-wordt-bewolkt-maar-grotendeels-droog~b7b392ca/",
    # Medium sites (need Selenium but work with standard approach)
    "NU.nl": "https://www.nu.nl/binnenland/6383127/lichaam-tijn-25-volgens-om-verbrand-in-olievat-bruut-en-onmenselijk.html",
    "Telegraaf": "https://www.telegraaf.nl/binnenland/live-marokko-fans-in-amsterdam-geloven-nog-in-de-overwinning-in-afrika-cup/124602227.html",
    "BNR": "https://www.bnr.nl/nieuws/internationaal/10591824/iraanse-oud-kroonprins-roept-op-bereid-je-voor-om-steden-in-te-nemen",
    # FD (special case - works partially with Trafilatura)
    "FD": "https://fd.nl/bedrijfsleven/1584390/moederbedrijf-batavus-en-sparta-heeft-weer-geld-nodig",
    # DPG Media sites (need enhanced Selenium)
    "AD": "https://www.ad.nl/buitenland/witte-huis-sluit-inzet-leger-niet-uit-bij-verkrijgen-groenland-al-houdt-minister-het-op-kopen~a7975ae2/",
    "ED": "https://www.ed.nl/buitenland/sydney-sluit-stranden-na-derde-haaienaanval-in-twee-dagen-tijd~a290cf50/",
    "BD": "https://www.bd.nl/buitenland/oostenrijk-in-rep-en-roer-kalfsvlees-voor-wienerschnitzel-komt-uit-nederland-zonder-dat-consument-dat-weet~a4a1c70a/",
    "Gelderlander": "https://www.gelderlander.nl/economie/topbestuurders-eisen-miljardenfonds-vertraging-nieuwe-energie-infrastructuur-bedreigt-banen~a7c40088/",
    "Destentor": "https://www.destentor.nl/buitenland/kijk-britse-bestuurder-is-zo-dronken-dat-hij-acht-pogingen-nodig-heeft-om-te-blazen~acf05a2d/",
}


# ============================================================================
# Progress Display
# ============================================================================


def print_header():
    """Print test header"""
    print("\n" + "=" * 100)
    print("COMPREHENSIVE DUTCH NEWS SCRAPER TEST".center(100))
    print("=" * 100)
    print(f"\nTesting {len(TEST_URLS)} domains with all scraping strategies")
    print()


def print_progress(current: int, total: int, name: str, result):
    """Print progress for each article"""
    print(f"\n{'=' * 100}")
    print(f"[{current}/{total}] {name}")
    print(f"{'=' * 100}")

    if result.success:
        article = result.article
        print(f"✓ SUCCESS")
        print(f"  Method:  {article.method}")
        print(f"  Time:    {result.time_taken:.1f}s")
        print(f"  Title:   {article.title[:80] if article.title else 'N/A'}...")

        if article.author:
            print(f"  Author:  {article.author}")
        if article.date:
            print(f"  Date:    {article.date}")
        if article.summary:
            print(f"  Summary: {article.summary[:100]}...")

        print(f"  Content: {len(article.content)} characters")
        print(f"\n  Preview:\n  {article.content[:200].replace(chr(10), ' ')}...")
    else:
        print(f"✗ FAILED")
        print(f"  Time:  {result.time_taken:.1f}s")
        print(f"  Error: {result.error}")


def print_summary(results: Dict):
    """Print comprehensive summary"""
    print(f"\n\n{'=' * 100}")
    print("SUMMARY".center(100))
    print(f"{'=' * 100}\n")

    # Overall stats
    successful = sum(1 for r in results.values() if r.success)
    total_time = sum(r.time_taken for r in results.values())

    print("Overall Statistics:")
    print(f"  Total sites:     {len(results)}")
    print(f"  Successful:      {successful} ({successful/len(results)*100:.1f}%)")
    print(f"  Failed:          {len(results) - successful}")
    print(f"  Total time:      {total_time:.1f}s")
    print(f"  Average time:    {total_time/len(results):.1f}s per article")

    # Method breakdown
    print("\nMethods Used:")
    methods = {}
    for result in results.values():
        if result.success:
            method = result.article.method
            methods[method] = methods.get(method, 0) + 1

    for method, count in sorted(methods.items()):
        print(f"  {method:20} {count} articles")

    # Speed categories
    print("\nSpeed Categories:")
    fast = sum(1 for r in results.values() if r.success and r.time_taken < 3)
    medium = sum(1 for r in results.values() if r.success and 3 <= r.time_taken < 6)
    slow = sum(1 for r in results.values() if r.success and r.time_taken >= 6)

    print(f"  Fast (<3s):      {fast} articles")
    print(f"  Medium (3-6s):   {medium} articles")
    print(f"  Slow (>6s):      {slow} articles")

    # Detailed results by domain
    print("\n" + "=" * 100)
    print("Detailed Results by Domain")
    print("=" * 100)

    # Group by category
    categories = {
        "Fast Sites (Trafilatura)": ["NOS", "Volkskrant", "NRC", "Trouw", "Parool"],
        "Medium Sites (Selenium)": ["NU.nl", "Telegraaf", "BNR"],
        "Special Case": ["FD"],
        "DPG Media Sites (Enhanced Selenium)": [
            "AD",
            "ED",
            "BD",
            "Gelderlander",
            "Destentor",
        ],
    }

    for category, sites in categories.items():
        print(f"\n{category}:")
        for site in sites:
            if site not in results:
                continue

            result = results[site]
            if result.success:
                article = result.article
                status = "✓"
                info = f"{len(article.content):5} chars - {result.time_taken:4.1f}s - {article.method}"
            else:
                status = "✗"
                info = f"FAILED - {result.error[:50]}"

            print(f"  {status} {site:15} - {info}")

    # Failed URLs
    failed = {name: result for name, result in results.items() if not result.success}
    if failed:
        print(f"\n{'=' * 100}")
        print("Failed URLs (for debugging)")
        print(f"{'=' * 100}")
        for name, result in failed.items():
            print(f"\n{name}:")
            print(f"  URL:   {TEST_URLS[name]}")
            print(f"  Error: {result.error}")


# ============================================================================
# Main Test Function
# ============================================================================


def run_comprehensive_test():
    """Run comprehensive test on all domains"""

    print_header()

    # Initialize all scrapers
    print("Initializing scrapers...")
    scrapers = [
        TrafilaturaScraper(),
        SeleniumScraper(headless=True, wait_time=3),
        DPGMediaScraper(
            headless=True, wait_time=6
        ),  # Longer wait for problematic sites
    ]

    print("✓ Scrapers initialized\n")
    print("Starting scraping process...")

    # Run test
    start_time = time.time()

    with DutchNewsScraperService(scrapers=scrapers) as service:
        results = service.scrape_batch(TEST_URLS, progress_callback=print_progress)

    total_elapsed = time.time() - start_time

    # Print summary
    print_summary(results)

    print(f"\n{'=' * 100}")
    print(f"Total execution time: {total_elapsed:.1f}s")
    print(f"{'=' * 100}\n")

    return results


# ============================================================================
# Quick Test (subset of URLs)
# ============================================================================


def run_quick_test():
    """Run quick test on representative URLs"""

    quick_urls = {
        "NOS": TEST_URLS["NOS"],
        "NU.nl": TEST_URLS["NU.nl"],
        "AD": TEST_URLS["AD"],
    }

    print("\n" + "=" * 100)
    print("QUICK TEST - Representative Sample".center(100))
    print("=" * 100)
    print("\nTesting 3 representative sites (fast, medium, slow)")

    scrapers = [
        TrafilaturaScraper(),
        SeleniumScraper(headless=True, wait_time=3),
        DPGMediaScraper(headless=True, wait_time=6),
    ]

    with DutchNewsScraperService(scrapers=scrapers) as service:
        results = service.scrape_batch(quick_urls, progress_callback=print_progress)

    # Quick summary
    print(f"\n\n{'=' * 100}")
    print("Quick Test Summary")
    print(f"{'=' * 100}")

    for name, result in results.items():
        if result.success:
            print(
                f"✓ {name:10} - {len(result.article.content):5} chars - {result.time_taken:4.1f}s - {result.article.method}"
            )
        else:
            print(f"✗ {name:10} - FAILED - {result.error}")

    return results


# ============================================================================
# Domain-Specific Tests
# ============================================================================


def test_fast_sites_only():
    """Test only fast sites (Trafilatura)"""
    fast_urls = {
        k: v
        for k, v in TEST_URLS.items()
        if k in ["NOS", "Volkskrant", "NRC", "Trouw", "Parool"]
    }

    print("\n" + "=" * 100)
    print("FAST SITES TEST (Trafilatura Only)".center(100))
    print("=" * 100)

    scrapers = [TrafilaturaScraper()]

    with DutchNewsScraperService(scrapers=scrapers) as service:
        return service.scrape_batch(fast_urls, progress_callback=print_progress)


def test_dpg_sites_only():
    """Test only DPG Media sites"""
    dpg_urls = {
        k: v
        for k, v in TEST_URLS.items()
        if k in ["AD", "ED", "BD", "Gelderlander", "Destentor"]
    }

    print("\n" + "=" * 100)
    print("DPG MEDIA SITES TEST".center(100))
    print("=" * 100)

    scrapers = [DPGMediaScraper(headless=True, wait_time=6)]

    with DutchNewsScraperService(scrapers=scrapers) as service:
        return service.scrape_batch(dpg_urls, progress_callback=print_progress)


# ============================================================================
# Main Entry Point
# ============================================================================

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        mode = sys.argv[1].lower()

        if mode == "full":
            run_comprehensive_test()
        elif mode == "quick":
            run_quick_test()
        elif mode == "fast":
            test_fast_sites_only()
        elif mode == "dpg":
            test_dpg_sites_only()
        else:
            print(f"Unknown mode: {mode}")
            print("\nUsage: python test_all_domains.py [full|quick|fast|dpg]")
            print("  full  - Test all 14 domains (takes ~5-10 minutes)")
            print("  quick - Test 3 representative sites (takes ~30 seconds)")
            print("  fast  - Test only fast sites with Trafilatura (takes ~10 seconds)")
            print("  dpg   - Test only DPG Media sites (takes ~2-3 minutes)")
    else:
        print("\n" + "=" * 100)
        print("Dutch News Scraper - Comprehensive Test Suite".center(100))
        print("=" * 100)
        print("\nAvailable test modes:")
        print("  python test_all_domains.py full   - Test all 14 domains (~5-10 min)")
        print("  python test_all_domains.py quick  - Test 3 sites (~30 sec)")
        print("  python test_all_domains.py fast   - Test fast sites only (~10 sec)")
        print("  python test_all_domains.py dpg    - Test DPG Media sites (~2-3 min)")
        print("\nRecommendation: Start with 'quick' to verify setup, then run 'full'")
        print("\nRunning QUICK test by default...\n")

        run_quick_test()

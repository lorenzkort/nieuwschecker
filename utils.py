from pathlib import Path

DEFAULT_RSS_URLS_PATH = Path('data/seeds/default_rss_urls.csv')

def parse_default_rss_urls():
    lines = []
    if not DEFAULT_RSS_URLS_PATH.exists():
        return
    with open(DEFAULT_RSS_URLS_PATH, 'r') as f:
        lines = f.readlines()
    return lines

DEFAULT_RSS_URLS = parse_default_rss_urls()
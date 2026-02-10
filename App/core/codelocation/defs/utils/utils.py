from pathlib import Path

from dagster import EnvVar, get_dagster_logger

logging = get_dagster_logger()

DATA_DIR = Path(EnvVar("DATA_DIR").get_value() or "")


def parse_default_rss_urls() -> list:
    DEFAULT_RSS_URLS_PATH = DATA_DIR / "seeds/default_rss_urls.csv"
    if not DEFAULT_RSS_URLS_PATH.exists():
        logging.error(f"{DEFAULT_RSS_URLS_PATH} does not exist")
        return []
    with open(DEFAULT_RSS_URLS_PATH, "r") as f:
        lines = f.readlines()
    return lines


DEFAULT_RSS_URLS = parse_default_rss_urls()


def html_to_text(html: str) -> str:
    """Parse HTML and return clean text, removing scripts and styles."""
    from bs4 import BeautifulSoup

    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)

import dagster as dg
import polars as pl
from datetime import datetime
from typing import Dict, Any, List
from urllib.parse import urlparse
from feedparser import FeedParserDict

logging = dg.get_dagster_logger()


def ingest_rss_to_text(feed_url: str) -> FeedParserDict:
    """Fetch and parse an RSS feed, raising an error if parsing fails."""
    from feedparser import parse
    feed = parse(
        feed_url,
        agent='',
        request_headers={
            'Accept': 'application/rss+xml, application/xml, text/xml',
            'Accept-Language': 'nl-NL,nl;q=0.9',
        }
    )
    if feed.bozo:
        raise ValueError(f"Failed to parse RSS feed: {feed.bozo_exception}")
    return feed


def parse_article(article: Dict[str, Any]) -> Dict[str, Any]:
    """Parse a single NOS article into a structured dict with features."""
    from utils.utils import html_to_text
    t = article.get("published_parsed")  # type: ignore
    publish_date = datetime(*t[:6]) if t else None

    title = article.get("title")
    link = article.get("link")
    summary_html = article.get("summary_detail", {}).get("value", "")
    if not summary_html:
        # Get description, but ensure it's a string
        description = article.get("description", "")
        summary_html = description if isinstance(description, str) else ""

    summary = html_to_text(summary_html) if summary_html else ""

    return {
        "publish_date": publish_date,
        "title": title,
        "link": link,
        "base_url": urlparse(link)[1],
        "summary": summary,
    }

def articles_to_df(feed_url: str) -> pl.DataFrame:
    """Fetch RSS feed, parse articles, and return a Polars DataFrame."""
    logging.info(f"parsing {feed_url}")
    feed = ingest_rss_to_text(feed_url)
    rows: List[Dict[str, Any]] = []

    entries = feed.get("entries") or []
    rows = [parse_article(article) for article in entries]

    return pl.DataFrame(rows)

@dg.asset(key_prefix='raw')
def rss_feeds_latest() -> pl.DataFrame:
    """Fetch the latest RSS feeds without partitioning."""
    from utils.utils import DEFAULT_RSS_URLS
    
    dfs = [articles_to_df(url) for url in DEFAULT_RSS_URLS or []]
    combined_df = pl.concat(dfs).unique(subset=["link"], keep="first")
    
    # Add ingestion timestamp
    combined_df = combined_df.with_columns(
        pl.lit(datetime.now()).alias("ingestion_timestamp")
    ).filter(pl.col("publish_date").is_not_nan())
    
    return combined_df

@dg.asset(
    key_prefix='raw',
    deps=[rss_feeds_latest],
    metadata={
        "mode": "overwrite",
        "delta_write_options": {
            "schema_mode": "overwrite"
        }
    }
)
def rss_feeds_historic(context: dg.AssetExecutionContext, rss_feeds_latest: pl.DataFrame) -> pl.DataFrame:
    """Maintain a historic table with unique links."""
    # Try to load the previous materialization of this asset
    try:
        # This loads the previous version of THIS asset (self-dependency pattern)
        historic_df = context.load_asset_value(
            asset_key=dg.AssetKey(["raw", "rss_feeds_historic"])
        )
    except Exception as e:
        context.log.warning(f"No previous historic data found: {e}")
        historic_df = pl.DataFrame()
    
    if historic_df.is_empty():
        return rss_feeds_latest
    
    # Merge new data with historic, keeping first occurrence of each link
    combined = pl.concat([historic_df, rss_feeds_latest])
    return combined.unique(subset=["link"], keep="first")

if __name__ == "__main__":
    feed_url = 'https://www.telegraaf.nl/rss/'
    print(
        articles_to_df(feed_url=feed_url)
    )
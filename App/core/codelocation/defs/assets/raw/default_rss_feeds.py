from datetime import datetime
from typing import Any, Dict, List
from urllib.parse import urlparse

import dagster as dg
import polars as pl
from feedparser import FeedParserDict

logging = dg.get_dagster_logger()


def ingest_rss_to_text(feed_url: str) -> FeedParserDict:
    """Fetch and parse an RSS feed, raising an error if parsing fails."""
    from feedparser import parse

    feed = parse(
        feed_url,
        agent="",
        request_headers={
            "Accept": "application/rss+xml, application/xml, text/xml",
            "Accept-Language": "nl-NL,nl;q=0.9",
        },
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
    base_url = urlparse(link)[1]
    summary_html = article.get("summary_detail", {}).get("value", "")
    if not summary_html:
        # Get description, but ensure it's a string
        description = article.get("description", "")
        summary_html = description if isinstance(description, str) else ""

    summary = html_to_text(summary_html) if summary_html else ""
    image_url = None

    image_url = list(article.get("links", []))
    image_url = next(
        (l.get("href") for l in image_url if l.get("type", "").startswith("image/")),
        None,
    )
    if image_url is None:
        # Fallback to media_content if no image in links
        media_content = article.get("media_content", [])
        if media_content:
            image_url = media_content[0].get("url")
    if image_url is None:
        # metro type image
        summary_html = article.get("summary_detail", {}).get("value", "")
        if summary_html:
            from bs4 import BeautifulSoup

            soup = BeautifulSoup(summary_html, "html.parser")
            img_tag = soup.find("img")
            if img_tag and img_tag.get("src"):
                image_url = img_tag.get("src")

    print(image_url)

    return {
        "publish_date": publish_date,
        "title": title,
        "link": link,
        "base_url": base_url,
        "summary": summary,
        "image_url": image_url,
    }


def rss_to_df(feed_url: str) -> pl.DataFrame:
    """Fetch RSS feed, parse articles, and return a Polars DataFrame."""
    logging.info(f"parsing {feed_url}")
    try:
        feed = ingest_rss_to_text(feed_url)
        rows: List[Dict[str, Any]] = []

        entries = feed.get("entries") or []
        rows = [parse_article(article) for article in entries]

        return pl.DataFrame(rows)
    except Exception as e:
        logging.warning(f'Could not ingest RSS-feed from "{feed_url}": {e}')
        return pl.DataFrame()


@dg.asset(key_prefix="raw")
def rss_feeds_latest() -> pl.DataFrame:
    """Fetch the latest RSS feeds without partitioning."""
    from utils.utils import DEFAULT_RSS_URLS

    dfs = [rss_to_df(url) for url in DEFAULT_RSS_URLS or []]
    debug_dfs = [(url, len(df)) for url, df in zip(DEFAULT_RSS_URLS, dfs)]
    print(debug_dfs)
    combined_df = pl.concat(dfs).unique(subset=["link"], keep="first")

    # Add ingestion timestamp
    combined_df = combined_df.with_columns(
        pl.lit(datetime.now()).alias("ingestion_timestamp")
    ).filter(pl.col("publish_date").is_not_null())

    return combined_df


@dg.asset(
    key_prefix="raw",
    deps=[rss_feeds_latest],
    metadata={"mode": "overwrite", "delta_write_options": {"schema_mode": "overwrite"}},
)
def rss_feeds_historic(
    context: dg.AssetExecutionContext, rss_feeds_latest: pl.DataFrame
) -> pl.DataFrame:
    """Maintain a historic table with unique links."""
    # Try to load the previous materialization of this asset
    try:
        # This loads the previous version of THIS asset (self-dependency pattern)
        historic_df = context.load_asset_value(
            asset_key=dg.AssetKey(["raw", "rss_feeds_historic"])
        )
    except Exception as e:
        context.log.warning(f"Error loading asset data within function: {e}")
        return rss_feeds_latest

    # Merge new data with historic, keeping first occurrence of each link
    combined = pl.concat([historic_df, rss_feeds_latest])
    return combined.unique(subset=["link"], keep="first")


if __name__ == "__main__":
    rss_feeds_latest()

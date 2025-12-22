import feedparser
from bs4 import BeautifulSoup
from features import gen_features
from datetime import datetime
import polars as pl
from typing import Dict, Any, List
from urllib.parse import urlparse


def ingest_rss_to_text(feed_url: str) -> feedparser.FeedParserDict:
    """Fetch and parse an RSS feed, raising an error if parsing fails."""
    feed = feedparser.parse(feed_url)
    if feed.bozo:
        raise ValueError(f"Failed to parse RSS feed: {feed.bozo_exception}")
    return feed


def parse_html(html: str) -> str:
    """Parse HTML and return clean text, removing scripts and styles."""
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    return soup.get_text(separator=" ", strip=True)


def parse_nos_article(article: Dict[str, Any]) -> Dict[str, Any]:
    """Parse a single NOS article into a structured dict with features."""
    t = article.get("published_parsed")  # type: ignore
    publish_date = datetime(*t[:6]) if t else None

    title = article.get("title")
    link = article.get("link")
    summary_html = article.get("summary_detail", {}).get("value", "")
    summary = parse_html(summary_html)

    features = gen_features(title=title, summary=summary)
    return {
        "publish_date": publish_date,
        "title": title,
        "link": link,
        "base_url": urlparse(link)[1],
        "summary": summary,
        "features": features,
    }


def article_to_row(article_dict: Dict[str, Any]) -> Dict[str, Any]:
    """Flatten the features dict into Polars-compatible row."""
    features = article_dict["features"]

    return {
        "publish_date": article_dict["publish_date"],
        "title": article_dict["title"],
        "link": article_dict["link"],
        "base_url": article_dict["base_url"],
        "summary": article_dict["summary"],
        "embedding": features["embedding"].tolist(),  # numpy → list
        "keywords": features["keywords"],
        "entities": features["entities"],
        "token_count": features["token_count"],
        "char_count": features["char_count"],
    }


def articles_to_df(feed_url: str) -> pl.DataFrame:
    """Fetch RSS feed, parse articles, and return a Polars DataFrame."""
    feed = ingest_rss_to_text(feed_url)
    rows: List[Dict[str, Any]] = []

    entries = feed.get("entries")
    if entries != None:
        for article in entries:
            parsed_article = parse_nos_article(article)
            row = article_to_row(parsed_article)
            rows.append(row)

    return pl.DataFrame(rows)

def parse_rss_feeds(feed_urls: List) -> pl.DataFrame:
    dfs = [articles_to_df(url) for url in feed_urls]
    return pl.union(dfs) 

if __name__ == "__main__":
    feed_urls = [
        "https://feeds.nos.nl/nosnieuwsalgemeen",
        "https://www.ad.nl/home/rss.xml"
    ]
    df = parse_rss_feeds(feed_urls)
    print(df)
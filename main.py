import feedparser
from pathlib import Path
from bs4 import BeautifulSoup
from features import gen_features

def ingest_rss_to_text(feed_url: str, output_file: Path) -> feedparser.FeedParserDict:
    feed = feedparser.parse(feed_url)

    if feed.bozo:
        raise ValueError(f"Failed to parse RSS feed: {feed.bozo_exception}")
    return feed

def parse_html(html) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # Remove junk explicitly
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    parsed_text = soup.get_text(separator=" ", strip=True)
    
    return parsed_text

def parse_nos_article(article):
    publish_date = article.get("published_parsed")
    title = article.get("title")
    link = article.get("link")
    summary = parse_html(article.get("summary_detail").get("value"))
    features = gen_features(title=title, summary=summary)
    del(article)
    return locals()

if __name__ == "__main__":
    RSS_FEED_URL = "https://feeds.nos.nl/nosnieuwsalgemeen"
    feed = ingest_rss_to_text(RSS_FEED_URL, Path(''))
    
    for article in feed["entries"]:
        parsed_article = parse_nos_article(article)
        print(parsed_article)
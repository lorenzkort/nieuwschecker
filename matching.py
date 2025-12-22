import polars as pl
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity

def match_articles(df: pl.DataFrame, threshold: float = 0.7) -> pl.DataFrame:
    """
    Compare articles from two different feeds in a dataframe and match them
    with a confidence score using cosine similarity of embeddings.
    
    Args:
        df: Polars DataFrame containing at least 'base_url', 'title', and 'embedding'.
        threshold: Minimum similarity score to consider a match.
        
    Returns:
        DataFrame with columns: feed_1_title, feed_2_title, similarity
    """
    # Split dataframe by base_url
    urls = df["base_url"].unique().to_list()
    if len(urls) < 2:
        raise ValueError("DataFrame must contain at least two distinct feeds.")
    
    df1 = df.filter(pl.col("base_url") == urls[0])
    df2 = df.filter(pl.col("base_url") == urls[1])

    matches = []

    embeddings1 = np.vstack(df1["embedding"].to_list())
    embeddings2 = np.vstack(df2["embedding"].to_list())

    # Compute pairwise cosine similarity
    sim_matrix = cosine_similarity(embeddings1, embeddings2)

    for i, row1 in enumerate(df1.to_dicts()):
        for j, row2 in enumerate(df2.to_dicts()):
            score = sim_matrix[i, j]
            if score >= threshold:
                matches.append({
                    "feed_1_title": row1["title"],
                    "feed_1_summary": row1["summary"],
                    "feed_2_title": row2["title"],
                    "feed_2_summary": row2["summary"],
                    "similarity": float(score)
                })

    return pl.DataFrame(matches)


if __name__ == "__main__":
    # Quick test
    from rss_parser import parse_rss_feeds  # your main script

    feed_urls = [
        "https://feeds.nos.nl/nosnieuwsalgemeen",
        "https://www.ad.nl/home/rss.xml"
    ]
    df = parse_rss_feeds(feed_urls)

    matched_df = match_articles(df, threshold=0.65)
    print(matched_df.sort("similarity").head(10).write_json())

import polars as pl
import numpy as np
from sklearn.metrics.pairwise import cosine_similarity
from collections import defaultdict


def cluster_articles(df: pl.DataFrame, threshold: float = 0.7) -> pl.DataFrame:
    """
    Cluster similar articles across multiple RSS feeds using cosine similarity.
    
    Args:
        df: Polars DataFrame containing 'base_url', 'title', 'link', 'summary', and 'embedding'.
        threshold: Minimum similarity score to consider articles as matching.
        
    Returns:
        DataFrame with one row per cluster containing:
        - cluster_id: Unique identifier for the cluster
        - title: Representative title (from the first article in cluster)
        - articles: List of dicts with all articles in the cluster
        - num_articles: Count of articles in cluster
        - feeds: List of unique feeds represented in cluster
    """
    if df.height == 0:
        return pl.DataFrame({
            "cluster_id": [],
            "title": [],
            "articles": [],
            "num_articles": [],
            "feeds": []
        })
    
    # Validate required columns
    required_cols = ["base_url", "title", "link", "embedding"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")
    
    # Extract embeddings and compute similarity matrix
    embeddings = np.vstack(df["embedding"].to_list())
    sim_matrix = cosine_similarity(embeddings)
    
    # Build clusters using Union-Find approach
    n = df.height
    parent = list(range(n))
    
    def find(x: int) -> int:
        """Find root of element x with path compression."""
        if parent[x] != x:
            parent[x] = find(parent[x])
        return parent[x]
    
    def union(x: int, y: int) -> None:
        """Union two elements into same cluster."""
        root_x, root_y = find(x), find(y)
        if root_x != root_y:
            parent[root_y] = root_x
    
    # Group articles by similarity
    for i in range(n):
        for j in range(i + 1, n):
            if sim_matrix[i, j] >= threshold:
                union(i, j)
    
    # Build clusters dictionary
    clusters = defaultdict(list)
    articles = df.to_dicts()
    
    for idx, article in enumerate(articles):
        root = find(idx)
        clusters[root].append({
            "title": article["title"],
            "link": article["link"],
            "feed": article["base_url"],
            "summary": article.get("summary", ""),
            
        })
    
    # Convert clusters to output format
    cluster_data = []
    for cluster_id, (root_idx, articles_in_cluster) in enumerate(clusters.items(), 1):
        # Use the title from the first article as representative
        representative_title = articles_in_cluster[0]["title"]
        
        # Get unique feeds
        feeds = list(set(art["feed"] for art in articles_in_cluster))
        
        cluster_data.append({
            "cluster_id": cluster_id,
            "title": representative_title,
            "articles": articles_in_cluster,
            "num_articles": len(articles_in_cluster),
            "feeds": feeds,
            "num_feeds": len(feeds)
        })
    
    # Sort by number of articles (largest clusters first)
    result_df = pl.DataFrame(cluster_data).sort("num_articles", descending=True)
    
    return result_df


def get_cross_feed_clusters(df: pl.DataFrame, threshold: float = 0.7, min_feeds: int = 2) -> pl.DataFrame:
    """
    Get only clusters that span multiple feeds (true duplicates/similar stories).
    
    Args:
        df: Polars DataFrame with article data
        threshold: Similarity threshold for clustering
        min_feeds: Minimum number of different feeds required in a cluster
        
    Returns:
        DataFrame with only cross-feed clusters
    """
    all_clusters = cluster_articles(df, threshold)
    
    if all_clusters.height == 0:
        return all_clusters
    
    # Filter to clusters that appear in multiple feeds
    cross_feed = all_clusters.filter(pl.col("num_feeds") >= min_feeds)
    
    return cross_feed


def export_clusters_readable(clusters_df: pl.DataFrame, output_path: str = "clusters.json") -> None:
    """
    Export clusters in a more readable JSON format.
    
    Args:
        clusters_df: DataFrame from cluster_articles()
        output_path: Path to save JSON file
    """
    clusters_df.write_json(output_path)
    print(f"Exported {clusters_df.height} clusters to {output_path}")


def print_cluster_summary(clusters_df: pl.DataFrame) -> None:
    """
    Print a human-readable summary of clusters.
    """
    if clusters_df.height == 0:
        print("No clusters found.")
        return
    
    print(f"\n{'='*80}")
    print(f"Found {clusters_df.height} article clusters")
    print(f"{'='*80}\n")
    
    for row in clusters_df.iter_rows(named=True):
        print(f"Cluster {row['cluster_id']}: {row['title'][:80]}...")
        print(f"  Articles: {row['num_articles']} | Feeds: {row['num_feeds']}")
        print(f"  Sources: {', '.join([feed.split('/')[-1] for feed in row['feeds']])}")
        
        for article in row['articles']:
            feed_name = article['feed'].split('/')[-1][:20]
            print(f"    - [{feed_name}] {article['title'][:60]}...")
        print()


if __name__ == "__main__":
    # Example usage with multiple feeds
    from rss_parser import parse_rss_feeds
    from utils import DEFAULT_RSS_URLS
    feed_urls = DEFAULT_RSS_URLS
    
    print("Fetching and parsing RSS feeds...")
    df = parse_rss_feeds(feed_urls)
    print(f"Loaded {df.height} articles from {df['base_url'].n_unique()} feeds\n")
    
    # Get all clusters
    print("Clustering articles...")
    all_clusters = cluster_articles(df, threshold=0.8)
    
    # Get only cross-feed clusters (stories covered by multiple sources)
    cross_feed_clusters = get_cross_feed_clusters(df, threshold=0.8, min_feeds=2)
    
    # Display results
    print_cluster_summary(cross_feed_clusters)
    
    # Export to JSON
    export_clusters_readable(all_clusters, "data/output/all_clusters.json")
    export_clusters_readable(cross_feed_clusters, "data/output/cross_feed_clusters.json")
    
    # Statistics
    total_articles = df.height
    clustered_articles = sum(c["num_articles"] for c in all_clusters.to_dicts())
    print(f"\nStatistics:")
    print(f"  Total articles: {total_articles}")
    print(f"  Articles in clusters: {clustered_articles}")
    print(f"  Total clusters: {all_clusters.height}")
    print(f"  Cross-feed clusters: {cross_feed_clusters.height}")
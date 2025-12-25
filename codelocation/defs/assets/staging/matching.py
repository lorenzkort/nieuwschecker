import dagster as dg
import polars as pl

logging = dg.get_dagster_logger()

def cluster_articles(df: pl.DataFrame, similarity_threshold: float = 0.7) -> pl.DataFrame:
    """
    Cluster similar articles across multiple RSS feeds using cosine similarity.
    
    Args:
        df: Polars DataFrame containing 'base_url', 'title', 'link', 'summary', and 'embedding'.
        similarity_threshold: Minimum similarity score to consider articles as matching.
        
    Returns:
        DataFrame with one row per cluster containing:
        - cluster_id: Unique identifier for the cluster
        - title: Representative title (from the first article in cluster)
        - articles: List of dicts with all articles in the cluster
        - num_articles: Count of articles in cluster
        - feeds: List of unique feeds represented in cluster
    """
    from sklearn.metrics.pairwise import cosine_similarity
    from collections import defaultdict
    import numpy as np
    
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
            if sim_matrix[i, j] >= similarity_threshold:
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

@dg.asset(
    key_prefix="staging",
    ins={
        "add_features": dg.AssetIn(["staging", "add_features"])   
    }
)
def cross_feed_clusters(
    context: dg.AssetExecutionContext,
    add_features: pl.DataFrame,
) -> pl.DataFrame:
    """
    Get only clusters that span multiple feeds (true duplicates/similar stories).
    
    Args:
        df: Polars DataFrame with article data
        similarity_threshold: Similarity similarity_threshold for clustering
        min_feeds: Minimum number of different feeds required in a cluster
        
    Returns:
        DataFrame with only cross-feed clusters
    """ 
    min_feeds: int = 2
    similarity_threshold: float = 0.7
    
    all_clusters = cluster_articles(add_features, similarity_threshold)
    
    # Filter to clusters that appear in multiple feeds
    cross_feed = all_clusters.filter(pl.col("num_feeds") >= min_feeds)

    # Create metadata for tracking
    metadata = {}
    
    # Summary metadata
    metadata["cluster_count"] = cross_feed.height
    metadata["total_articles"] = cross_feed["num_articles"].sum() if cross_feed.height > 0 else 0
    metadata["avg_articles_per_cluster"] = cross_feed["num_articles"].mean() if cross_feed.height > 0 else 0
    metadata["avg_feeds_per_cluster"] = cross_feed["num_feeds"].mean() if cross_feed.height > 0 else 0
    
    # Create cluster summary table
    if cross_feed.height > 0:
        cluster_records = []
        for row in cross_feed.iter_rows(named=True):
            cluster_records.append({
                "cluster_id": row['cluster_id'],
                "title_preview": row['title'][:80] + "...",
                "num_articles": row['num_articles'],
                "num_feeds": row['num_feeds'],
                "sources": ', '.join([feed.split('/')[-1] for feed in row['feeds']])
            })
        
        # Add cluster summary table as metadata
        metadata["cluster_summary"] = {
            "type": "table",
            "raw_value": {
                "schema": [
                    {"name": "cluster_id", "type": "int"},
                    {"name": "title_preview", "type": "string"},
                    {"name": "num_articles", "type": "int"},
                    {"name": "num_feeds", "type": "int"},
                    {"name": "sources", "type": "string"}
                ],
                "records": cluster_records
            }
        }
        
        # Create detailed articles table
        article_records = []
        for row in cross_feed.iter_rows(named=True):
            for article in row['articles']:
                article_records.append({
                    "cluster_id": row['cluster_id'],
                    "feed_name": article['feed'].split('/')[-1][:20],
                    "article_title": article['title'][:60] + "..."
                })
        
        # Add articles detail table as metadata
        metadata["article_details"] = {
            "type": "table",
            "raw_value": {
                "schema": [
                    {"name": "cluster_id", "type": "int"},
                    {"name": "feed_name", "type": "string"},
                    {"name": "article_title", "type": "string"}
                ],
                "records": article_records[:20]  # Limit to first 100 for display
            }
        }
        
        # Add a markdown summary
        summary_text = f"""
# Clustering Summary

- **Total Clusters Found**: {cross_feed.height}
- **Total Articles**: {metadata['total_articles']}
- **Avg Articles per Cluster**: {metadata['avg_articles_per_cluster']:.1f}
- **Avg Feeds per Cluster**: {metadata['avg_feeds_per_cluster']:.1f}
        """
        
        metadata["summary"] = {
            "type": "md",
            "raw_value": summary_text
        }
    else:
        metadata["summary"] = {
            "type": "md", 
            "raw_value": "No clusters found."
        }
    
    # Log the metadata
    context.add_output_metadata(metadata)
    
    return cross_feed

import dagster as dg
import polars as pl
from datetime import datetime, timedelta

logging = dg.get_dagster_logger()

def cluster_articles(
    df: pl.DataFrame, 
    similarity_threshold: float = 0.7,
    max_time_window_hours: int | None = None
) -> pl.DataFrame:
    """
    Cluster similar articles across multiple RSS feeds using cosine similarity.
    
    Args:
        df: Polars DataFrame containing 'base_url', 'title', 'link', 'summary', 'embedding', 
            and optionally 'publish_date' for time-based clustering.
        similarity_threshold: Minimum similarity score to consider articles as matching.
        max_time_window_hours: If provided, only cluster articles publish_date within this time window.
        
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
    
    # Check if time-based clustering is requested
    use_time_constraint = max_time_window_hours is not None and "publish_date" in df.columns
    
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
    
    # Group articles by similarity (and optionally time)
    for i in range(n):
        for j in range(i + 1, n):
            # Check similarity threshold
            if sim_matrix[i, j] >= similarity_threshold:
                # If time constraint is enabled, check time window
                if use_time_constraint and max_time_window_hours is not None:
                    pub_i = df[i, "publish_date"]
                    pub_j = df[j, "publish_date"]
                    
                    # Handle both datetime and string formats
                    if isinstance(pub_i, str):
                        pub_i = datetime.fromisoformat(pub_i.replace('Z', '+00:00'))
                    if isinstance(pub_j, str):
                        pub_j = datetime.fromisoformat(pub_j.replace('Z', '+00:00'))
                    
                    time_diff_hours = abs((pub_i - pub_j).total_seconds() / 3600)
                    
                    if time_diff_hours <= max_time_window_hours:
                        union(i, j)
                else:
                    union(i, j)
    
    # Build clusters dictionary
    clusters = defaultdict(list)
    articles = df.to_dicts()
    
    for idx, article in enumerate(articles):
        root = find(idx)
        article_data = {
            "title": article["title"],
            "link": article["link"],
            "feed": article["base_url"],
        }
        # Include publish_date date if available
        if "publish_date" in article:
            article_data["publish_date"] = article["publish_date"]
        
        clusters[root].append(article_data)
    
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


def reconstruct_df_from_articles(
    articles: list[dict], 
    original_df: pl.DataFrame
) -> pl.DataFrame:
    """
    Reconstruct a DataFrame from a list of article dictionaries.
    Used to re-cluster subsets of articles.
    
    Args:
        articles: List of article dictionaries from a cluster
        original_df: Original DataFrame to extract embeddings and other data
        
    Returns:
        New DataFrame containing only the specified articles
    """
    # Extract links to identify which rows to keep
    links = [art["link"] for art in articles]
    
    # Filter original dataframe to only these articles
    filtered_df = original_df.filter(pl.col("link").is_in(links))
    
    return filtered_df


def two_stage_cluster(
    df: pl.DataFrame,
    stage1_threshold: float = 0.6,
    stage2_threshold: float = 0.85,
    max_cluster_size: int = 10,
    max_time_window_hours: int | None = 24
) -> pl.DataFrame:
    """
    Perform two-stage clustering to handle both broad topics and specific events.
    
    Stage 1: Broad topical clustering (lower threshold)
    Stage 2: Refined clustering within large clusters (higher threshold + time constraints)
    
    Args:
        df: Input DataFrame with articles
        stage1_threshold: Similarity threshold for initial broad clustering
        stage2_threshold: Stricter threshold for refining large clusters
        max_cluster_size: Clusters larger than this will be re-clustered
        max_time_window_hours: Time window for stage 2 clustering (None to disable)
        
    Returns:
        DataFrame with refined clusters
    """
    logging.info(f"Starting two-stage clustering on {df.height} articles")
    
    # Stage 1: Broad topical clustering
    logging.info(f"Stage 1: Broad clustering with threshold {stage1_threshold}")
    broad_clusters = cluster_articles(df, similarity_threshold=stage1_threshold)
    
    logging.info(f"Stage 1 produced {broad_clusters.height} clusters")
    
    # Stage 2: Refine large clusters
    refined_clusters = []
    clusters_refined = 0
    
    for row in broad_clusters.iter_rows(named=True):
        if row['num_articles'] > max_cluster_size:
            logging.info(
                f"Refining large cluster (id={row['cluster_id']}, "
                f"size={row['num_articles']}) with threshold {stage2_threshold}"
            )
            
            # Reconstruct dataframe for this cluster
            sub_df = reconstruct_df_from_articles(row['articles'], df)
            
            # Re-cluster with stricter criteria and time constraint
            sub_clusters = cluster_articles(
                sub_df, 
                similarity_threshold=stage2_threshold,
                max_time_window_hours=max_time_window_hours
            )
            
            logging.info(
                f"Split into {sub_clusters.height} sub-clusters"
            )
            
            refined_clusters.extend(sub_clusters.to_dicts())
            clusters_refined += 1
        else:
            # Keep small clusters as-is
            refined_clusters.append(row)
    
    logging.info(
        f"Stage 2: Refined {clusters_refined} large clusters. "
        f"Total clusters now: {len(refined_clusters)}"
    )
    
    # Rebuild dataframe and reassign cluster IDs
    result_df = pl.DataFrame(refined_clusters)
    
    if result_df.height > 0:
        # Reassign cluster IDs sequentially
        result_df = result_df.select(pl.exclude("cluster_id")).with_row_index(name="cluster_id").sort("num_articles", descending=True)
        
    return result_df


@dg.asset(
    key_prefix="staging",
    ins={
        "add_features": dg.AssetIn(["staging", "add_features"])   
    }
)
def cross_feed_clusters(
    add_features: pl.DataFrame,
) -> pl.DataFrame:
    """
    Get only clusters that span multiple feeds (true duplicates/similar stories).
    Uses two-stage clustering to handle both broad topics and specific events.
    
    Args:
        add_features: Polars DataFrame with article data including embeddings
        
    Returns:
        DataFrame with only cross-feed clusters
    """ 
    min_feeds: int = 2
    
    # Two-stage clustering parameters
    stage1_threshold: float = 0.6  # Broad topical grouping
    stage2_threshold: float = 0.85  # Refined event-specific clustering
    max_cluster_size: int = 10  # Trigger refinement above this size
    max_time_window_hours: int = 24  # Only cluster articles within 24 hours
    
    logging.info("Starting two-stage clustering pipeline")
    
    # Perform two-stage clustering
    all_clusters = two_stage_cluster(
        add_features,
        stage1_threshold=stage1_threshold,
        stage2_threshold=stage2_threshold,
        max_cluster_size=max_cluster_size,
        max_time_window_hours=max_time_window_hours
    )
    
    # Filter to clusters that appear in multiple feeds
    cross_feed = all_clusters.filter(pl.col("num_feeds") >= min_feeds)
    
    logging.info(
        f"Filtered to {cross_feed.height} cross-feed clusters "
        f"(from {all_clusters.height} total clusters)"
    )
    
    # Add min and max publish dates as columns
    if cross_feed.height > 0:
        def extract_dates(articles_list):
            """Extract min and max publish dates from articles."""
            if not articles_list or 'publish_date' not in articles_list[0]:
                return None, None
            
            pub_dates = [art.get('publish_date') for art in articles_list if art.get('publish_date')]
            if not pub_dates:
                return None, None
            
            min_date = min(pub_dates)
            max_date = max(pub_dates)
            
            # Convert to datetime if needed
            if isinstance(min_date, str):
                min_date = datetime.fromisoformat(min_date.replace('Z', '+00:00'))
            if isinstance(max_date, str):
                max_date = datetime.fromisoformat(max_date.replace('Z', '+00:00'))
            
            return min_date, max_date
        
        # Extract dates for each row
        min_dates = []
        max_dates = []
        time_spans = []
        
        for row in cross_feed.iter_rows(named=True):
            min_date, max_date = extract_dates(row['articles'])
            min_dates.append(min_date)
            max_dates.append(max_date)
            
            # Calculate time span in hours
            if min_date and max_date:
                time_span = (max_date - min_date).total_seconds() / 3600
                time_spans.append(time_span)
            else:
                time_spans.append(None)
        
        # Add as new columns
        cross_feed = cross_feed.with_columns([
            pl.Series("min_published_date", min_dates),
            pl.Series("max_published_date", max_dates),
            pl.Series("time_span_hours", time_spans)
        ])

    # Create metadata for tracking
    metadata = {}
    
    # Summary metadata
    metadata["cluster_count"] = cross_feed.height
    metadata["total_articles"] = cross_feed["num_articles"].sum() if cross_feed.height > 0 else 0
    metadata["avg_articles_per_cluster"] = cross_feed["num_articles"].mean() if cross_feed.height > 0 else 0
    metadata["avg_feeds_per_cluster"] = cross_feed["num_feeds"].mean() if cross_feed.height > 0 else 0
    
    # Add configuration metadata
    metadata["clustering_config"] = {
        "stage1_threshold": stage1_threshold,
        "stage2_threshold": stage2_threshold,
        "max_cluster_size": max_cluster_size,
        "max_time_window_hours": max_time_window_hours,
        "min_feeds": min_feeds
    }
    
    # Create cluster summary table
    if cross_feed.height > 0:
        cluster_records = []
        for row in cross_feed.iter_rows(named=True):
            # Get time range if publish_date dates are available
            time_range = ""
            min_published = None
            max_published = None
            
            if row['articles'] and 'publish_date' in row['articles'][0]:
                pub_dates = [art.get('publish_date') for art in row['articles'] if art.get('publish_date')]
                if pub_dates:
                    min_date = min(pub_dates)
                    max_date = max(pub_dates)
                    
                    # Convert to datetime if needed
                    if isinstance(min_date, str):
                        min_date = datetime.fromisoformat(min_date.replace('Z', '+00:00'))
                    if isinstance(max_date, str):
                        max_date = datetime.fromisoformat(max_date.replace('Z', '+00:00'))
                    
                    time_span = round((max_date - min_date).total_seconds() / 3600, 2)
                    time_range = f"{time_span:.1f}h"
                    
                    # Store as ISO format strings for the table
                    min_published = min_date.isoformat()
                    max_published = max_date.isoformat()
            
            cluster_records.append({
                "cluster_id": row['cluster_id'],
                "title_preview": row['title'][:80] + "..." if len(row['title']) > 80 else row['title'],
                "num_articles": row['num_articles'],
                "num_feeds": row['num_feeds'],
                "time_span": time_range,
                "min_published_date": min_published,
                "max_published_date": max_published,
                "sources": ', '.join([feed.split('/')[-1] for feed in row['feeds'][:3]])  # Show first 3 sources
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
                    {"name": "time_span", "type": "string"},
                    {"name": "min_published_date", "type": "string"},
                    {"name": "max_published_date", "type": "string"},
                    {"name": "sources", "type": "string"}
                ],
                "records": cluster_records
            }
        }
    
    return cross_feed
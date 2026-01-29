from datetime import datetime, timedelta

import dagster as dg
import polars as pl

logging = dg.get_dagster_logger()


def cluster_articles(
    df: pl.DataFrame,
    similarity_threshold: float = 0.8,
    max_time_window_hours: int | None = None,
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
    from collections import defaultdict

    import numpy as np
    from sklearn.metrics.pairwise import cosine_similarity

    if df.height == 0:
        return pl.DataFrame(
            {
                "cluster_id": [],
                "title": [],
                "articles": [],
                "num_articles": [],
                "feeds": [],
            }
        )

    # Validate required columns
    required_cols = ["base_url", "title", "link", "embedding"]
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")

    # Check if time-based clustering is requested
    use_time_constraint = (
        max_time_window_hours is not None and "publish_date" in df.columns
    )

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

        cluster_data.append(
            {
                "cluster_id": cluster_id,
                "title": representative_title,
                "articles": articles_in_cluster,
                "num_articles": len(articles_in_cluster),
                "feeds": feeds,
                "num_feeds": len(feeds),
            }
        )

    # Sort by number of articles (largest clusters first)
    result_df = pl.DataFrame(cluster_data).sort("num_articles", descending=True)

    return result_df


def reconstruct_df_from_articles(
    articles: list[dict], original_df: pl.DataFrame
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
    max_cluster_size: int = 20,  # TODO change to nr. of feeds
    max_time_window_hours: int | None = 24,
) -> pl.DataFrame:
    """
    Perform two-stage clustering to handle both broad topics and specific events.

    Stage 1: Broad topical clustering (lower threshold) with time constraint
    Stage 2: Refined clustering within large clusters (higher threshold + time constraints)

    Args:
        df: Input DataFrame with articles
        stage1_threshold: Similarity threshold for initial broad clustering
        stage2_threshold: Stricter threshold for refining large clusters
        max_cluster_size: Clusters larger than this will be re-clustered
        max_time_window_hours: Time window for both stage 1 and stage 2 clustering (None to disable)

    Returns:
        DataFrame with refined clusters
    """
    logging.info(f"Starting two-stage clustering on {df.height} articles")

    # Stage 1: Broad topical clustering WITH time constraint
    logging.info(
        f"Stage 1: Broad clustering with threshold {stage1_threshold} and time window {max_time_window_hours}h"
    )
    broad_clusters = cluster_articles(
        df,
        similarity_threshold=stage1_threshold,
        max_time_window_hours=max_time_window_hours,  # Now applied to Stage 1
    )

    logging.info(f"Stage 1 produced {broad_clusters.height} clusters")

    # Stage 2: Refine large clusters
    refined_clusters = []
    clusters_refined = 0

    for row in broad_clusters.iter_rows(named=True):
        if row["num_articles"] > max_cluster_size:
            logging.info(
                f"Refining large cluster (id={row['cluster_id']}, "
                f"size={row['num_articles']}) with threshold {stage2_threshold}"
            )

            # Reconstruct dataframe for this cluster
            sub_df = reconstruct_df_from_articles(row["articles"], df)

            # Re-cluster with stricter criteria and time constraint
            sub_clusters = cluster_articles(
                sub_df,
                similarity_threshold=stage2_threshold,
                max_time_window_hours=max_time_window_hours,
            )

            logging.info(f"Split into {sub_clusters.height} sub-clusters")

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
        result_df = (
            result_df.select(pl.exclude("cluster_id"))
            .with_row_index(name="cluster_id")
            .sort("num_articles", descending=True)
        )

    return result_df


@dg.asset(
    key_prefix="staging",
    ins={"add_features": dg.AssetIn(["staging", "add_features"])},
    metadata={"mode": "overwrite", "delta_write_options": {"schema_mode": "overwrite"}},
)
def cross_feed_clusters(
    context: dg.AssetExecutionContext,
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
    stage1_threshold: float = 0.8  # Broad topical grouping
    stage2_threshold: float = 0.85  # Refined event-specific clustering
    max_cluster_size: int = 10  # Trigger refinement above this size
    max_time_window_hours: int = (
        24  # Only cluster articles within 24 hours (applied to BOTH stages)
    )
    cluster_merge_lookback_days: int = (
        7  # Include last X days of articles to avoid missing clusters
    )

    logging.info("Starting two-stage clustering pipeline")

    # Determine articles to process
    historic_cross_feed_clusters = context.load_asset_value(
        asset_key=dg.AssetKey(["staging", "cross_feed_clusters"])
    )
    # Add the last x days of articles to avoid missing clusters
    lookback_date = datetime.now() - timedelta(days=cluster_merge_lookback_days)
    articles_to_process = add_features.filter(pl.col("publish_date") >= lookback_date)

    # To avoid overlap of current and historic articles,
    # filter historic clusters to those before the lookback date
    historic_to_match = historic_cross_feed_clusters.filter(
        pl.col("max_published_date") < lookback_date
    )
    logging.info(f"Articles to process: {len(articles_to_process)}")

    # Perform two-stage clustering
    all_clusters = two_stage_cluster(
        articles_to_process,
        stage1_threshold=stage1_threshold,
        stage2_threshold=stage2_threshold,
        max_cluster_size=max_cluster_size,
        max_time_window_hours=max_time_window_hours,
    )

    # Filter to clusters that appear in multiple feeds
    cross_feed = all_clusters.filter(pl.col("num_feeds") >= min_feeds)

    logging.info(
        f"Filtered to {cross_feed.height} cross-feed clusters "
        f"(from {all_clusters.height} total clusters)"
    )

    # Add min and max publish dates as columns
    if cross_feed.height > 0:
        cross_feed = cross_feed.with_columns(
            min_published_date=pl.col("articles")
            .list.eval(pl.element().struct.field("publish_date"))
            .list.min(),
            max_published_date=pl.col("articles")
            .list.eval(pl.element().struct.field("publish_date"))
            .list.max(),
        ).with_columns(
            time_span_hours=(
                pl.col("max_published_date") - pl.col("min_published_date")
            )
            .dt.total_hours()
            .round(1)
        )

    # combine historic and new clusters
    return pl.concat([historic_to_match, cross_feed]).unique(
        subset=["title", "max_published_date"]
    )


if __name__ == "__main__":
    import polars as pl

    df = pl.read_parquet(
        "/Users/lorenzkort/Documents/LocalCode/news-data/data/staging/add_features.parquet"
    )
    clusters = cluster_articles(df)
    test_cluster = clusters.filter(pl.col("title").str.contains("Maduro"))

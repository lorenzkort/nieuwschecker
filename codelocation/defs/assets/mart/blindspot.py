from datetime import datetime, timedelta

import polars as pl

# --- Load data ---------------------------------------------------------------
timeline = pl.read_parquet(
    "/Users/lorenzkort/Documents/LocalCode/news-data/data/staging/timeline.parquet"
)

sentiments = pl.read_parquet(
    "/Users/lorenzkort/Documents/LocalCode/news-data/data/staging/sentiments.parquet"
)

# --- 1) Define 7-day cutoff --------------------------------------------------
now = datetime.now()
cutoff = now - timedelta(days=7)

# --- 2) Recent clusters ------------------------------------------------------
recent_clusters = timeline.filter(pl.col("max_published_date") >= cutoff)

# --- 3) Per-entity mention counts over last 7 days ---------------------------
# (no date in sentiments shown, so no time filter here)
entity_mentions = sentiments.group_by("entity").agg(
    pl.col("mention_count").sum().alias("total_mentions_7d")
)

# --- 4) Bottom 25% entities (low-mention) -----------------------------------
q25 = entity_mentions.select(pl.col("total_mentions_7d").quantile(0.25)).item()

low_mention_entities = entity_mentions.filter(
    pl.col("total_mentions_7d") <= q25
).select("entity")

# --- 5) Link clusters to entities via article links --------------------------
articles_long = recent_clusters.explode("articles").select(
    "cluster_id",
    pl.col("articles").struct.field("link").alias("link"),
)

cluster_entities = articles_long.join(
    sentiments.select("link", "entity"), on="link", how="inner"
).unique(["cluster_id", "entity"])

# --- 6) Clusters that have at least one low-mention entity -------------------
blindspot_clusters = (
    cluster_entities.join(low_mention_entities, on="entity", how="inner")
    .select("cluster_id")
    .unique()
)

# --- 7) Base result: recent clusters that are blindspots ---------------------
result = recent_clusters.join(blindspot_clusters, on="cluster_id", how="inner")

# --- 8) Compute side_ratio and quantile thresholds ---------------------------
recent_with_ratio = result.with_columns(
    side_ratio=(
        (pl.col("left") + pl.col("centre left"))
        / (pl.col("right") + pl.col("centre right"))
    ).round(2)
)

q10, q90 = recent_with_ratio.select(
    pl.col("side_ratio").quantile(0.10).alias("q10"),
    pl.col("side_ratio").quantile(0.90).alias("q90"),
).row(0)

# --- 9) Keep only clusters in tails and label left/right blindspots ---------
result_with_imbalance = (
    (
        recent_with_ratio.filter(
            (pl.col("side_ratio") <= q10) | (pl.col("side_ratio") >= q90)
        ).with_columns(
            blindspot_left=pl.when(pl.col("side_ratio") >= q90)
            .then(True)
            .otherwise(False),
            blindspot_right=pl.when(pl.col("side_ratio") <= q10)
            .then(True)
            .otherwise(False),
        )
    )
    .sort("num_articles", descending=True)
    .limit(20)
)

# result_with_imbalance now has:
# - side_ratio
# - blindspot_left  (True when left+centre-left >> right+centre-right)
# - blindspot_right (True when right+centre-right >> left+centre-left)

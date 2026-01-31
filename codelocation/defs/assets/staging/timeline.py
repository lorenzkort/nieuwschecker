import dagster as dg
from polars import DataFrame


@dg.asset(
    key_prefix="staging",
    ins={
        "cross_feed_clusters": dg.AssetIn(["staging", "cross_feed_clusters"]),
        "unique_feeds": dg.AssetIn(["staging", "unique_feeds"]),
        "news_agencies": dg.AssetIn(["raw", "news_agencies"]),
    },
)
def timeline(
    cross_feed_clusters: DataFrame, unique_feeds: DataFrame, news_agencies
) -> DataFrame:
    import polars as pl

    # 1) feeds: literal list of all unique feeds
    feeds = pl.lit(unique_feeds["base_url"].to_list())

    # 2) missing_feeds: set difference between global feeds and row-wise feeds
    missing_feeds = cross_feed_clusters.with_columns(
        missing_feeds=feeds.list.set_difference("feeds")
    )

    # 3) Build a lookup table from feed URL to owner/reach/left_right_label
    feed_lookup = news_agencies.select(
        pl.col("url").alias("feed"),
        pl.col("owner"),
        pl.col("reach"),
        pl.col("owner_reach"),
        pl.col("owner_agencies"),
        pl.col("left_right_label"),
    )

    # 4a) Owner-level aggregation
    owner_reach_df = (
        cross_feed_clusters.select("cluster_id", "feeds")
        .explode("feeds")
        .join(feed_lookup, left_on="feeds", right_on="feed", how="left")
        .group_by("cluster_id", "owner", "owner_agencies")
        .agg(
            total_reach=pl.col("reach").sum(),
            agencies=pl.col("feeds").count(),
        )
        .group_by("cluster_id")
        .agg(
            owner_reach=pl.struct(
                "owner",
                "total_reach",
                (pl.col("agencies") / pl.col("owner_agencies"))
                .round(2)
                .alias("agencies_perc"),
            )
        )
    )

    # 4b) Sum reach per (cluster_id, left_right_label)
    label_reach_long = (
        cross_feed_clusters.select("cluster_id", "feeds")
        .explode("feeds")
        .join(feed_lookup, left_on="feeds", right_on="feed", how="left")
        .group_by("cluster_id", "left_right_label")
        .agg(pl.col("reach").sum().alias("total_reach"))
        .with_columns(
            cluster_reach=pl.col("total_reach").sum().over("cluster_id"),
        )
        .with_columns(
            reach_perc=(pl.col("total_reach") / pl.col("cluster_reach")).round(2)
        )
        .sort("cluster_id")
    )

    # 4c) Reach per cluster
    total_reach = label_reach_long.group_by("cluster_id", "cluster_reach").agg()

    # 5) Pivot labels to columns: one column per left_right_label
    label_reach_wide = label_reach_long.pivot(
        on="left_right_label",  # becomes columns
        index="cluster_id",  # one row per cluster
        values="reach_perc",  # cell values
        aggregate_function="sum",  # safety if duplicates
    )

    # 6) Attach both owner_reach and left_right_reach back
    merged = (
        missing_feeds.join(owner_reach_df, on="cluster_id", how="left")
        .join(label_reach_wide, on="cluster_id", how="left")
        .join(total_reach, on="cluster_id", how="left")
    )

    # 7) Add Blind Spot columns
    merged = merged.with_columns(
        blindspot_left=pl.when(
            pl.sum_horizontal("left", "centre left") < 0.2,
            (pl.col("centre right") + pl.col("right")) > 0.8,
        )
        .then(1)
        .otherwise(0),
        blindspot_right=pl.when(
            pl.sum_horizontal("right", "centre right") < 0.2,
            pl.sum_horizontal("left", "centre left") > 0.3,
            pl.col("num_articles") > 5,
        )
        .then(1)
        .otherwise(0),
    ).sort("max_published_date", descending=True)
    
    # 8) Add single owner column
    merged = merged.with_columns(
        single_owner_high_reach=pl.when(
            (pl.col("owner_reach").list.len() == 1)
            & (pl.col("num_articles") > 7)
        )
        .then(1)
        .otherwise(0)
    )

    # Format date
    merged = merged.with_columns(
        max_published_date_fmt=pl.col("max_published_date").dt.to_string(
            "%d-%m-%Y %H:%M"
        )
    )

    return merged

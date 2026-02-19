"""News cluster enrichment and blind spot detection pipeline."""

from datetime import datetime

import dagster as dg
import polars as pl

CLUSTER_PUBLISH_DELAY_HOURS = 5
TIMELINE_CUTOFF_DAYS = 1
ARTICLES_ON_FRONTPAGE = 40
DATE_FORMAT = "%d-%m-%Y %H:%M"


def enrich_clusters(
    cross_feed_clusters: pl.DataFrame,
    unique_feeds: pl.DataFrame,
    news_agencies: pl.DataFrame,
) -> pl.DataFrame:
    """
    Enrich clusters with missing feeds, owner reach, and political label reach shares.

    Parameters
    ----------
    cross_feed_clusters : pl.DataFrame
        Raw cluster table with ``cluster_id`` and ``feeds`` list column.
    unique_feeds : pl.DataFrame
        All known feeds; must contain ``base_url``.
    news_agencies : pl.DataFrame
        Agency metadata with ``url``, ``owner``, ``reach``, ``owner_reach``,
        ``owner_agencies``, and ``left_right_label``.

    Returns
    -------
    pl.DataFrame
        Clusters with missing_feeds, owner_reach struct list, per-label reach
        share columns, and cluster_reach.
    """
    feed_lookup = news_agencies.select(
        pl.col("url").alias("feed"),
        "owner",
        "reach",
        "owner_reach",
        "owner_agencies",
        "left_right_label",
    )

    exploded = (
        cross_feed_clusters.select("cluster_id", "feeds")
        .explode("feeds")
        .join(feed_lookup, left_on="feeds", right_on="feed", how="left")
    )

    owner_reach = (
        exploded.group_by("cluster_id", "owner", "owner_agencies")
        .agg(total_reach=pl.col("reach").sum(), agencies=pl.col("feeds").count())
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

    label_reach_long = (
        exploded.group_by("cluster_id", "left_right_label")
        .agg(pl.col("reach").sum().alias("total_reach"))
        .with_columns(cluster_reach=pl.col("total_reach").sum().over("cluster_id"))
        .with_columns(
            reach_perc=(pl.col("total_reach") / pl.col("cluster_reach")).round(2)
        )
    )

    all_feeds = pl.lit(unique_feeds["base_url"].to_list())
    return (
        cross_feed_clusters.with_columns(
            missing_feeds=all_feeds.list.set_difference("feeds")
        )
        .join(owner_reach, on="cluster_id", how="left")
        .join(
            label_reach_long.pivot(
                on="left_right_label",
                index="cluster_id",
                values="reach_perc",
                aggregate_function="sum",
            ),
            on="cluster_id",
            how="left",
        )
        .join(
            label_reach_long.group_by("cluster_id", "cluster_reach").agg(),
            on="cluster_id",
            how="left",
        )
    )


def add_blindspot_flags(df: pl.DataFrame) -> pl.DataFrame:
    """
    Flag clusters as left or right blind spots based on political reach share.

    A left blind spot: right-leaning reach > 60%, left-leaning reach < 40%.
    A right blind spot: right-leaning reach < 20%, left-leaning reach > 30%.
    The centre < 0.7 guard excludes centrist-dominated clusters from both sides.

    Parameters
    ----------
    df : pl.DataFrame
        Must contain reach share columns: ``left``, ``centre left``, ``centre``,
        ``centre right``, ``right``.

    Returns
    -------
    pl.DataFrame
        Input extended with integer flag columns ``blindspot_left`` and ``blindspot_right``.
    """
    centre_guard = pl.col("centre") < 0.7
    return df.with_columns(
        blindspot_left=pl.when(
            (pl.sum_horizontal("centre right", "right") > 0.6)
            & (pl.sum_horizontal("left", "centre left") < 0.4)
            & centre_guard
        )
        .then(1)
        .otherwise(0),
        blindspot_right=pl.when(
            (pl.sum_horizontal("right", "centre right") < 0.2)
            & (pl.sum_horizontal("left", "centre left") > 0.3)
            & centre_guard
        )
        .then(1)
        .otherwise(0),
    )


def build_frontpage(
    blindspot_df: pl.DataFrame, n: int = ARTICLES_ON_FRONTPAGE
) -> pl.DataFrame:
    """
    Interleave the most recent left and right blind spot clusters for the front page.

    Parameters
    ----------
    blindspot_df : pl.DataFrame
        Output of :func:`add_blindspot_flags`.
    n : int
        Total clusters to surface, split evenly between sides.

    Returns
    -------
    pl.DataFrame
        Interleaved selection with ``max_published_date_fmt`` display column.
    """

    def top(flag: str) -> pl.DataFrame:
        return (
            blindspot_df.filter(pl.col(flag) == 1)
            .sort(["num_feeds"], descending=True)
            .head(n // 2)
            .sort("max_published_date", descending=True)
        )

    return (
        pl.concat(
            [
                top("blindspot_left")
                .with_row_index("idx")
                .with_columns(pl.lit(0).alias("src")),
                top("blindspot_right")
                .with_row_index("idx")
                .with_columns(pl.lit(1).alias("src")),
            ]
        )
        .sort(["idx", "src"])
        .drop(["idx", "src"])
        .with_columns(
            max_published_date_fmt=pl.col("max_published_date").dt.to_string(
                DATE_FORMAT
            )
        )
    )


@dg.asset(
    key_prefix="staging",
    ins={
        "cross_feed_clusters": dg.AssetIn(["staging", "cross_feed_clusters"]),
        "unique_feeds": dg.AssetIn(["staging", "unique_feeds"]),
        "news_agencies": dg.AssetIn(["raw", "news_agencies"]),
    },
)
def timeline(
    cross_feed_clusters: pl.DataFrame,
    unique_feeds: pl.DataFrame,
    news_agencies: pl.DataFrame,
) -> pl.DataFrame:
    """
    Run the full enrichment and front-page selection pipeline.

    Parameters
    ----------
    cross_feed_clusters : pl.DataFrame
        Raw cluster table.
    unique_feeds : pl.DataFrame
        All known feed base URLs.
    news_agencies : pl.DataFrame
        Master agency metadata table.
    now : datetime, optional
        Reference timestamp; defaults to ``datetime.now()``.

    Returns
    -------
    pl.DataFrame
        Front-page ready, interleaved blind spot clusters.
    """
    now = datetime.now()
    lower = now - pl.duration(days=TIMELINE_CUTOFF_DAYS)
    upper = now - pl.duration(hours=CLUSTER_PUBLISH_DELAY_HOURS)

    return build_frontpage(
        add_blindspot_flags(
            enrich_clusters(cross_feed_clusters, unique_feeds, news_agencies)
            .filter(pl.col("max_published_date").is_between(lower, upper))
            .sort("max_published_date", descending=True)
        )
    )

import dagster as dg
from polars import DataFrame


@dg.asset(
    key_prefix="staging",
    ins={
        "cross_feed_clusters": dg.AssetIn(["staging", "cross_feed_clusters"]),
        "unique_feeds": dg.AssetIn(["staging", "unique_feeds"]),
    }
)
def timeline(cross_feed_clusters:DataFrame, unique_feeds: DataFrame) -> DataFrame:
    import polars as pl
    
    feeds = pl.lit(unique_feeds.to_series().to_list())

    return cross_feed_clusters.with_columns(
        missing_feeds = feeds.list.set_difference("feeds")
    )
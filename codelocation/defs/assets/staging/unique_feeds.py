import dagster as dg
from polars import DataFrame


@dg.asset(
    key_prefix="staging",
    ins={
        "rss_feeds_historic": dg.AssetIn(["raw", "rss_feeds_historic"])   
    }
)
def unique_feeds(rss_feeds_historic:DataFrame) -> DataFrame:
    return rss_feeds_historic.group_by("base_url").agg()
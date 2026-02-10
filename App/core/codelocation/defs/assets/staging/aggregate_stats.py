import dagster as dg
import polars as pl


@dg.asset(
    key_prefix="staging", ins={"add_features": dg.AssetIn(["staging", "add_features"])}
)
def articles_per_publisher_day(add_features: pl.DataFrame):
    return (
        add_features.filter(pl.col("publish_date") > pl.date(2025, 12, 22))
        .group_by(
            "base_url", pl.col("publish_date").cast(pl.Date).alias("publish_date")
        )
        .agg(
            pl.len().alias("articles"),
            pl.col("char_count").mean().round(0).alias("avg_characters"),
        )
        .sort("publish_date")
    )

from dagster import asset
from polars import DataFrame

@asset(
    key_prefix="raw",
)
def news_agencies() -> DataFrame:
    from utils.utils import DATA_DIR
    import polars as pl
    
    left=-0.4
    centre_left=-0.2
    centre_right=0.2
    right=0.4
    
    df = pl.read_csv(DATA_DIR / "seeds/news_agencies.csv", try_parse_dates=True, separator=";")
    return df.with_columns(
        owner_reach = pl.col("reach").sum().over("owner"),
        owner_agencies = pl.col("url").count().over("owner"),
        left_right_label = (
            pl.when(pl.col("left_right") >= right)
            .then(pl.lit("right"))
            .when((pl.col("left_right") >= centre_right) & (pl.col("left_right") < right))
            .then(pl.lit("centre right"))
            .when((pl.col("left_right") < centre_right) & (pl.col("left_right") > centre_left))
            .then(pl.lit("centre"))
            .when((pl.col("left_right") <= centre_left) & (pl.col("left_right") > left))
            .then(pl.lit("centre left"))
            .when(pl.col("left_right") <= left)
            .then(pl.lit("left"))
            .otherwise(pl.lit("unmeasured"))
        ),
    )

@asset(
    key_prefix="raw",
)
def agency_owners() -> DataFrame:
    from utils.utils import DATA_DIR
    import polars as pl
    return pl.read_csv(DATA_DIR / "seeds/agency_owners.csv", try_parse_dates=True, separator=";")

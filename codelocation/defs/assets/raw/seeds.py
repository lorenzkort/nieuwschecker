from dagster import asset
from polars import DataFrame

@asset(
    key_prefix="raw",
)
def news_agencies() -> DataFrame:
    from utils.utils import DATA_DIR
    import polars as pl
    return pl.read_csv(DATA_DIR / "seeds/news_agencies.csv", try_parse_dates=True, separator=";")
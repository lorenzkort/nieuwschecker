from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

import ibis
import polars as pl
from dagster import (
    ConfigurableIOManager,
    InputContext,
    OutputContext,
)
from ibis import BaseBackend
from psycopg2 import sql

_engine_cache = None


@dataclass(frozen=True)
class PostgresConfig:
    """Configuration for connecting to a PostgreSQL database."""

    host: str
    port: int
    database: str
    user: str
    password: str


@contextmanager
def get_ibis_engine(config: PostgresConfig) -> Iterator[BaseBackend]:
    global _engine_cache
    if _engine_cache is None:
        _engine_cache = ibis.postgres.connect(
            host=config.host,
            port=config.port,
            database=config.database,
            user=config.user,
            password=config.password,
        )
    yield _engine_cache


class PostgresPolarsIOManager(ConfigurableIOManager):
    """This IOManager takes a Polars DataFrame and stores it in PostgreSQL."""

    host: str
    port: int
    user: str
    password: str
    database: str

    @property
    def _config(self):
        return self.model_dump()

    def _get_config(self) -> PostgresConfig:
        return PostgresConfig(**self._config)

    def handle_null_dtype(self, obj: pl.DataFrame) -> pl.DataFrame:
        """Cast NULL dtype to UTF-8"""
        return obj.with_columns([pl.col(col).cast(pl.Utf8) for col in obj.columns if obj[col].dtype == pl.Null])

    def handle_struct_dtype(self, obj: pl.DataFrame) -> pl.DataFrame:
        """Cast STRUCT dtype to JSON-encoded string, including structs within arrays"""
        columns_to_transform = []

        for col in obj.columns:
            dtype = obj[col].dtype

            # Direct struct column
            if dtype == pl.Struct:
                columns_to_transform.append(pl.col(col).struct.json_encode())
            # Array containing structs
            elif isinstance(dtype, pl.List):
                inner_dtype = dtype.inner
                if inner_dtype == pl.Struct:
                    columns_to_transform.append(pl.col(col).list.eval(pl.element().struct.json_encode()))

        if columns_to_transform:
            return obj.with_columns(columns_to_transform)
        return obj

    def handle_output(self, context: OutputContext, obj: pl.DataFrame):
        schema, table = self._get_schema_table(context.asset_key)

        if not isinstance(obj, pl.DataFrame):
            raise Exception(f"Outputs of type {type(obj)} not supported. Expected polars.DataFrame")

        row_count = obj.height
        context.log.info(f"Row count: {row_count}")

        # Casting all columns fully NULL to utf-8 data type
        df_null_patch = self.handle_null_dtype(obj)

        # JSON Decode all structs to json-strings
        df_struct_patch = self.handle_struct_dtype(df_null_patch)

        # use ibis to translate datatypes to Postgres
        ibis_table = ibis.memtable(df_struct_patch)
        context.log.debug(f"ibis schema: {ibis_table.schema()}")

        # Connect with ibis to postgres
        with get_ibis_engine(self._get_config()) as conn:
            # Create / Overwrite Postgres Table
            conn.create_table(table, database=schema, obj=ibis_table, overwrite=True)

    def load_input(self, context: InputContext) -> pl.DataFrame:
        schema, table = self._get_schema_table(context.asset_key)
        return self._load_input(table, schema, context)

    def _load_input(self, table: str, schema: str, context: InputContext) -> pl.DataFrame:
        # Connect with ibis
        with get_ibis_engine(self._get_config()) as conn:
            obj: pl.DataFrame = conn.table(table, database=schema).to_polars()

        # use ibis to translate datatypes to Postgres
        context.log.debug(f"polars schema: {obj.schema}")

        return obj

    def _get_schema_table(self, asset_key):
        # AssetKey.path gives you the full path as a list
        path_components = asset_key.path

        # Extract schema from first component
        schema = path_components[0]

        # Join remaining components for table name
        table_name = "_".join(path_components[1:])

        return schema, table_name

    def _get_select_statement(
        self,
        table: str,
        schema: str,
        columns: Optional[Sequence[str]],
    ):
        if columns:
            col_sql = sql.SQL(", ").join(sql.Identifier(col) for col in columns)
        else:
            col_sql = sql.SQL("*")

        return sql.SQL("SELECT {fields} FROM {schema}.{table}").format(
            fields=col_sql,
            schema=sql.Identifier(schema),
            table=sql.Identifier(table),
        )
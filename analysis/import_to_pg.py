
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
import json
from datetime import datetime

def quick_ingest_and_query(
    parquet_path: str = "/Users/lorenzkort/Documents/LocalCode/news-data/data/staging/timeline.parquet",
    connection_string: str = "postgresql://postgres:postgres@localhost:5420/app",
    table_name: str = "timeline"
) -> None:
    """
    Ingest parquet file and run basic queries.

    Parameters
    ----------
    parquet_path : str
        Path to parquet file
    connection_string : str
        PostgreSQL connection string
    table_name : str
        Name for the table
    """
    engine = create_engine(connection_string, isolation_level="AUTOCOMMIT")
    
    with engine.connect() as conn:
        result = conn.execute(
            text("SELECT 1 FROM pg_database WHERE datname = 'app'")
        )
        if not result.fetchone():
            conn.execute(text('CREATE DATABASE "app"'))
    
    engine = create_engine(connection_string)
    
    df = pd.read_parquet(parquet_path)
    
    def serialize_value(x):
        if hasattr(x, 'tolist'):
            return json.dumps(x.tolist(), default=str)
        return x
    
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(serialize_value)
    
    df.to_sql(table_name, engine, if_exists="replace", index=False)
    
    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}")).scalar()
        print(f"Row count: {count:,}")
        
        sample = pd.read_sql(f"SELECT * FROM {table_name} LIMIT 10", conn)
        print(f"\nSample data:\n{sample}")

if __name__ == "__main__":
    quick_ingest_and_query(
        parquet_path = "/Users/lorenzkort/Documents/LocalCode/news-data/data/staging/sentiments_over_time.parquet",
        connection_string = "postgresql://postgres:postgres@localhost:5420/app",
        table_name = "sentiments_over_time"
    )
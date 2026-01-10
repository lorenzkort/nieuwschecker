import dagster as dg
from polars import DataFrame
logging = dg.get_dagster_logger()

@dg.asset(
    key_prefix="mart",
    ins={
        "sentiments": dg.AssetIn(["staging", "sentiments"])   
    }
)
def create_network_entity_graphv2(sentiments: DataFrame) -> None:
    import polars as pl
    import json
    from utils.utils import DATA_DIR
    
    df = sentiments.filter(pl.col("mention_count").is_not_null())
    
    # Prepare data for visualization
    # Convert to a format suitable for network analysis and visualization
    entities_data = df.select([
        "entity",
        "variable",
        "mention_count",
        "positive_mentions",
        "negative_mentions",
        "avg_sentiment_score",
        "link"
    ]).to_dicts()
    
    # Create entity type summary for quick filtering
    entity_types = df.group_by("variable").agg([
        pl.count().alias("count"),
        pl.col("avg_sentiment_score").mean().alias("avg_sentiment"),
        pl.col("mention_count").sum().alias("total_mentions")
    ]).to_dicts()
    
    # Calculate co-occurrence matrix (entities appearing in same links)
    # Group by link to find entities that appear together
    link_entities = df.group_by("link").agg([
        pl.col("entity").alias("entities"),
        pl.col("variable").alias("types"),
        pl.col("avg_sentiment_score").alias("sentiments")
    ])
    
    # Create connections between entities that appear in the same articles
    connections = []
    for row in link_entities.iter_rows(named=True):
        entities = row["entities"]
        if entities and len(entities) > 1:
            # Create edges between all pairs in the same article
            for i in range(len(entities)):
                for j in range(i + 1, len(entities)):
                    connections.append({
                        "source": entities[i],
                        "target": entities[j],
                        "link": row.get("link", "")
                    })
    
    # Export data as JSON for the web interface
    export_data = {
        "entities": entities_data,
        "entity_types": entity_types,
        "connections": connections,
        "metadata": {
            "total_entities": len(df),
            "unique_entities": df["entity"].n_unique(),
            "total_links": df["link"].n_unique() if "link" in df.columns else 0
        }
    }
    
    # Save to JSON file
    output_path = DATA_DIR / "visuals" / "network_data.json"
    with open(output_path, "w") as f:
        json.dump(export_data, f)
    
    logging.info(f"Exported {len(entities_data)} entities to {output_path}")
    
    return None
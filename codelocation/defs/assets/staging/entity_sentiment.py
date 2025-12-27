"""
Entity-Specific Sentiment Analysis using RobBERT-v2

This script analyses sentiment towards specific entities mentioned in text.
It uses the RobBERT-v2 model (Dutch BERT) to determine whether an entity
is portrayed positively or negatively.
"""

import dagster as dg
from polars import DataFrame

logging = dg.get_dagster_logger()

# Global variable to store the pipeline so it's only loaded once
_sentiment_pipeline = None

def get_sentiment_pipeline(model_name: str = "DTAI-KULeuven/robbert-v2-dutch-sentiment"):
    """
    Get or create the sentiment pipeline (singleton pattern).
    This ensures the model is only loaded once.
    """
    global _sentiment_pipeline
    if _sentiment_pipeline is None:
        from transformers import pipeline
        _sentiment_pipeline = pipeline(
            "sentiment-analysis",
            model=model_name,
            tokenizer=model_name
        )
    return _sentiment_pipeline

def extract_entity_context(text: str, entity: str) -> list:
    """
    Extract full sentences containing entity mentions.
    
    Args:
        text: The full text to analyse
        entity: The entity to find
        context_window: Not used (kept for backwards compatibility)
    
    Returns:
        List of complete sentences containing the entity
    """
    import re
    
    contexts = []
    # Convert both to lowercase for case-insensitive matching
    text_lower = text.lower()
    entity_lower = entity.lower()
    
    # Split text into sentences (handles ., !, ?)
    # This pattern keeps the sentence delimiter with the sentence
    sentence_pattern = r'[^.!?]+[.!?]+'
    sentences = re.findall(sentence_pattern, text_lower)
    
    # Handle case where text doesn't end with punctuation
    if sentences:
        last_match_end = sum(len(s) for s in sentences)
        if last_match_end < len(text):
            sentences.append(text[last_match_end:])
    else:
        sentences = [text]
    
    # Find sentences containing the entity (case-insensitive)
    for sentence in sentences:
        if entity_lower in sentence.lower():
            contexts.append(sentence.strip())
    
    return contexts


def analyse_entity_sentiment(text: str, entity: str, model_name: str = "DTAI-KULeuven/robbert-v2-dutch-sentiment"):
    """
    Analyse sentiment towards a specific entity in text.
    
    Args:
        text: The text containing the entity
        entity: The entity to analyse sentiment for
        model_name: HuggingFace model identifier for RobBERT-v2 sentiment
    
    Returns:
        Dictionary with sentiment scores and analysis
    """
    import json
    
    # Get the sentiment pipeline (will reuse existing one)
    sentiment_pipeline = get_sentiment_pipeline(model_name)
    
    # Extract contexts where entity is mentioned
    contexts = extract_entity_context(text, entity)
    
    if not contexts:
        return json.dumps({
            "entity": entity,
            "found": False,
        })
    
    
    # Analyse sentiment for each context
    results = []
    for i, context in enumerate(contexts):
        sentiment = sentiment_pipeline(context)[0]
        results.append({
            "context": context,
            "label": sentiment["label"],
            "score": round(sentiment["score"], 3)
        })
    
    # Calculate aggregate scores
    positive_count = sum(1 for r in results if r["label"].lower() in ["positive", "pos"])
    negative_count = sum(1 for r in results if r["label"].lower() in ["negative", "neg"])
    
    # Calculate weighted average sentiment
    # Assuming positive = +1, negative = -1
    total_sentiment = 0
    for r in results:
        if r["label"].lower() in ["positive", "pos"]:
            total_sentiment += r["score"]
        elif r["label"].lower() in ["negative", "neg"]:
            total_sentiment -= r["score"]
    
    avg_sentiment = round(total_sentiment / len(results), 3) if results else 0
    
    return json.dumps({
        "entity": entity,
        "results": results,
        "mention_count": len(contexts),
        "positive_mentions": positive_count,
        "negative_mentions": negative_count,
        "average_sentiment_score": avg_sentiment,
    })

def process_entity_sentiments(title: str, summary: str, keywords: list) -> list[str]:
    """
    Convert a list of entities into a dictionary with sentiment scores.
    Uses title + summary as the text input for sentiment analysis.
    """

    if keywords is None or len(keywords) == 0:
        return []
    logging.info(f"processing {keywords}")
    
    text = f"{title}. {summary}"
    result = [analyse_entity_sentiment(text, keyword) for keyword in keywords]
    return result


@dg.asset(
    key_prefix="staging",
    ins={
        "add_features": dg.AssetIn(["staging", "add_features"])
    }
)
def entity_sentiments(add_features: DataFrame) -> DataFrame:
    import polars as pl
    
    add_features = add_features
    # Load the model once before processing
    logging.info("Loading sentiment model...")
    get_sentiment_pipeline()
    logging.info("Model loaded successfully!")
    
    # These are columns which have lists of keywords
    sentiment_cols = ["PERSON", "ORG", "GPE", "LOC", "EVENT"]    
    for col in sentiment_cols:
        logging.info(f"\nProcessing column: {col}")
        add_features = add_features.with_columns(
            pl.struct(["title", "summary", col])
            .map_elements(
                lambda row: process_entity_sentiments(row["title"], row["summary"], row[col]),
                return_dtype=pl.List(pl.List(pl.String)),
            ).alias(f"{col}_sentiment")
        )
    
    return add_features

@dg.asset(
    key_prefix="staging",
    ins={
        "entity_sentiments": dg.AssetIn(["staging", "entity_sentiments"])
    }
)
def sentiments(entity_sentiments: DataFrame) -> DataFrame:
    import polars as pl
    
    results_field_type = pl.List(
        pl.Struct([
            pl.Field("context", pl.String),
            pl.Field("label", pl.String),
            pl.Field("score", pl.Float64),
        ])
    )

    schema = pl.Struct([
        pl.Field("entity", pl.String),
        pl.Field("results", results_field_type),
        pl.Field("mention_count", pl.Int64),
        pl.Field("positive_mentions", pl.Int64),
        pl.Field("negative_mentions", pl.Int64),
        pl.Field("average_sentiment_score", pl.Float64),
    ])

    exploded = (
        entity_sentiments
        .unpivot(
            on=[
                "PERSON_sentiment", "ORG_sentiment", "GPE_sentiment",
                "LOC_sentiment", "EVENT_sentiment",
            ],
            index="link",
        )
        # 1. explode outer list -> list[str]
        .explode("value", keep_nulls=False, empty_as_null=False)  
        # 2. explode inner list -> str
        .with_columns(pl.col("value").list.explode().alias("value"))
        # 3. decode JSON strings into a struct
        .with_columns(
            decoded=pl.col("value").str.json_decode(schema)
        )
        # 4. (optional) put struct fields into separate columns
        .unnest("decoded")
    ).select(pl.exclude("value"))
    return exploded

@dg.asset(
    key_prefix="staging",
    ins={
        "sentiments": dg.AssetIn(["staging", "sentiments"])
    }
)
def sentiments_per_entity(sentiments: DataFrame) -> DataFrame:
    import polars as pl
    return sentiments.group_by(["entity"]).agg(
            pl.col("average_sentiment_score").mean().round(3),
            pl.col("mention_count").sum().alias("mentions"),
            pl.col("mention_count").std().round(3).alias("std"),
            pl.col("link").count().alias("articles")
        ).sort("entity")
    
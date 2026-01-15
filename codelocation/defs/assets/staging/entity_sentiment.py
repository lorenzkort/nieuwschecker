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

def extract_entity_context(text: str, entity: str, min_context_chars: int = 100) -> list:
    """
    Extract sentences containing entity mentions, with minimum context length.
    
    If a sentence is shorter than min_context_chars, expands to include
    min_context_chars before and after the entity within the full text.
    
    Args:
        text: The full text to analyse
        entity: The entity to find
        min_context_chars: Minimum characters to include before and after entity (default: 100)
    
    Returns:
        List of context strings containing the entity
    """
    import re
    
    contexts = []
    # Convert both to lowercase for case-insensitive matching
    text_lower = text.lower()
    entity_lower = entity.lower()
    
    # Split text into sentences (handles ., !, ?)
    sentence_pattern = r'[^.!?]+[.!?]+'
    sentences = re.findall(sentence_pattern, text)
    
    # Handle case where text doesn't end with punctuation
    if sentences:
        last_match_end = sum(len(s) for s in sentences)
        if last_match_end < len(text):
            sentences.append(text[last_match_end:])
    else:
        sentences = [text]
    
    # Track position in original text
    current_pos = 0
    
    for sentence in sentences:
        sentence_lower = sentence.lower()
        if entity_lower in sentence_lower:
            # If sentence is long enough, use it as-is
            if len(sentence) >= min_context_chars * 2:
                contexts.append(sentence.strip())
            else:
                # Find entity position in original text
                entity_pos_in_text = text_lower.find(entity_lower, current_pos)
                
                if entity_pos_in_text != -1:
                    # Calculate context boundaries
                    start = max(0, entity_pos_in_text - min_context_chars)
                    end = min(len(text), entity_pos_in_text + len(entity) + min_context_chars)
                    
                    context = text[start:end].strip()
                    contexts.append(context)
        
        current_pos += len(sentence)
    
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
        "avg_sentiment_score": avg_sentiment,
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
    },
    metadata={
        "mode": "overwrite",
        "delta_write_options": {
            "schema_mode": "overwrite"
        }
    }
)
def entity_sentiments(context: dg.AssetExecutionContext, add_features: DataFrame) -> DataFrame:
    import polars as pl
    
    historic_entity_sentiments = context.load_asset_value(
            asset_key=dg.AssetKey(["staging", "entity_sentiments"])
        )
    
    # Only process new rows who have not been processed before
    processed_entity_sentiments = DataFrame()
    sentiment_cols = ["PERSON", "ORG", "GPE", "LOC", "EVENT"]    
    articles_to_process = add_features.filter(
        ~pl.col("link").is_in(historic_entity_sentiments["link"]) # gets all new articles
    )
    logging.info(f'Articles to process: {len(articles_to_process)}')
    get_sentiment_pipeline() # Loading LLM model
    exprs = [
        pl.struct(["title", "summary", col])
        .map_elements(
            lambda row, c=col: process_entity_sentiments(
                row["title"], row["summary"], row[c]
            ),
            return_dtype=pl.List(pl.List(pl.String)),
        )
        .alias(f"{col}_sentiment")
        for col in sentiment_cols
    ]
    processed_entity_sentiments = articles_to_process.with_columns(*exprs)
    
    # Combine historic and processed
    combined_entity_sentiments = pl.concat([historic_entity_sentiments, processed_entity_sentiments])
    return combined_entity_sentiments

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
        pl.Field("avg_sentiment_score", pl.Float64),
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
            pl.col("avg_sentiment_score").mean().round(3),
            pl.col("mention_count").sum().alias("mentions"),
            pl.col("link").count().alias("articles")
        ).sort("entity")
    
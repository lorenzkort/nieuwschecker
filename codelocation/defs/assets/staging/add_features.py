import spacy
from typing import List, Dict, Any
from sentence_transformers import SentenceTransformer
from collections import Counter
import dagster as dg
import polars as pl

from utils.utils import DATA_DIR

logging = dg.get_dagster_logger()

# Load once at module level
nlp = spacy.load("nl_core_news_lg")
embedder = SentenceTransformer(
    "paraphrase-multilingual-MiniLM-L12-v2",
    cache_folder=str(DATA_DIR / "cache")
)

def gen_features(text: str) -> pl.Expr:
    """
    Generate matching features for a news article.
    These features are designed for cross-outlet deduplication.
    """

    text = text.strip()

    # ---------- NLP ----------
    doc = nlp(text)

    # ---------- Named entities ----------
    entities = {
        "PERSON": set(),
        "ORG": set(),
        "GPE": set(),
        "LOC": set(),
        "EVENT": set(),
    }

    for ent in doc.ents:
        if ent.label_ in entities:
            entities[ent.label_].add(ent.text.lower())

    # ---------- Lexical keywords (cheap blocking) ----------
    tokens = [
        t.lemma_.lower()
        for t in doc
        if t.is_alpha and not t.is_stop and len(t) > 2
    ]

    keyword_counts = Counter(tokens)
    top_keywords = [w for w, _ in keyword_counts.most_common(15)]

    # ---------- Semantic embedding ----------
    embedding = embedder.encode(text, normalize_embeddings=True)

    # ---------- Structural features ----------
    features = pl.struct({
        "embedding": embedding,                  # primary similarity
        "entities": {k: sorted(v) for k, v in entities.items()},
        "keywords": top_keywords,                 # blocking / prefilter
        "token_count": len(tokens),
        "char_count": len(text),
    })

    return features

def gen_features_batch(texts: List[str]) -> List[Dict[str, Any]]:
    texts = [t.strip() for t in texts]

    # ---------- spaCy in batch ----------
    docs = list(nlp.pipe(texts, batch_size=32))

    # ---------- embeddings in batch ----------
    embeddings = embedder.encode(
        texts,
        normalize_embeddings=True,
        batch_size=32,
        show_progress_bar=False,
    )

    results = []

    for doc, embedding, text in zip(docs, embeddings, texts):
        entities = {
            "PERSON": set(),
            "ORG": set(),
            "GPE": set(),
            "LOC": set(),
            "EVENT": set(),
        }

        for ent in doc.ents:
            if ent.label_ in entities:
                entities[ent.label_].add(ent.text.lower())

        tokens = [
            t.lemma_.lower()
            for t in doc
            if t.is_alpha and not t.is_stop and len(t) > 2
        ]

        top_keywords = [
            w for w, _ in Counter(tokens).most_common(15)
        ]

        results.append({
            "embedding": embedding.tolist(),  # important
            "entities": {k: sorted(v) for k, v in entities.items()},
            "keywords": top_keywords,
            "token_count": len(tokens),
            "char_count": len(text),
        })

    return results

@dg.asset(
    key_prefix="staging",
    ins={
        "rss_feeds_historic": dg.AssetIn(["raw", "rss_feeds_historic"])
    }
)
def add_features(rss_feeds_historic: pl.DataFrame) -> pl.DataFrame:
    full_text = rss_feeds_historic.select(["title", "summary"]).with_columns(
        (pl.col("title") + ". " + pl.col("summary")).alias("full_text")
    ).get_column("full_text").to_list()
    
    features = gen_features_batch(full_text)

    added_features = rss_feeds_historic.with_columns(
        pl.Series("features", features)
    ).unnest("features").unnest("entities")
    
    # Get publish_date from ingestion_timestamp
    added_missing_publish_date = added_features.with_columns(
        publish_date=(
            pl.when(pl.col("publish_date").is_null())
            .then(pl.col("ingestion_timestamp"))
            .otherwise(pl.col("publish_date"))
        )
    )

    return added_missing_publish_date
import logging
print("importing spacy")
import spacy
print("importing sentence_transformers")
from sentence_transformers import SentenceTransformer
print("importing collections")
from collections import Counter

# Load once at module level
print("loading spacy model")
nlp = spacy.load("nl_core_news_sm")
print("loading embedder")
embedder = SentenceTransformer("paraphrase-multilingual-MiniLM-L12-v2")
print("done")

def gen_features(title: str, summary: str) -> dict:
    """
    Generate matching features for a news article.
    These features are designed for cross-outlet deduplication.
    """

    text = f"{title}. {summary}".strip()

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
    features = {
        "embedding": embedding,                  # primary similarity
        "entities": {k: sorted(v) for k, v in entities.items()},
        "keywords": top_keywords,                 # blocking / prefilter
        "token_count": len(tokens),
        "char_count": len(text),
    }

    return features

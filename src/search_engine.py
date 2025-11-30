import csv
import json
import math
import re
import time
from collections import defaultdict

LEXICON_PATH = "data/index/lexicon_complete.json"
FORWARD_INDEX_PATH = "data/index/forward_index_termid.json"
INVERTED_INDEX_PATH = "data/index/inverted_index_termid.json"
MARKET_VALUE_PATH = "data/raw/player_latest_market_value/player_latest_market_value.csv"
PROFILE_DATA_PATH = "data/processed/complete_player_profiles.json"

# BM25 parameters
K1 = 1.2
B = 0.75

#scoring boost for better searching
NAME_TOKEN_WEIGHT = 0.75
NAME_PREFIX_BONUS = 1.25
EXACT_NAME_BONUS = 3.0
RAW_SUBSTRING_BONUS = 0.25
MARKET_VALUE_WEIGHT = 12.0
PROFILE_LENGTH_WEIGHT = 4.0
NON_NAME_MATCH_PENALTY = 1.5

#text normalization
COMPREHENSIVE_STOP_WORDS = {
    "the", "and", "in", "for", "with", "on", "at", "from", "by", "as", "is", "was",
    "are", "were", "be", "been", "have", "has", "had", "to", "of", "a", "an", "that",
    "this", "these", "those", "it", "its", "or", "but", "not", "what", "which", "who",
    "when", "where", "why", "how", "all", "any", "both", "each", "few", "more", "most",
    "other", "some", "such", "no", "nor", "only", "own", "same", "so", "than", "too",
    "very", "can", "will", "just", "should", "now", "player", "club", "team", "football",
    "soccer", "match", "game", "season", "league", "cup", "champions", "premier", "la",
    "bundesliga", "serie", "current", "main", "position", "nationality", "birth", "place"
}

def simple_stemmer(word: str) -> str:
    if word.endswith("ing") and len(word) > 5:
        return word[:-3]
    elif word.endswith("ed") and len(word) > 4:
        return word[:-2]
    elif word.endswith("es") and len(word) > 4:
        return word[:-2]
    elif word.endswith("s") and len(word) > 3:
        return word[:-1]
    return word

def normalize_and_tokenize(text: str):
    text = text.lower()
    tokens = re.findall(r"\b[a-z]+\b", text)
    result = []
    for w in tokens:
        if w in COMPREHENSIVE_STOP_WORDS or len(w) <= 2:
            continue
        result.append(simple_stemmer(w))
    return result


def normalize_name_tokens(value: str):
    """Normalize player names so we can compare against query tokens."""
    if not isinstance(value, str):
        return []
    tokens = re.findall(r"[a-z]+", value.lower())
    return [simple_stemmer(tok) for tok in tokens if tok]


def build_name_metadata(name: str):
    tokens = normalize_name_tokens(name)
    token_set = set(tokens)
    normalized = " ".join(tokens)
    return {
        "tokens": tokens,
        "token_set": token_set,
        "normalized": normalized,
        "raw_lower": name.lower() if isinstance(name, str) else "",
    }


def load_market_values(path: str):
    values = {}
    try:
        with open(path, "r", encoding="utf-8") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                try:
                    player_id = int(row.get("player_id", ""))
                except (TypeError, ValueError):
                    continue
                raw_value = row.get("value")
                try:
                    value = float(raw_value)
                except (TypeError, ValueError):
                    continue
                date_key = row.get("date_unix", "") or ""
                current = values.get(player_id)
                if current is None or date_key > current[0]:
                    values[player_id] = (date_key, value)
    except FileNotFoundError:
        print(f"[warn] Market value file not found at {path}")
        return {}
    return {pid: info[1] for pid, info in values.items()}


def load_profile_lengths(path: str):
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError:
        print(f"[warn] Profile data file not found at {path}")
        return {}

    lengths = {}
    for entry in data:
        player_id = entry.get("player_id")
        if not isinstance(player_id, int):
            continue
        detailed = entry.get("detailed_content")
        if isinstance(detailed, str) and detailed:
            lengths[player_id] = len(detailed)
    return lengths

#loading indexes
print("[init] Loading lexicon...")
with open(LEXICON_PATH, "r", encoding="utf-8") as f:
    lexicon_entries = json.load(f)
token_to_id = {entry["token"]: entry["term_id"] for entry in lexicon_entries}
termid_to_token = {entry["term_id"]: entry["token"] for entry in lexicon_entries}
print(f"[done] Lexicon loaded: {len(token_to_id):,} tokens")

print("[init] Loading forward index...")
with open(FORWARD_INDEX_PATH, "r", encoding="utf-8") as f:
    forward_index = json.load(f)
doc_by_id = {doc["player_id"]: doc for doc in forward_index}
N = len(doc_by_id)
avg_doc_len = sum(d["total_terms"] for d in forward_index) / N if N > 0 else 0.0
name_metadata = {doc_id: build_name_metadata(doc.get("player_name"))
                 for doc_id, doc in doc_by_id.items()}
print(f"[done] Forward index: {N:,} documents (avg_len={avg_doc_len:.2f})")

print("[init] Loading inverted index...")
with open(INVERTED_INDEX_PATH, "r", encoding="utf-8") as f:
    inv_data = json.load(f)
# term_id keys are strings in JSON; convert to int for convenience
term_document_frequency = {int(tid): df for tid, df in inv_data["term_document_frequency"].items()}
inverted_index = {int(tid): {int(doc_id): info for doc_id, info in docs.items()}
                  for tid, docs in inv_data["inverted_index"].items()}
print(f"[done] Inverted index: {len(inverted_index):,} term_ids")

print("[init] Loading market values...")
player_market_value = load_market_values(MARKET_VALUE_PATH)
max_market_value = max(player_market_value.values(), default=0.0)
market_value_log_max = math.log1p(max_market_value) if max_market_value > 0 else 1.0
print(f"[done] Market values loaded for {len(player_market_value):,} players")

print("[init] Loading profile metadata...")
profile_length_by_id = load_profile_lengths(PROFILE_DATA_PATH)
max_profile_length = max(profile_length_by_id.values(), default=0)
profile_length_log_max = math.log1p(max_profile_length) if max_profile_length > 0 else 1.0
print(f"[done] Profile metadata loaded for {len(profile_length_by_id):,} players")

#query to termid conversion
def tokens_to_term_ids(tokens):
    seen = set()
    unique_term_ids = []
    for tok in tokens:
        tid = token_to_id.get(tok)
        if tid is None or tid in seen:
            continue
        seen.add(tid)
        unique_term_ids.append(tid)
    return unique_term_ids

#scoring algorithm
def bm25_score(tf, df, doc_len, N, avg_doc_len, k1=K1, b=B):
    # idf with small smoothing to avoid zero / negatives
    idf = math.log((N - df + 0.5) / (df + 0.5) + 1.0)
    denom = tf + k1 * (1 - b + b * (doc_len / avg_doc_len))
    return idf * (tf * (k1 + 1) / denom)

#searching function

def search(query: str, top_k: int = 10, verbose: bool = True):
    start_time = time.perf_counter()

    log = print if verbose else (lambda *args, **kwargs: None)

    log(f"\n[query] {query}")
    query_tokens = normalize_and_tokenize(query)
    term_ids = tokens_to_term_ids(query_tokens)
    if not term_ids:
        elapsed = (time.perf_counter() - start_time) * 1000
        log(f"No query terms found in lexicon. (took {elapsed:.2f} ms)")
        return []

    log("Query tokens -> term_ids:",
        [(termid_to_token[tid], tid) for tid in term_ids if tid in termid_to_token])

    scores = defaultdict(float)

    for tid in term_ids:
        df = term_document_frequency.get(tid, 0)
        if df == 0:
            continue
        postings = inverted_index.get(tid)
        if not postings:
            continue

        for doc_id, info in postings.items():
            tf = info["tf"]
            doc_len = doc_by_id[doc_id]["total_terms"]
            scores[doc_id] += bm25_score(tf, df, doc_len, N, avg_doc_len)

    if scores:
        query_name_tokens = normalize_name_tokens(query)
        query_name = " ".join(query_name_tokens)
        raw_query_lower = query.lower().strip()

        for doc_id in scores:
            boost = 0.0
            meta = name_metadata.get(doc_id)
            has_name_match = False
            match_count = 0
            if meta:
                if query_tokens:
                    match_count = sum(1 for tok in query_tokens if tok in meta["token_set"])
                    if match_count:
                        boost += NAME_TOKEN_WEIGHT * match_count
                        has_name_match = True
                if query_name:
                    if meta["normalized"] == query_name:
                        boost += EXACT_NAME_BONUS
                        has_name_match = True
                    elif meta["normalized"].startswith(query_name):
                        boost += NAME_PREFIX_BONUS
                        has_name_match = True
                if raw_query_lower and raw_query_lower in meta["raw_lower"]:
                    boost += RAW_SUBSTRING_BONUS
                    has_name_match = True

            if not has_name_match and query_tokens:
                boost -= NON_NAME_MATCH_PENALTY

            if has_name_match:
                value = player_market_value.get(doc_id)
                if value and market_value_log_max > 0.0:
                    boost += MARKET_VALUE_WEIGHT * (math.log1p(value) / market_value_log_max)

                length = profile_length_by_id.get(doc_id)
                if length and profile_length_log_max > 0.0:
                    boost += PROFILE_LENGTH_WEIGHT * (math.log1p(length) / profile_length_log_max)

            scores[doc_id] += boost

    if not scores:
        elapsed = (time.perf_counter() - start_time) * 1000
        log(f"No documents matched these terms. (took {elapsed:.2f} ms)")
        return []

    ranked = sorted(scores.items(), key=lambda x: x[1], reverse=True)[:top_k]

    results = []
    for rank, (doc_id, score) in enumerate(ranked, start=1):
        doc = doc_by_id[doc_id]
        results.append({
            "rank": rank,
            "doc_id": doc_id,
            "player_id": doc["player_id"],
            "player_name": doc["player_name"],
            "score": score,
            "market_value": player_market_value.get(doc_id),
        })

    elapsed = (time.perf_counter() - start_time) * 1000

    log("\n[top] Results:")
    for r in results:
        value = r["market_value"]
        length = profile_length_by_id.get(r["doc_id"])
        extras = []
        if value:
            extras.append(f"market_value~{value:,.0f} EUR")
        if length:
            extras.append(f"profile_chars={length}")
        extra_text = f" [{', '.join(extras)}]" if extras else ""
        log(f"{r['rank']:2d}. [{r['score']:.3f}] {r['player_name']} (player_id={r['player_id']}){extra_text}")
    log(f"\n[time] {elapsed:.2f} ms")
    if elapsed < 500:
        log("[perf] Under 500 ms goal")
    else:
        log("[perf] Above 500 ms goal")

    return results

#Simple cli for searching

if __name__ == "__main__":
    print("\n[ready] Search engine ready. Type a query (1-5+ words) or press Enter to exit.")
    while True:
        q = input("\nQuery> ").strip()
        if not q:
            break
        search(q, top_k=10)
    print("\n[exit] Exiting search engine.")

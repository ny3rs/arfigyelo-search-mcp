"""Product search utilities for the Árfigyelő export."""

from __future__ import annotations

import math
import unicodedata
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional

import pandas as pd
from rapidfuzz import fuzz, process


@dataclass
class ColumnSchema:
    name_columns: List[str]
    brand_columns: List[str]
    store_columns: List[str]
    price_columns: List[str]
    id_columns: List[str]


@dataclass
class MatchResult:
    label: str
    score: float
    store: Optional[str]
    prices: Dict[str, float]
    brand: Optional[str]
    product_id: Optional[str]
    source_row: Dict[str, object]


SPECIAL_KEYS = {
    "name": ["termek", "termék", "megnevez", "product", "item", "cikk"],
    "brand": ["marka", "márka", "brand"],
    "store": ["aruhaz", "áruház", "bolt", "lanc", "lánc", "store"],
    "price": ["ar", "ár", "brutto", "bruttó", "price"],
    "id": ["id", "azonosito", "azonosító", "ean", "gtin"],
}


def strip_accents(value: str) -> str:
    normalized = unicodedata.normalize("NFD", value)
    return "".join(char for char in normalized if unicodedata.category(char) != "Mn")


def normalize_text(value: object) -> str:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return ""
    return strip_accents(str(value)).lower().strip()


def detect_columns(df: pd.DataFrame) -> ColumnSchema:
    lower = {col: normalize_text(col) for col in df.columns}

    def pick(keys: Iterable[str]) -> List[str]:
        return [col for col, lowered in lower.items() if any(key in lowered for key in keys)]

    name_columns = pick(SPECIAL_KEYS["name"]) or [df.columns[0]]
    brand_columns = pick(SPECIAL_KEYS["brand"])
    store_columns = pick(SPECIAL_KEYS["store"])
    price_columns = [
        col
        for col, lowered in lower.items()
        if any(key in lowered for key in SPECIAL_KEYS["price"]) and pd.api.types.is_numeric_dtype(df[col])
    ]
    if not price_columns:
        price_columns = [col for col in df.columns if pd.api.types.is_numeric_dtype(df[col])]

    id_columns = pick(SPECIAL_KEYS["id"])

    return ColumnSchema(
        name_columns=name_columns,
        brand_columns=brand_columns,
        store_columns=store_columns,
        price_columns=price_columns,
        id_columns=id_columns,
    )


def build_search_text(row: pd.Series, schema: ColumnSchema) -> str:
    parts: List[str] = []
    for col in schema.name_columns + schema.brand_columns:
        value = normalize_text(row.get(col))
        if value:
            parts.append(value)
    return " ".join(parts)


def prepare_index(df: pd.DataFrame, schema: Optional[ColumnSchema] = None) -> pd.DataFrame:
    schema = schema or detect_columns(df)
    df = df.copy()
    df["__search_text"] = df.apply(lambda row: build_search_text(row, schema), axis=1)
    df["__label"] = df[schema.name_columns].astype(str).agg(" | ".join, axis=1)
    if schema.brand_columns:
        df["__label"] = df["__label"] + " (" + df[schema.brand_columns[0]].astype(str) + ")"
    return df


def _extract_prices(row: pd.Series, schema: ColumnSchema) -> Dict[str, float]:
    prices: Dict[str, float] = {}
    for col in schema.price_columns:
        value = row.get(col)
        if isinstance(value, (int, float)) and not math.isnan(value):
            prices[col] = float(value)
    return prices


def _first_non_empty(row: pd.Series, columns: List[str]) -> Optional[str]:
    for col in columns:
        value = row.get(col)
        if pd.notna(value):
            text = str(value).strip()
            if text:
                return text
    return None


def search_products(
    df: pd.DataFrame,
    query: str,
    limit: int = 5,
    min_score: float = 45.0,
    schema: Optional[ColumnSchema] = None,
) -> List[MatchResult]:
    """Run a fuzzy search over the loaded DataFrame.

    Parameters
    ----------
    df:
        DataFrame produced by :func:`prepare_index`.
    query:
        Free-text product description.
    limit:
        Maximum number of matches to return.
    min_score:
        Minimum RapidFuzz similarity score (0-100) to include.
    schema:
        Optional :class:`ColumnSchema` override.
    """

    if "__search_text" not in df.columns:
        schema = schema or detect_columns(df)
        df = prepare_index(df, schema)
    else:
        schema = schema or detect_columns(df)

    normalized_query = normalize_text(query)
    candidates = df["__search_text"].tolist()
    scored = process.extract(normalized_query, candidates, scorer=fuzz.token_set_ratio, limit=limit)

    results: List[MatchResult] = []
    for match_text, score, idx in scored:
        if score < min_score:
            continue
        row = df.iloc[int(idx)]
        label = str(row.get("__label", match_text))
        store = _first_non_empty(row, schema.store_columns)
        brand = _first_non_empty(row, schema.brand_columns)
        product_id = _first_non_empty(row, schema.id_columns)
        prices = _extract_prices(row, schema)
        results.append(
            MatchResult(
                label=label,
                score=float(score),
                store=store,
                prices=prices,
                brand=brand,
                product_id=product_id,
                source_row=row.to_dict(),
            )
        )
    return results

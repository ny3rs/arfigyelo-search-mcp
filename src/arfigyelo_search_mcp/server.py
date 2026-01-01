"""MCP server exposing a fuzzy price search over the Árfigyelő Excel export."""

from __future__ import annotations

import os
from dataclasses import asdict
from typing import List

import pandas as pd
from mcp.server.fastmcp import FastMCP

from . import data
from .search import ColumnSchema, MatchResult, detect_columns, prepare_index, search_products

server = FastMCP("arfigyelo-search")


@server.tool()
def refresh_cache() -> str:
    """Force re-download of the latest Árfigyelő Excel file."""

    path = data.download_excel(force=True)
    return f"Downloaded latest excel to {path}"


def _load_index(force_download: bool = False) -> tuple[pd.DataFrame, ColumnSchema]:
    frame = data.load_dataframe(force_download=force_download)
    schema = detect_columns(frame)
    frame = prepare_index(frame, schema)
    return frame, schema


@server.tool()
def search_products_tool(
    query: str,
    limit: int = 5,
    min_score: float = 45.0,
    force_refresh: bool = False,
) -> List[dict]:
    """Search the Árfigyelő price list for products similar to the query.

    Parameters
    ----------
    query:
        Free-text product description (e.g., "Coca Cola 1.75 l").
    limit:
        Maximum number of results.
    min_score:
        Minimum RapidFuzz score to include a match (0-100).
    force_refresh:
        If True, redownloads the Excel file before searching.
    """

    try:
        frame, schema = _load_index(force_download=force_refresh)
    except Exception as exc:  # pragma: no cover - defensive path
        raise RuntimeError(
            "Failed to load Árfigyelő dataset. Check network access to the export URL."
        ) from exc

    matches = search_products(frame, query=query, limit=limit, min_score=min_score, schema=schema)
    return [_result_to_dict(match) for match in matches]


def _result_to_dict(result: MatchResult) -> dict:
    payload = asdict(result)
    payload["source_row"] = _compact_row(result.source_row)
    return payload


def _compact_row(row: dict) -> dict:
    # Reduce payload size by removing cached helper fields
    return {k: v for k, v in row.items() if not str(k).startswith("__")}


@server.tool()
def dataset_columns(force_refresh: bool = False) -> dict:
    """Inspect column detection for the cached dataset."""

    try:
        frame, schema = _load_index(force_download=force_refresh)
    except Exception as exc:  # pragma: no cover - defensive path
        raise RuntimeError(
            "Failed to load Árfigyelő dataset. Check network access to the export URL."
        ) from exc
    return {
        "rows": len(frame),
        "detected_schema": asdict(schema),
        "columns": list(frame.columns),
    }


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8000"))
    server.run(port=port)

"""Command-line helper for running a single search query."""
from __future__ import annotations

import argparse
from typing import Optional

from .data import load_dataframe
from .search import prepare_index, search_products


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run a fuzzy Árfigyelő product search")
    parser.add_argument("query", help="Product description to search for")
    parser.add_argument("--limit", type=int, default=5, help="Maximum matches to return (default: 5)")
    parser.add_argument("--nrows", type=int, default=None, help="Optional row limit for sampling")
    parser.add_argument("--source", help="Local Excel file to use instead of downloading", default=None)
    parser.add_argument(
        "--no-trust-env",
        action="store_true",
        help="Disable requests proxy/environment configuration when downloading",
    )
    return parser.parse_args()


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args()

    df = load_dataframe(source=args.source, nrows=args.nrows, trust_env=not args.no_trust_env)
    df_indexed = prepare_index(df)
    results = search_products(df_indexed, args.query, limit=args.limit)

    if not results:
        print("No matches found")
        return 1

    for idx, match in enumerate(results, 1):
        print(f"#{idx} score={match.score:.1f} label={match.label}")
        if match.brand:
            print(f"  brand: {match.brand}")
        if match.store:
            print(f"  store: {match.store}")
        if match.product_id:
            print(f"  id: {match.product_id}")
        if match.prices:
            print("  prices:")
            for col, price in match.prices.items():
                print(f"    {col}: {price}")
        print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

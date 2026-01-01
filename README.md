# Árfigyelő search MCP server

This repository provides an [MCP](https://modelcontextprotocol.io/) server that indexes the daily Árfigyelő Excel export and offers a fuzzy/AI-assisted product price lookup tool. It is designed for deployment on Hugging Face Spaces but can run anywhere Python 3.12+ is available.

## Features
- Downloads and caches the latest Árfigyelő daily Excel export.
- Normalizes and tokenizes product names, brands, and packaging for fuzzy matching.
- Uses RapidFuzz scoring to return the closest products to a free-text query.
- Returns store-level price rows with metadata for each match.

## Setup
```bash
pip install -e .
```

## Running the MCP server
```bash
python -m arfigyelo_search_mcp.server
```

Set `PORT` to control the listening port (defaults to `8000`).

### One-off searches from the CLI
Use the included CLI helper to try a query without wiring up a client:

```bash
arfigyelo-search "Alpro This is Not Milk Zabital 3,5%" --limit 5
```

Set `ARFIGYELO_SOURCE` to point at a local Excel export (or pass `--source`) to
skip downloading. If your environment injects proxies that block the Azureedge
download, pass `--no-trust-env` to disable proxy lookup during download or set
`ARFIGYELO_TRUST_ENV=0`.

### Exposed tools
- `search_products_tool`: fuzzy search returning store-level prices for the closest matches.
- `dataset_columns`: preview detected schema and column names (helpful for debugging).
- `refresh_cache`: force a redownload of the Excel export.

## Example tool call
```
# In a client that speaks MCP
call search_products {"query": "Coca Cola 1.75l", "limit": 5}
```

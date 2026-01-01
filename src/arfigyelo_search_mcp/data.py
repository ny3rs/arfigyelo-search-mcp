"""Data loading and caching helpers for the Árfigyelő Excel export."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd
import requests

ARFIGYELO_URL = "https://cdnarfigyeloprodweu.azureedge.net/excel/arfigyelo_napi_termekadatok.xlsx"
DEFAULT_CACHE_DIR = Path(os.environ.get("ARFIGYELO_CACHE", Path.home() / ".cache" / "arfigyelo-search"))


def _cache_path(url: str = ARFIGYELO_URL) -> Path:
    digest = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    return DEFAULT_CACHE_DIR / f"{digest}.xlsx"


def download_excel(
    url: str = ARFIGYELO_URL,
    force: bool = False,
    timeout: int = 60,
    trust_env: bool = True,
) -> Path:
    """Download the Árfigyelő Excel export to the local cache.

    Returns the cached path. If the file already exists and ``force`` is False, the
    cached version is reused.
    """

    destination = _cache_path(url)
    destination.parent.mkdir(parents=True, exist_ok=True)

    if destination.exists() and not force:
        return destination

    session = requests.Session()
    session.trust_env = trust_env
    response = session.get(url, stream=True, timeout=timeout)
    response.raise_for_status()

    temp_path = destination.with_suffix(".tmp")
    with temp_path.open("wb") as handle:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            if chunk:
                handle.write(chunk)
    temp_path.replace(destination)
    return destination


def load_dataframe(
    url: str = ARFIGYELO_URL,
    force_download: bool = False,
    nrows: Optional[int] = None,
    usecols: Optional[Iterable[str]] = None,
    source: Optional[str] = None,
    trust_env: Optional[bool] = None,
) -> pd.DataFrame:
    """Load the Árfigyelő Excel export as a DataFrame.

    Parameters
    ----------
    url:
        Source URL of the Excel export.
    force_download:
        If True, redownloads the file even when a cached copy is present.
    nrows:
        Optional row limit for sampling/testing.
    usecols:
        Optional explicit column subset.

    ``ARFIGYELO_SOURCE`` environment variable can point to a local file to
    bypass downloading the remote export (useful when working offline).

    The ``trust_env`` flag is forwarded to ``requests.Session.trust_env`` to
    allow bypassing proxy configuration when necessary.
    """

    resolved_source = source or os.environ.get("ARFIGYELO_SOURCE")
    resolved_trust_env = trust_env
    if resolved_trust_env is None:
        env_value = os.environ.get("ARFIGYELO_TRUST_ENV")
        if env_value is not None:
            resolved_trust_env = env_value.lower() not in {"0", "false", "no"}
        else:
            resolved_trust_env = True

    if resolved_source:
        excel_path = Path(resolved_source)
        if not excel_path.exists():
            raise FileNotFoundError(f"ARFIGYELO_SOURCE={resolved_source} not found")
        return pd.read_excel(excel_path, nrows=nrows, usecols=usecols)

    excel_path = download_excel(url=url, force=force_download, trust_env=resolved_trust_env)
    return pd.read_excel(excel_path, nrows=nrows, usecols=usecols)

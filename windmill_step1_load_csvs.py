"""Windmill flow step 1: load CSV files from GitHub raw URLs."""

from __future__ import annotations

import csv
from io import StringIO
from typing import Any

import requests

BASE_URL = (
    "https://raw.githubusercontent.com/"
    "kagna-voyager/windmill-onebill/main/Sample%20Data"
)


def _fetch_csv(url: str, row_limit: int) -> tuple[list[str], list[dict[str, str]]]:
    response = requests.get(url, timeout=45)
    response.raise_for_status()

    reader = csv.DictReader(StringIO(response.text))
    columns = reader.fieldnames or []

    rows: list[dict[str, str]] = []
    for idx, row in enumerate(reader):
        rows.append({k: (v or "") for k, v in row.items()})
        if idx + 1 >= row_limit:
            break

    return columns, rows


def main(
    file_names: list[str] | None = None,
    row_limit: int = 1000,
    base_url: str = BASE_URL,
) -> dict[str, Any]:
    """Download CSV files and return structured datasets.

    Args:
        file_names: CSV file names in Sample Data. Defaults to one file.
        row_limit: Max rows to read per file.
        base_url: GitHub raw base URL for CSV files.
    """
    if row_limit < 1:
        raise ValueError("row_limit must be >= 1")

    selected_files = file_names or ["1B_Subscription.csv"]

    datasets: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for file_name in selected_files:
        url = f"{base_url}/{file_name}"
        try:
            columns, rows = _fetch_csv(url, row_limit)
        except Exception as exc:  # noqa: BLE001
            errors.append({"file_name": file_name, "url": url, "error": str(exc)})
            continue

        datasets.append(
            {
                "file_name": file_name,
                "url": url,
                "columns": columns,
                "row_count": len(rows),
                "rows": rows,
            }
        )

    return {
        "base_url": base_url,
        "requested_files": selected_files,
        "loaded_files": [d["file_name"] for d in datasets],
        "datasets": datasets,
        "errors": errors,
    }

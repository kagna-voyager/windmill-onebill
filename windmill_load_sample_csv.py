"""Windmill example: load sample CSV from this repository."""

from __future__ import annotations

import csv
from io import StringIO
from typing import Any

import requests

BASE_URL = (
    "https://raw.githubusercontent.com/"
    "kagna-voyager/windmill-onebill/main/Sample%20Data"
)


def main(file_name: str = "1B_Subscription.csv", limit: int = 100) -> dict[str, Any]:
    """Download a sample CSV and return parsed rows.

    Args:
        file_name: CSV file in the Sample Data folder.
        limit: Maximum number of rows to return.
    """
    if limit < 1:
        raise ValueError("limit must be >= 1")

    url = f"{BASE_URL}/{file_name}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()

    reader = csv.DictReader(StringIO(response.text))
    rows = []
    for index, row in enumerate(reader):
        rows.append(row)
        if index + 1 >= limit:
            break

    return {
        "url": url,
        "row_count": len(rows),
        "columns": reader.fieldnames or [],
        "rows": rows,
    }

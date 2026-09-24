"""Windmill flow step 2: clean and preview CSV datasets."""

from __future__ import annotations

from typing import Any


def _clean_text(value: Any) -> Any:
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned if cleaned != "" else None
    return value


def main(load_result: dict[str, Any], preview_rows: int = 5) -> dict[str, Any]:
    """Normalize loaded datasets and create compact previews.

    Args:
        load_result: Output from windmill_step1_load_csvs.main.
        preview_rows: Number of rows to keep as display preview per file.
    """
    if preview_rows < 1:
        raise ValueError("preview_rows must be >= 1")

    datasets = load_result.get("datasets", [])

    prepared_datasets: list[dict[str, Any]] = []
    previews: list[dict[str, Any]] = []

    for dataset in datasets:
        columns = dataset.get("columns", [])
        rows = dataset.get("rows", [])

        cleaned_rows: list[dict[str, Any]] = []
        for row in rows:
            cleaned_row = {key: _clean_text(row.get(key)) for key in columns}
            cleaned_rows.append(cleaned_row)

        prepared_datasets.append(
            {
                "file_name": dataset.get("file_name"),
                "url": dataset.get("url"),
                "columns": columns,
                "row_count": len(cleaned_rows),
                "rows": cleaned_rows,
            }
        )

        previews.append(
            {
                "file_name": dataset.get("file_name"),
                "columns": columns,
                "row_count": len(cleaned_rows),
                "preview": cleaned_rows[:preview_rows],
            }
        )

    return {
        "loaded_files": load_result.get("loaded_files", []),
        "errors": load_result.get("errors", []),
        "prepared_datasets": prepared_datasets,
        "preview": previews,
    }

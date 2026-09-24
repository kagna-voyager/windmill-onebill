"""Windmill flow step 3: write prepared CSV datasets to a SQL database."""

from __future__ import annotations

import re
from typing import Any

from sqlalchemy import Column, MetaData, Table, Text, create_engine


def _sanitize_identifier(value: str) -> str:
    candidate = re.sub(r"[^0-9a-zA-Z_]+", "_", value.strip().lower())
    candidate = re.sub(r"_+", "_", candidate).strip("_")
    if not candidate:
        candidate = "col"
    if candidate[0].isdigit():
        candidate = f"c_{candidate}"
    return candidate


def _unique_identifiers(values: list[str]) -> list[str]:
    used: dict[str, int] = {}
    unique: list[str] = []
    for value in values:
        base = _sanitize_identifier(value)
        current = used.get(base, 0)
        used[base] = current + 1
        unique.append(base if current == 0 else f"{base}_{current}")
    return unique


def _chunked(items: list[dict[str, Any]], size: int) -> list[list[dict[str, Any]]]:
    return [items[idx : idx + size] for idx in range(0, len(items), size)]


def main(
    prepared_result: dict[str, Any],
    database_url: str,
    table_prefix: str = "sample_",
    write_mode: str = "append",
    batch_size: int = 500,
) -> dict[str, Any]:
    """Write prepared datasets into SQL tables.

    Args:
        prepared_result: Output from windmill_step2_prepare_preview.main.
        database_url: SQLAlchemy database URL.
        table_prefix: Prefix added to generated table names.
        write_mode: One of append or replace.
        batch_size: Number of rows inserted per batch.
    """
    if write_mode not in {"append", "replace"}:
        raise ValueError("write_mode must be one of: append, replace")
    if batch_size < 1:
        raise ValueError("batch_size must be >= 1")

    datasets = prepared_result.get("prepared_datasets", [])
    engine = create_engine(database_url)

    writes: list[dict[str, Any]] = []

    with engine.begin() as connection:
        for dataset in datasets:
            file_name = str(dataset.get("file_name", "dataset.csv"))
            source_columns = list(dataset.get("columns", []))
            rows = list(dataset.get("rows", []))

            table_stub = file_name.rsplit(".", 1)[0]
            table_name = _sanitize_identifier(f"{table_prefix}{table_stub}")

            sql_columns = _unique_identifiers(source_columns)
            metadata = MetaData()
            table = Table(
                table_name,
                metadata,
                *[Column(column_name, Text, nullable=True) for column_name in sql_columns],
            )

            if write_mode == "replace":
                table.drop(connection, checkfirst=True)
            table.create(connection, checkfirst=True)

            column_map = dict(zip(source_columns, sql_columns, strict=False))
            normalized_rows: list[dict[str, Any]] = []
            for row in rows:
                normalized_rows.append(
                    {
                        column_map[source_col]: (
                            None if row.get(source_col) is None else str(row.get(source_col))
                        )
                        for source_col in source_columns
                    }
                )

            inserted = 0
            for batch in _chunked(normalized_rows, batch_size):
                if batch:
                    connection.execute(table.insert(), batch)
                    inserted += len(batch)

            writes.append(
                {
                    "file_name": file_name,
                    "table_name": table_name,
                    "source_columns": source_columns,
                    "table_columns": sql_columns,
                    "rows_inserted": inserted,
                }
            )

    return {
        "write_mode": write_mode,
        "table_prefix": table_prefix,
        "writes": writes,
        "errors": prepared_result.get("errors", []),
    }

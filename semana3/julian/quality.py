from __future__ import annotations

def inspect_parquet(processed_paths: list[str]) -> dict:
    try:
        import pyarrow.parquet as pq
    except ModuleNotFoundError as exc:
        raise RuntimeError("pyarrow es requerido para perfilar Parquet") from exc

    rows = 0
    columns: set[str] = set()
    for path in processed_paths:
        table = pq.read_table(path)
        rows += table.num_rows
        columns.update(table.column_names)
    return {
        "paths_count": len(processed_paths),
        "rows_count": rows,
        "columns": sorted(columns),
    }


def evaluate_quality(profile: dict) -> dict:
    required_columns = {"order_id", "status"}
    present_columns = set(profile.get("columns", []))
    missing_columns = sorted(required_columns - present_columns)
    passed = profile.get("rows_count", 0) > 0 and not missing_columns
    return {
        "passed": passed,
        "missing_columns": missing_columns,
        "rows_count": profile.get("rows_count", 0),
    }

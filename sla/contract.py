"""Validación del contrato JSON de eventos SLA."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterable

from jsonschema import Draft202012Validator, FormatChecker

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCHEMA = ROOT / "docs" / "contrato_evento.json"


def load_schema(path: str | Path | None = None) -> dict[str, Any]:
    schema_path = Path(path) if path else DEFAULT_SCHEMA
    with schema_path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def _check_datetime_formats(event: Any) -> None:
    """Refuerza format=date-time sin depender de extras opcionales de jsonschema."""
    if not isinstance(event, dict):
        return
    for field in ("event_time", "promised_at", "picked_up_at", "delivered_at"):
        value = event.get(field)
        if isinstance(value, str):
            try:
                datetime.fromisoformat(value.replace("Z", "+00:00"))
            except ValueError as error:
                raise ValueError(f"{field} no es ISO-8601 válido: {value!r}") from error


def validate_event(event: Any, schema: dict[str, Any] | None = None) -> None:
    """Lanza una excepción si el evento no cumple el contrato."""
    validator = Draft202012Validator(
        schema or load_schema(),
        format_checker=FormatChecker(),
    )
    validator.validate(event)
    _check_datetime_formats(event)


def is_valid(event: Any, schema: dict[str, Any] | None = None) -> bool:
    try:
        validate_event(event, schema=schema)
    except Exception:
        return False
    return True


def load_events(path: str | Path) -> list[dict[str, Any]]:
    """Carga un evento único, una lista JSON o un archivo NDJSON."""
    source = Path(path)
    text = source.read_text(encoding="utf-8").strip()
    if not text:
        return []

    if text.startswith("["):
        data = json.loads(text)
        return list(data)

    if text.startswith("{"):
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return [json.loads(line) for line in text.splitlines() if line.strip()]
        return [data]

    return [json.loads(line) for line in text.splitlines() if line.strip()]


def validate_file(path: str | Path, schema: dict[str, Any] | None = None) -> int:
    events = load_events(path)
    for index, event in enumerate(events):
        try:
            validate_event(event, schema=schema)
        except Exception as error:
            raise ValueError(f"{path}: evento {index + 1} inválido: {error}") from error
    return len(events)


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Valida eventos contra docs/contrato_evento.json")
    parser.add_argument(
        "files",
        nargs="+",
        help="Archivos JSON o NDJSON con uno o varios eventos",
    )
    parser.add_argument("--schema", default=str(DEFAULT_SCHEMA), help="Ruta del JSON Schema")
    args = parser.parse_args(list(argv) if argv is not None else None)

    schema = load_schema(args.schema)
    exit_code = 0
    for path in args.files:
        try:
            count = validate_file(path, schema=schema)
            print(f"OK {path}: {count} evento(s) válido(s)")
        except Exception as error:
            print(f"ERROR {path}: {error}", file=sys.stderr)
            exit_code = 1
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())

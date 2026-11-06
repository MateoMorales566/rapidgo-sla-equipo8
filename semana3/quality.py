"""Lectura de Parquet y quality gate del lote procesado de RapidGo."""
from __future__ import annotations
import math
from typing import Iterable

REQUIRED = {'order_id', 'status', 'zone', 'event_time', 'promised_at'}
VALID_STATUSES = {'created', 'picked_up', 'in_transit', 'delivered', 'cancelled'}

def inspect_parquet(paths: Iterable[str]) -> dict:
    """Lee solo columnas necesarias, sin cargar Spark ni Kafka en Airflow."""
    import pyarrow.parquet as pq
    files = list(paths)
    if not files:
        raise ValueError('No se recibieron archivos Parquet')
    total_rows = 0
    problems = []
    columns = None
    for path in files:
        table = pq.read_table(path)
        available = set(table.column_names)
        columns = available if columns is None else columns & available
        total_rows += table.num_rows
        for i, row in enumerate(table.to_pylist()):
            if not row.get('order_id'):
                problems.append(f'{path} fila {i}: order_id vacío')
            if row.get('status') not in VALID_STATUSES:
                problems.append(f'{path} fila {i}: estado no válido')
            if not row.get('zone'):
                problems.append(f'{path} fila {i}: zone vacía')
    return {'files': len(files), 'columns': sorted(columns or []),
            'total_rows': total_rows, 'invalid_count': len(problems),
            'sample_problems': problems[:10]}

def evaluate_quality(profile: dict) -> dict:
    """Falla si hay esquema incompleto, archivo vacío, IDs nulos o estados inválidos."""
    missing = REQUIRED - set(profile.get('columns', []))
    total = profile.get('total_rows', 0)
    problems = []
    if missing:
        problems.append('Columnas faltantes: ' + ', '.join(sorted(missing)))
    if not total:
        problems.append('No hay filas procesadas')
    if profile.get('invalid_count', 0):
        problems += profile.get('sample_problems', ['Filas inválidas'])

    result = {'passed': not problems, 'total_rows': total,
              'invalid_rows_or_checks': len(problems), 'problems': problems[:20]}
    if problems:
        raise ValueError('QUALITY_GATE_FAILED: ' + '; '.join(problems[:5]))
    return result

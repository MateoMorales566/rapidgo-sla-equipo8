"""Funciones de las tasks: control batch del Data Lake, no reemplaza al streaming."""
from __future__ import annotations
import json
import os
from pathlib import Path
from datetime import datetime, timezone
from semana3.julian.quality import inspect_parquet, evaluate_quality
from semana3.kevin.reporting import summarize_sla, save_report

LAKE = Path(os.environ.get('RAPIDGO_DATALAKE', '/opt/rapidgo/datalake'))
REPORTS = Path(os.environ.get('RAPIDGO_REPORTS', '/opt/airflow/reports'))
FLAGS = Path(os.environ.get('RAPIDGO_FLAGS', '/opt/airflow/flags'))

def validate_source() -> dict:
    """XCom: paths de los datasets que produjo Spark y ruta del lote procesado."""
    processed = LAKE / 'processed'
    if not processed.is_dir():
        raise FileNotFoundError(f'No existe la capa processed: {processed}')
    paths = sorted(str(p) for p in processed.rglob('*.parquet'))
    if not paths:
        raise ValueError(f'No hay archivos Parquet para procesar en {processed}')
    return {'processed_paths': paths, 'discovered_files': len(paths)}

def profile_data(ti) -> dict:
    source = ti.xcom_pull(task_ids='validar_fuente')
    if not source:
        raise ValueError('XCom de validar_fuente ausente')
    return inspect_parquet(source['processed_paths'])

def quality_gate(ti) -> dict:
    profile = ti.xcom_pull(task_ids='perfilar_datos')
    if not profile:
        raise ValueError('XCom de perfilar_datos ausente')
    if (FLAGS / 'forzar_fallo_calidad').exists():
        raise RuntimeError('FALLO_CONTROLADO: bandera forzar_fallo_calidad activa')
    return evaluate_quality(profile)

def compute_metrics(ti) -> dict:
    gate = ti.xcom_pull(task_ids='validar_calidad')
    if not gate or not gate['passed']:
        raise RuntimeError('Quality gate no aprobado')
    source = ti.xcom_pull(task_ids='validar_fuente')
    return summarize_sla(source['processed_paths'])

def write_report(ti) -> dict:
    metrics = ti.xcom_pull(task_ids='calcular_indicadores')
    quality = ti.xcom_pull(task_ids='validar_calidad')
    if metrics is None or quality is None:
        raise ValueError('No están los XCom necesarios para reportar')
    return save_report({'metrics': metrics, 'quality': quality,
                        'generated_at': datetime.now(timezone.utc).isoformat()}, REPORTS)

def publish_summary(ti) -> None:
    output = ti.xcom_pull(task_ids='generar_reporte')
    if not output or not Path(output['path']).is_file():
        raise FileNotFoundError('No se generó el reporte final')
    print('Reporte SLA listo:', json.dumps(output, ensure_ascii=False))

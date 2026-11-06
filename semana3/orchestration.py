"""Funciones que ejecuta el DAG y un runner local equivalente.

Airflow solo aporta el scheduler, los retries y la interfaz visual. La lógica
vive aquí para poder probarla con pytest y ejecutarla con
``python -m semana3.orchestration`` sin levantar Airflow.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import tempfile
from pathlib import Path
from typing import Any

from semana3.config import quality_failure_flag
from semana3.quality import evaluate_quality, inspect_parquet, resolve_parquet_paths
from semana3.reporting import build_report, read_report, save_report, summarize_sla
from semana3.config import datalake_path, reports_path

TASK_IDS = (
    "validar_fuente",
    "perfilar_datos",
    "validar_calidad",
    "calcular_indicadores",
    "generar_reporte",
    "publicar_resultado",
)


def validate_source() -> dict[str, Any]:
    """Task 1: comprueba que Spark dejó Parquet en ``processed``.

    Devuelve por XCom solo metadatos pequeños (rutas y conteos), nunca las
    filas completas.
    """

    lake = datalake_path()
    processed = lake / "processed"
    if not processed.is_dir():
        raise FileNotFoundError(f"No existe la capa processed: {processed}")

    processed_paths = resolve_parquet_paths([processed])
    if not processed_paths:
        raise ValueError(f"No hay archivos Parquet para procesar en {processed}")

    raw = lake / "raw"
    raw_paths = resolve_parquet_paths([raw]) if raw.is_dir() else []
    total_bytes = sum(path.stat().st_size for path in processed_paths)

    return {
        "lake": str(lake),
        "processed_paths": [str(path) for path in processed_paths],
        "discovered_files": len(processed_paths),
        "raw_files": len(raw_paths),
        "processed_bytes": total_bytes,
    }


def profile_data(ti: Any) -> dict[str, Any]:
    """Task 2: perfila esquema, filas y problemas de calidad."""

    source = ti.xcom_pull(task_ids="validar_fuente")
    if not source:
        raise ValueError("XCom de validar_fuente ausente")
    return inspect_parquet(source["processed_paths"])


def quality_gate(ti: Any, failure_flag: str | Path | None = None) -> dict[str, Any]:
    """Task 3: aplica el gate y permite un fallo controlado por bandera."""

    profile = ti.xcom_pull(task_ids="perfilar_datos")
    if not profile:
        raise ValueError("XCom de perfilar_datos ausente")

    flag = Path(failure_flag) if failure_flag is not None else quality_failure_flag()
    if flag.exists():
        raise RuntimeError(
            "FALLO_CONTROLADO: bandera de calidad activa "
            f"({flag}). Bórrela y vuelva a ejecutar el DAG."
        )

    return evaluate_quality(profile)


def compute_metrics(ti: Any) -> dict[str, Any]:
    """Task 4: calcula KPIs y alertas desde la capa processed."""

    gate = ti.xcom_pull(task_ids="validar_calidad")
    if not gate or not gate.get("passed"):
        raise RuntimeError("Quality gate no aprobado")
    source = ti.xcom_pull(task_ids="validar_fuente")
    if not source:
        raise ValueError("XCom de validar_fuente ausente")
    return summarize_sla(source["processed_paths"])


def write_report(ti: Any) -> dict[str, Any]:
    """Task 5: escribe el reporte JSON de forma atómica."""

    metrics = ti.xcom_pull(task_ids="calcular_indicadores")
    quality = ti.xcom_pull(task_ids="validar_calidad")
    source = ti.xcom_pull(task_ids="validar_fuente")
    if metrics is None or quality is None or source is None:
        raise ValueError("No están los XCom necesarios para generar el reporte")

    report = build_report(
        metrics=metrics,
        quality=quality,
        source_files=source["processed_paths"],
    )
    return save_report(report, reports_path())


def publish_summary(ti: Any) -> dict[str, Any]:
    """Task 6: valida que el reporte exista y publica un resumen en el log."""

    output = ti.xcom_pull(task_ids="generar_reporte")
    if not output:
        raise ValueError("XCom de generar_reporte ausente")
    path = Path(output["path"])
    if not path.is_file():
        raise FileNotFoundError(f"No se generó el reporte final: {path}")

    report = read_report(path)
    metrics = report.get("metrics", {})
    summary = {
        "report": str(path),
        "size_bytes": output.get("size_bytes"),
        "pedidos_unicos": metrics.get("pedidos_unicos"),
        "tiempo_promedio_min": metrics.get("tiempo_promedio_min"),
        "porcentaje_fuera_sla": metrics.get("porcentaje_fuera_sla"),
        "alertas": {
            "SLA_EXCEEDED": metrics.get("alertas_sla_excedido"),
            "DELIVERY_STOPPED": metrics.get("alertas_delivery_stopped"),
        },
    }
    print("Reporte SLA listo:", json.dumps(summary, ensure_ascii=False))
    return summary


class _FakeTI:
    """Implementación mínima de XCom para ejecutar el DAG sin Airflow."""

    def __init__(self) -> None:
        self.values: dict[str, Any] = {}

    def xcom_pull(self, task_ids: Any = None, key: str = "return_value", **_: Any) -> Any:
        if isinstance(task_ids, (list, tuple, set)):
            return {task_id: self.values.get(task_id) for task_id in task_ids}
        return self.values.get(task_ids)


def run_pipeline(force_failure: bool = False) -> dict[str, Any]:
    """Ejecuta las seis tareas en orden con un XCom en memoria."""

    ti = _FakeTI()
    ti.values["validar_fuente"] = validate_source()
    ti.values["perfilar_datos"] = profile_data(ti)

    flag: Path | None = None
    if force_failure:
        handle = tempfile.NamedTemporaryFile(
            mode="w", prefix="fallo_calidad_", suffix=".flag", delete=False
        )
        handle.close()
        flag = Path(handle.name)

    try:
        ti.values["validar_calidad"] = quality_gate(ti, failure_flag=flag)
    finally:
        if flag is not None:
            flag.unlink(missing_ok=True)

    ti.values["calcular_indicadores"] = compute_metrics(ti)
    ti.values["generar_reporte"] = write_report(ti)
    ti.values["publicar_resultado"] = publish_summary(ti)

    output = ti.values["generar_reporte"]
    return {
        "tasks": list(TASK_IDS),
        "output": output,
        "report": read_report(output["path"]),
        "summary": ti.values["publicar_resultado"],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Runner local de las tareas de la Semana 3 (sin Airflow)"
    )
    parser.add_argument("--lake", help="Raíz del Data Lake (default: RAPIDGO_DATALAKE)")
    parser.add_argument("--reports", help="Directorio de reportes (default: RAPIDGO_REPORTS)")
    parser.add_argument(
        "--force-failure",
        action="store_true",
        help="Fuerza el fallo del quality gate para evidenciar la recuperación",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Imprime el reporte completo en JSON",
    )
    args = parser.parse_args(argv)

    if args.lake:
        os.environ["RAPIDGO_DATALAKE"] = args.lake
    if args.reports:
        os.environ["RAPIDGO_REPORTS"] = args.reports

    try:
        result = run_pipeline(force_failure=args.force_failure)
    except Exception as error:  # noqa: BLE001 - la CLI debe mostrar el error real
        print(f"ERROR: {error}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(result["report"], ensure_ascii=False, indent=2, sort_keys=True))
    else:
        summary = result["summary"]
        print(f"Reporte: {summary['report']}")
        print(f"Pedidos únicos: {summary['pedidos_unicos']}")
        print(f"Tiempo promedio: {summary['tiempo_promedio_min']} min")
        print(f"Porcentaje fuera de SLA: {summary['porcentaje_fuera_sla']}%")
        print(f"Alertas SLA_EXCEEDED: {summary['alertas']['SLA_EXCEEDED']}")
        print(f"Alertas DELIVERY_STOPPED: {summary['alertas']['DELIVERY_STOPPED']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

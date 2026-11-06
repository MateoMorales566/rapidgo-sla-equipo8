"""Validaciones estáticas del DAG (sin necesitar Airflow instalado).

La importación real del DAG se hace en el job ``dag-airflow`` del CI, donde sí
se instala Airflow. Estas pruebas protegen la estructura mínima del archivo.
"""

from __future__ import annotations

import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DAG_PATH = ROOT / "airflow" / "dags" / "rapidgo_sla_orchestration.py"
ORCHESTRATION_PATH = ROOT / "semana3" / "orchestration.py"

EXPECTED_TASKS = {
    "validar_fuente",
    "perfilar_datos",
    "validar_calidad",
    "calcular_indicadores",
    "generar_reporte",
    "publicar_resultado",
}


def _tree() -> ast.Module:
    return ast.parse(DAG_PATH.read_text(encoding="utf-8"))


def test_dag_has_six_python_operators():
    calls = [
        node
        for node in ast.walk(_tree())
        if isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == "PythonOperator"
    ]
    task_ids = {
        keyword.value.value
        for call in calls
        for keyword in call.keywords
        if keyword.arg == "task_id" and isinstance(keyword.value, ast.Constant)
    }
    assert len(calls) == 6
    assert task_ids == EXPECTED_TASKS


def test_dag_is_manual_with_retries_and_one_active_run():
    source = DAG_PATH.read_text(encoding="utf-8")
    assert "schedule=None" in source
    assert '"retries": 2' in source
    assert "max_active_runs=1" in source
    assert "catchup=False" in source
    assert 'dag_id="rapidgo_sla_semana3"' in source


def test_dag_chains_all_tasks():
    source = DAG_PATH.read_text(encoding="utf-8")
    assert source.count(">>") == 5
    for task_id in EXPECTED_TASKS:
        assert task_id in source


def test_tasks_use_xcom_and_import_from_package():
    source = ORCHESTRATION_PATH.read_text(encoding="utf-8")
    dag_source = DAG_PATH.read_text(encoding="utf-8")

    assert "xcom_pull" in source
    assert "from semana3.orchestration import" in dag_source
    assert "semana3.andres" not in dag_source
    assert "semana3.julian" not in dag_source
    assert "semana3.kevin" not in dag_source


def test_failure_flag_is_documented():
    source = DAG_PATH.read_text(encoding="utf-8")
    assert "forzar_fallo_calidad" in source
    assert "FALLO_CONTROLADO" in source

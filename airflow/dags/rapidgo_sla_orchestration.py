"""DAG Semana 3: control batch del Data Lake con seis tareas.

Flujo:
    validar_fuente -> perfilar_datos -> validar_calidad
    -> calcular_indicadores -> generar_reporte -> publicar_resultado

El DAG no reemplaza a Spark Structured Streaming. Lee la capa ``processed`` que
dejó Spark, aplica un quality gate y publica un reporte JSON. Se ejecuta de
forma manual (``schedule=None``) para no competir con el streaming.

Fallo controlado:
    Crear ``airflow/flags/forzar_fallo_calidad`` y ejecutar el DAG. La tarea
    ``validar_calidad`` falla con ``FALLO_CONTROLADO`` y las tareas aguas abajo
    quedan bloqueadas. Borrar la bandera y lanzar un nuevo run para recuperar.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from semana3.orchestration import (
    compute_metrics,
    profile_data,
    publish_summary,
    quality_gate,
    validate_source,
    write_report,
)

with DAG(
    dag_id="rapidgo_sla_semana3",
    description="Calidad, KPIs y reporte batch sobre la capa processed de RapidGo",
    doc_md=__doc__,
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    max_active_runs=1,
    dagrun_timeout=timedelta(minutes=30),
    default_args={
        "owner": "equipo8",
        "retries": 2,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(minutes=10),
        "depends_on_past": False,
    },
    tags=["rapidgo", "equipo8", "sla", "semana3", "calidad"],
) as dag:
    validar_fuente = PythonOperator(
        task_id="validar_fuente",
        python_callable=validate_source,
    )
    perfilar_datos = PythonOperator(
        task_id="perfilar_datos",
        python_callable=profile_data,
    )
    validar_calidad = PythonOperator(
        task_id="validar_calidad",
        python_callable=quality_gate,
        # Un fallo de calidad no se arregla reintentando: se corrige la bandera
        # o el dato. Sin retries, el fallo controlado termina y deja las tareas
        # aguas abajo en UPSTREAM_FAILED; el resto del DAG conserva retries=2.
        retries=0,
    )
    calcular_indicadores = PythonOperator(
        task_id="calcular_indicadores",
        python_callable=compute_metrics,
    )
    generar_reporte = PythonOperator(
        task_id="generar_reporte",
        python_callable=write_report,
    )
    publicar_resultado = PythonOperator(
        task_id="publicar_resultado",
        python_callable=publish_summary,
    )

    (
        validar_fuente
        >> perfilar_datos
        >> validar_calidad
        >> calcular_indicadores
        >> generar_reporte
        >> publicar_resultado
    )

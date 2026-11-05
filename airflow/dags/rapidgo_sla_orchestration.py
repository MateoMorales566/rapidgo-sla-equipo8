"""DAG Semana 3: seis tasks secuenciales, retries, XCom y recuperación."""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from semana3.andres.orchestration import (
    validate_source, profile_data, quality_gate, compute_metrics,
    write_report, publish_summary,
)

with DAG(
    dag_id='rapidgo_sla_semana3',
    description='Control batch del Data Lake generado por Spark Structured Streaming',
    start_date=datetime(2026, 11, 1),
    schedule=None,
    catchup=False,
    default_args={'owner': 'equipo8', 'retries': 2,
                  'retry_delay': timedelta(seconds=30)},
    tags=['rapidgo', 'equipo8', 'sla', 'semana3'],
) as dag:
    validar_fuente = PythonOperator(task_id='validar_fuente', python_callable=validate_source)
    perfilar_datos = PythonOperator(task_id='perfilar_datos', python_callable=profile_data)
    validar_calidad = PythonOperator(task_id='validar_calidad', python_callable=quality_gate)
    calcular_indicadores = PythonOperator(task_id='calcular_indicadores', python_callable=compute_metrics)
    generar_reporte = PythonOperator(task_id='generar_reporte', python_callable=write_report)
    publicar_resultado = PythonOperator(task_id='publicar_resultado', python_callable=publish_summary)

    validar_fuente >> perfilar_datos >> validar_calidad >> calcular_indicadores >> generar_reporte >> publicar_resultado

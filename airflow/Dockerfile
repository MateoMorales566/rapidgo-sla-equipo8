# Imagen de Airflow para la demo de la Semana 3.
# Se agrega pyarrow porque el quality gate lee Parquet sin cargar Spark.
FROM apache/airflow:2.9.3

USER airflow

RUN pip install --no-cache-dir "pyarrow==16.1.0"

# ADR-003 — Orquestación batch y recuperación de fallos

## Contexto
La Semana 2 mantiene Spark Structured Streaming como procesamiento continuo. Airflow no debe sustituirlo ni reescribir sus checkpoints: debe inspeccionar el Parquet ya producido, ejecutar controles y emitir reportes.

## Opciones consideradas
1. Controlar directamente los procesos streaming desde tareas periódicas de Airflow.
2. Usar un DAG batch bajo demanda para revisar la capa `processed` y generar indicadores/reportes.

## Decisión
Se adopta la opción 2: un DAG sin cron (`schedule=None`) con seis tareas, `retries=2`, `retry_delay=30s` y XCom para transportar metadatos pequeños. La lectura de los eventos sigue en Parquet; **no se intercambian datasets grandes por XCom**.

## Recuperación
Se crea `airflow/flags/forzar_fallo_calidad`, se ejecuta el DAG y se comprueba el fallo `FALLO_CONTROLADO`. Después se elimina la bandera y se ejecuta otra vez. Las tareas no borran Parquet ni checkpoints de Spark. El reporte se reemplaza atómicamente (`.tmp` -> `.json`) para evitar salidas parciales.

## Evidencia pendiente
Completar al ejecutar localmente: fecha, DAG run ID, error exacto copiado de Airflow, captura del primer run fallido y segundo run exitoso, enlaces de PR y Actions. No atribuir estas pruebas a una ejecución que aún no ocurrió.

## Consecuencias
Ventaja: separación de streaming y control batch, fácil depuración. Coste: más componentes Docker y dependencia de `pyarrow`. El gate falla cuando el directorio está vacío o faltan datos fundamentales.

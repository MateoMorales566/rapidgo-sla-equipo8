# Bitácora de ejecución Semana 3

| Validación | Comando / lugar | Evidencia real | Estado |
|---|---|---|---|
| Calidad y tests | `python -m pytest -q` | Completar conteo real | Pendiente |
| Docker | `docker compose -f docker-compose.yml -f docker-compose.semana3.yml config --quiet` | Captura/salida | Pendiente |
| DAG disponible | Airflow http://localhost:8088 | Captura de 6 tasks | Pendiente |
| Fallo controlado | Bandera `forzar_fallo_calidad` | Captura y log real | Pendiente |
| Recuperación | Eliminar bandera, volver a ejecutar | Captura run verde | Pendiente |
| CI | GitHub Actions en cada PR | Enlaces reales | Pendiente |

Para fallar: crear archivo vacío `airflow/flags/forzar_fallo_calidad` desde VS Code/terminal. Esperar fin de retries del run fallido. Borrar archivo, lanzar un NUEVO run. Registrar qué tareas quedaron bloqueadas.

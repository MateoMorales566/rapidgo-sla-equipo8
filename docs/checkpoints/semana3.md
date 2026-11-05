# Checkpoint Semana 3 — COMPLETAR CON EVIDENCIAS REALES (máximo 1 página)

1. **Qué quedó funcionando:** DAG `rapidgo_sla_semana3` con seis tareas, XCom, retries, validación de Parquet, quality gate y reporte JSON. Confirmar con ejecución real.
2. **Resultado real:** Fecha ___; DAG run ID ___; cantidad de archivos ___; filas ___; promedio SLA ___; porcentaje fuera de SLA ___; captura ___
3. **Decisión:** orquestación batch desacoplada de Spark streaming; ver ADR-003.
4. **Fallo controlado:** tarea `validar_calidad`; mensaje REAL del log ___; tareas aguas abajo bloqueadas ___; ajuste efectuado ___; run de recuperación ___
5. **Trazabilidad:** Issue Andrés ___ / PR ___; Issue Julián ___ / PR ___; Issue Kevin ___ / PR ___; Actions CI ___; Release `v0.3.0-orchestration` ___

> No inventar un log, un resultado ni un link: reemplazar los campos después de probar.

"""Demostración batch de las mismas transformaciones, sin Kafka.

Sirve para validar el lote de evidencia y para ejecutar Spark localmente
cuando no se quiere levantar el stack completo.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:  # python -m spark.batch_demo
    from spark.transforms import build_alerts, compute_kpis, enrich_events, filter_valid
except ImportError:  # python spark/batch_demo.py
    from transforms import build_alerts, compute_kpis, enrich_events, filter_valid  # type: ignore

from pyspark.sql import SparkSession, functions as F


def run(input_path: str, write_path: str | None = None) -> int:
    spark = (
        SparkSession.builder.appName("RapidGoSLABatchDemo")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.option("multiLine", "true").json(input_path)
        enriched = filter_valid(enrich_events(df)).cache()
        total = enriched.count()
        if total == 0:
            print("No hay eventos válidos para analizar", flush=True)
            return 1

        reference = enriched.agg(F.max("event_time")).first()[0]
        kpis = compute_kpis(enriched).orderBy("ventana_inicio", "zone")
        alerts = build_alerts(enriched, reference_time=F.lit(reference))

        print(f"Eventos válidos: {total}", flush=True)
        print(f"Reloj de referencia: {reference}", flush=True)
        print("KPIs por ventana de 15 minutos:", flush=True)
        kpis.show(50, truncate=False)
        print("Alertas:", flush=True)
        alerts.show(50, truncate=False)

        if write_path:
            try:
                kpis.write.mode("overwrite").parquet(f"{write_path}/kpis")
                alerts.write.mode("overwrite").parquet(f"{write_path}/alerts")
                print(f"Resultados escritos en {write_path}", flush=True)
            except Exception as error:  # Windows requiere winutils para escribir Parquet local
                print(
                    f"ADVERTENCIA: no se pudo escribir Parquet en este sistema: {error}",
                    flush=True,
                )
    finally:
        spark.stop()

    return 0


def main(argv: Iterable[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Demo batch de KPIs y alertas SLA")
    parser.add_argument("--input", required=True)
    parser.add_argument("--write", default="")
    args = parser.parse_args(list(argv) if argv is not None else None)
    return run(args.input, args.write or None)


if __name__ == "__main__":
    raise SystemExit(main())

"""Job de Spark Structured Streaming para la variante SLA de RapidGo."""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path
from typing import Iterable

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:  # spark-submit ejecuta el archivo directamente
    from spark.transforms import (
        build_alerts,
        compute_kpis,
        enrich_events,
        filter_valid,
        parse_events,
    )
except ImportError:  # pragma: no cover - fallback para spark-submit desde spark/
    from transforms import (  # type: ignore
        build_alerts,
        compute_kpis,
        enrich_events,
        filter_valid,
        parse_events,
    )

from pyspark.sql import DataFrame, SparkSession, functions as F

DEFAULT_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
DEFAULT_TOPIC = os.getenv("KAFKA_TOPIC", "rapidgo.sla.events")
DEFAULT_DATALAKE = os.getenv("DATALAKE_BASE", "/opt/rapidgo/datalake")


def default_kafka_package() -> str:
    import pyspark

    major = int(pyspark.__version__.split(".")[0])
    scala = "2.13" if major >= 4 else "2.12"
    return f"org.apache.spark:spark-sql-kafka-0-10_{scala}:{pyspark.__version__}"


def build_spark(master: str, kafka_package: str) -> SparkSession:
    builder = (
        SparkSession.builder.appName("RapidGoSLAStreaming")
        .master(master)
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    )
    if kafka_package:
        builder = builder.config("spark.jars.packages", kafka_package)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel(os.getenv("SPARK_LOG_LEVEL", "WARN"))
    return spark


def resolve_base(path: str) -> str:
    resolved = Path(path).resolve()
    resolved.mkdir(parents=True, exist_ok=True)
    return resolved.as_uri()


def read_kafka(spark: SparkSession, bootstrap: str, topic: str) -> DataFrame:
    return (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )


def write_curated(batch_df: DataFrame, batch_id: int, base_path: str) -> None:
    """Escribe KPIs y alertas de cada micro-lote en la capa curated."""
    if batch_df.isEmpty():
        return

    kpis = compute_kpis(batch_df).cache()
    alerts = build_alerts(batch_df).cache()

    kpi_rows = kpis.count()
    alert_rows = alerts.count()

    if kpi_rows:
        kpis.write.mode("append").parquet(f"{base_path}/curated/kpis")
    if alert_rows:
        alerts.write.mode("append").parquet(f"{base_path}/curated/alerts")

    print(f"[curated] batch={batch_id} kpis={kpi_rows} alertas={alert_rows}", flush=True)
    kpis.unpersist()
    alerts.unpersist()


def start_query(query, trigger: str, interval: str):
    if trigger == "availableNow":
        return query.trigger(availableNow=True).start()
    return query.trigger(processingTime=interval).start()


def run(args: argparse.Namespace) -> int:
    base = resolve_base(args.datalake)
    checkpoint_root = f"{base}/_checkpoints"
    master = args.spark_master or os.getenv("SPARK_MASTER", "local[*]")
    package = args.kafka_package or os.getenv("SPARK_KAFKA_PACKAGE") or default_kafka_package()

    print(
        f"Spark master={master} kafka={args.bootstrap_servers} topic={args.topic} "
        f"datalake={base} trigger={args.trigger}",
        flush=True,
    )
    if package:
        print(f"Paquete Kafka-Spark: {package}", flush=True)

    spark = build_spark(master, package)
    queries = []

    try:
        kafka_df = read_kafka(spark, args.bootstrap_servers, args.topic)

        payload = kafka_df.select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("raw_value"),
            "topic",
            "partition",
            "offset",
            F.col("timestamp").alias("kafka_timestamp"),
        ).withColumn("ingested_at", F.current_timestamp())

        raw_query = (
            payload.writeStream.format("parquet")
            .outputMode("append")
            .option("path", f"{base}/raw")
            .option("checkpointLocation", f"{checkpoint_root}/raw")
            .start()
        )
        queries.append(raw_query)

        parsed = parse_events(payload)
        enriched = filter_valid(enrich_events(parsed)).withWatermark(
            "event_time", "2 minutes"
        )
        processed = enriched.select(
            "event_id",
            "team_id",
            "event_type",
            "event_time",
            "order_id",
            "zone",
            "distance_km",
            "promised_at",
            "picked_up_at",
            "delivered_at",
            "status",
            "delivery_minutes",
            "minutes_late",
            "sla_status",
            "is_late",
            "event_date",
            "kafka_key",
            "kafka_timestamp",
            "ingested_at",
        )

        processed_query = (
            processed.writeStream.format("parquet")
            .outputMode("append")
            .option("path", f"{base}/processed")
            .option("checkpointLocation", f"{checkpoint_root}/processed")
            .start()
        )
        queries.append(processed_query)

        curated_query = (
            processed.writeStream.outputMode("append")
            .foreachBatch(lambda df, batch_id: write_curated(df, batch_id, base))
            .option("checkpointLocation", f"{checkpoint_root}/curated")
            .start()
        )
        queries.append(curated_query)

        print("Consultas activas: raw, processed, curated", flush=True)

        if args.duration:
            spark.streams.awaitAnyTermination(args.duration)
        else:
            spark.streams.awaitAnyTermination()

    except KeyboardInterrupt:
        print("Deteniendo consultas por interrupción del usuario...", flush=True)
    finally:
        for query in queries:
            try:
                query.stop()
            except Exception:
                pass
        spark.stop()

    return 0


def parse_args(argv: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Spark Structured Streaming - SLA RapidGo")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP)
    parser.add_argument("--topic", default=DEFAULT_TOPIC)
    parser.add_argument("--datalake", default=DEFAULT_DATALAKE)
    parser.add_argument("--spark-master", default=os.getenv("SPARK_MASTER", "local[*]"))
    parser.add_argument("--kafka-package", default="")
    parser.add_argument(
        "--trigger",
        choices=("processingTime", "availableNow"),
        default="processingTime",
    )
    parser.add_argument("--interval", default="10 seconds")
    parser.add_argument(
        "--duration",
        type=int,
        default=0,
        help="Segundos antes de detener el job (0 = indefinido)",
    )
    return parser.parse_args(list(argv) if argv is not None else None)


if __name__ == "__main__":
    args = parse_args()
    if args.trigger == "availableNow":
        args.duration = 0
    raise SystemExit(run(args))

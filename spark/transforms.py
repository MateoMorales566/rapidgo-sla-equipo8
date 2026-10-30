"""Transformaciones reutilizables de Spark para la variante SLA."""

from __future__ import annotations

from pyspark.sql import DataFrame, functions as F, types as T

TEAM_ID = "equipo8"
SLA_TARGET_MINUTES = 45.0
STOPPED_THRESHOLD_MINUTES = 30.0
FINAL_STATUSES = ("delivered", "cancelled")
VALID_STATUSES = ("created", "picked_up", "in_transit", "delivered", "cancelled")

SLA_SCHEMA = T.StructType(
    [
        T.StructField("event_id", T.StringType(), True),
        T.StructField("team_id", T.StringType(), True),
        T.StructField("event_type", T.StringType(), True),
        T.StructField("event_time", T.StringType(), True),
        T.StructField("order_id", T.StringType(), True),
        T.StructField("zone", T.StringType(), True),
        T.StructField("distance_km", T.DoubleType(), True),
        T.StructField("promised_at", T.StringType(), True),
        T.StructField("picked_up_at", T.StringType(), True),
        T.StructField("delivered_at", T.StringType(), True),
        T.StructField("status", T.StringType(), True),
    ]
)


def parse_events(kafka_df: DataFrame) -> DataFrame:
    """Parsea el JSON de Kafka y conserva los metadatos del mensaje."""
    return (
        kafka_df.withColumn("data", F.from_json(F.col("raw_value"), SLA_SCHEMA))
        .select("*", "data.*")
        .drop("data")
    )


def enrich_events(df: DataFrame) -> DataFrame:
    """Transformación 1-3: timestamps, duración real y clasificación del SLA."""
    return (
        df.withColumn("event_time", F.to_timestamp("event_time"))
        .withColumn("promised_at", F.to_timestamp("promised_at"))
        .withColumn("picked_up_at", F.to_timestamp("picked_up_at"))
        .withColumn("delivered_at", F.to_timestamp("delivered_at"))
        .withColumn(
            "delivery_minutes",
            F.round(
                (F.col("delivered_at").cast("long") - F.col("picked_up_at").cast("long"))
                / 60.0,
                2,
            ),
        )
        .withColumn(
            "minutes_late",
            F.round(
                (F.col("delivered_at").cast("long") - F.col("promised_at").cast("long"))
                / 60.0,
                2,
            ),
        )
        .withColumn(
            "sla_status",
            F.when(F.col("status") == "cancelled", F.lit("CANCELADO"))
            .when(F.col("delivered_at").isNull(), F.lit("PENDIENTE"))
            .when(F.col("delivered_at") <= F.col("promised_at"), F.lit("CUMPLE"))
            .otherwise(F.lit("FUERA_SLA")),
        )
        .withColumn("is_late", F.col("sla_status") == F.lit("FUERA_SLA"))
        .withColumn("event_date", F.to_date("event_time"))
    )


def filter_valid(df: DataFrame) -> DataFrame:
    """Descarta eventos incompletos, de otro equipo o con estado desconocido."""
    return df.filter(
        F.col("event_id").isNotNull()
        & F.col("order_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("promised_at").isNotNull()
        & F.col("status").isin(*VALID_STATUSES)
        & (F.col("team_id") == F.lit(TEAM_ID))
    )


def compute_kpis(df: DataFrame) -> DataFrame:
    """Agregación por ventana de 15 minutos y zona.

    Se ejecuta dentro de foreachBatch (batch), por eso puede usar
    percentile_approx sin las restricciones de las agregaciones streaming.
    """
    delivered = F.col("status") == F.lit("delivered")
    in_progress = ~F.col("status").isin(*FINAL_STATUSES)

    return (
        df.groupBy(F.window("event_time", "15 minutes").alias("window"), "zone")
        .agg(
            F.count("*").alias("eventos"),
            F.count(F.when(delivered, F.lit(1))).alias("pedidos_entregados"),
            F.count(F.when(in_progress, F.lit(1))).alias("pedidos_en_curso"),
            F.round(F.avg(F.when(delivered, F.col("delivery_minutes"))), 2).alias(
                "tiempo_promedio_min"
            ),
            F.round(
                F.percentile_approx(F.when(delivered, F.col("delivery_minutes")), 0.95),
                2,
            ).alias("p95_entrega_min"),
            F.sum(F.when(F.col("is_late"), F.lit(1)).otherwise(F.lit(0))).alias(
                "pedidos_retrasados"
            ),
        )
        .withColumn(
            "porcentaje_fuera_sla",
            F.when(
                F.col("pedidos_entregados") > 0,
                F.round(
                    100.0 * F.col("pedidos_retrasados") / F.col("pedidos_entregados"), 2
                ),
            ).otherwise(F.lit(0.0)),
        )
        .select(
            F.col("window.start").alias("ventana_inicio"),
            F.col("window.end").alias("ventana_fin"),
            "zone",
            "eventos",
            "pedidos_entregados",
            "pedidos_en_curso",
            "tiempo_promedio_min",
            "p95_entrega_min",
            "pedidos_retrasados",
            "porcentaje_fuera_sla",
        )
    )


def build_alerts(df: DataFrame, reference_time=None) -> DataFrame:
    """Genera las dos alertas de la variante: SLA_EXCEEDED y DELIVERY_STOPPED."""
    from pyspark.sql import Window

    latest = (
        df.withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("order_id").orderBy(F.col("event_time").desc())
            ),
        )
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    if reference_time is None:
        reference = F.current_timestamp()
    elif isinstance(reference_time, str):
        reference = F.to_timestamp(F.lit(reference_time))
    else:
        reference = reference_time

    late = (
        latest.filter(F.col("sla_status") == F.lit("FUERA_SLA"))
        .select(
            F.lit("SLA_EXCEEDED").alias("alert_code"),
            "order_id",
            "zone",
            "status",
            F.col("minutes_late").alias("minutes"),
            F.when(F.col("minutes_late") >= 15, F.lit("ALTA"))
            .otherwise(F.lit("MEDIA"))
            .alias("severity"),
            "promised_at",
            F.col("delivered_at").alias("detected_at"),
        )
    )

    stopped = (
        latest.filter(
            ~F.col("status").isin(*FINAL_STATUSES)
            & F.col("promised_at").isNotNull()
            & (
                reference
                > F.col("promised_at")
                + F.expr(f"INTERVAL {int(STOPPED_THRESHOLD_MINUTES)} MINUTES")
            )
        )
        .select(
            F.lit("DELIVERY_STOPPED").alias("alert_code"),
            "order_id",
            "zone",
            "status",
            F.round(
                (F.unix_timestamp(reference) - F.unix_timestamp("promised_at")) / 60.0, 2
            ).alias("minutes"),
            F.when(
                (F.unix_timestamp(reference) - F.unix_timestamp("promised_at")) / 60.0
                >= STOPPED_THRESHOLD_MINUTES * 2,
                F.lit("ALTA"),
            )
            .otherwise(F.lit("MEDIA"))
            .alias("severity"),
            "promised_at",
            reference.alias("detected_at"),
        )
    )

    return late.unionByName(stopped)

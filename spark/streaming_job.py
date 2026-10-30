"""
Spark Structured Streaming - Procesamiento de eventos SLA de RapidGo
Consume del topic Kafka 'rapidgo.sla.events' y escribe en las capas Data Lake.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, to_timestamp, expr
from pyspark.sql.types import StructType, StringType, DoubleType, TimestampType

# Esquema JSON del evento SLA
sla_schema = StructType([
    ("event_id", StringType()),
    ("event_type", StringType()),
    ("event_time", StringType()),
    ("order_id", StringType()),
    ("zone", StringType()),
    ("distance_km", DoubleType()),
    ("promised_at", StringType()),
    ("picked_up_at", StringType()),
    ("delivered_at", StringType()),
    ("status", StringType())
])

def create_spark_session():
    """Crea y configura la sesión Spark."""
    spark = SparkSession.builder \
        .appName("RapidGoSLAStreaming") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
        .getOrCreate()
    return spark

def read_kafka_stream(spark, topic, bootstrap_servers):
    """Lee el stream desde Kafka."""
    df = spark.read.format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .option("startingOffsets", "latest") \
        .load()
    
    # Parsear el valor del mensaje JSON
    kafka_value = df.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), sla_schema).alias("data")).select("data.*")
    
    # Convertir timestamps
    kafka_value = kafka_value.withColumn("event_time", to_timestamp(col("event_time")))
    kafka_value = kafka_value.withColumn("promised_at_ts", to_timestamp(col("promised_at")))
    kafka_value = kafka_value.withColumn("picked_up_at_ts", to_timestamp(col("picked_up_at")))
    kafka_value = kafka_value.withColumn("delivered_at_ts", to_timestamp(col("delivered_at")))
    
    return kafka_value

def process_stream(df, spark):
    """Aplica transformaciones y calcular KPIs/SLA."""
    # Ventana de 15 minutos para calcular KPIs
    windowed = df.withWatermark("event_time", "2 minutes") \
        .groupBy(
            window(col("event_time"), "15 minutes"),
            col("order_id"),
            col("zone")
        ) \
        .agg(
            expr("count(*) as event_count"),
            expr("approx_percentile(distance_km, 0.5, 0.01) as median_distance"),
            expr("max(case when status = 'delivered' then 1 else 0 end) as delivered")
        )
    
    # Calcular tiempo de entrega
    delivery_times = df \
        .filter(col("status") == "delivered") \
        .withColumn("delivery_duration", col("delivered_at_ts").cast("long") - col("picked_up_at_ts").cast("long")) \
        .withColumn("delivery_duration_minutes", (col("delivery_duration") / 60))
    
    return windowed, delivery_times

def write_to_data_lake(df, spark, layer, mode="append"):
    """Escribe los datos en la capa correspondiente del Data Lake."""
    base_path = "file:///C:/tmp/equipo_bigdata/Julian/datalake"
    
    if layer == "raw":
        path = f"{base_path}/raw"
    elif layer == "processed":
        path = f"{base_path}/processed"
    elif layer == "curated":
        path = f"{base_path}/curated"
    else:
        path = base_path
    
    df.write.mode(mode).parquet(path)
    print(f"Written to {layer} layer at {path}")

if __name__ == "__main__":
    spark = create_spark_session()
    
    # Leer stream desde Kafka
    kafka_df = read_kafka_stream(spark, "rapidgo.sla.events", "kafka:29092")
    
    # Procesar
    windowed, delivery_times = process_stream(kafka_df, spark)
    
    # Escribir en Data Lake
    write_to_data_lake(windowed, spark, "raw")
    write_to_data_lake(delivery_times, spark, "processed")
    
    # Mostrar resultados (para debugging)
    query = windowed.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
    
    query.awaitTermination(60)  # 60 segundos para testing
    spark.stop()
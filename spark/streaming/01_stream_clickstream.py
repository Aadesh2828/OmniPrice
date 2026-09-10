from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_timestamp,
    when
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType
)


# ============================================================
# OMNIPRICE - CLICKSTREAM STRUCTURED STREAMING
# ============================================================

APP_NAME = "OmniPrice_Clickstream_Streaming"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

KAFKA_TOPIC = "omniprice_clickstream"

OUTPUT_PATH = "/omniprice/streaming/clickstream"

CHECKPOINT_PATH = "/omniprice/checkpoints/clickstream"


# ============================================================
# CREATE SPARK SESSION
# ============================================================

spark = (
    SparkSession.builder
    .appName(APP_NAME)
    .master("local[*]")
    .config(
        "spark.sql.adaptive.enabled",
        "false"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


print("=" * 70)
print("OMNIPRICE CLICKSTREAM STRUCTURED STREAMING")
print("=" * 70)

print("Kafka Broker :", KAFKA_BOOTSTRAP_SERVERS)
print("Kafka Topic  :", KAFKA_TOPIC)
print("Output Path  :", OUTPUT_PATH)
print("=" * 70)


# ============================================================
# CLICKSTREAM JSON SCHEMA
# ============================================================

clickstream_schema = StructType([
    StructField(
        "event_time",
        StringType(),
        True
    ),

    StructField(
        "session_id",
        StringType(),
        True
    ),

    StructField(
        "user_id",
        StringType(),
        True
    ),

    StructField(
        "product_id",
        StringType(),
        True
    ),

    StructField(
        "product_description",
        StringType(),
        True
    ),

    StructField(
        "event_type",
        StringType(),
        True
    ),

    StructField(
        "device",
        StringType(),
        True
    ),

    StructField(
        "country",
        StringType(),
        True
    )
])


# ============================================================
# READ KAFKA STREAM
# ============================================================

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option(
        "subscribe",
        KAFKA_TOPIC
    )
    .option(
        "startingOffsets",
        "latest"
    )
    .option(
        "failOnDataLoss",
        "false"
    )
    .load()
)


# ============================================================
# CONVERT KAFKA VALUE FROM BINARY TO STRING
# ============================================================

json_stream = (
    raw_stream
    .selectExpr(
        "CAST(value AS STRING) AS json_value"
    )
)


# ============================================================
# PARSE JSON
# ============================================================

parsed_stream = (
    json_stream
    .select(
        from_json(
            col("json_value"),
            clickstream_schema
        ).alias("data")
    )
    .select("data.*")
)


# ============================================================
# DATA CLEANING AND TRANSFORMATION
# ============================================================

clean_stream = (
    parsed_stream
    .filter(
        col("product_id").isNotNull()
    )
    .filter(
        col("event_type").isNotNull()
    )
    .filter(
        col("event_time").isNotNull()
    )
    .withColumn(
        "event_timestamp",
        to_timestamp(
            col("event_time")
        )
    )
    .withColumn(
        "processed_at",
        current_timestamp()
    )
)


# ============================================================
# CREATE DEMAND SIGNAL
# ============================================================

demand_stream = (
    clean_stream
    .withColumn(
        "demand_signal",
        when(
            col("event_type") == "purchase",
            5
        )
        .when(
            col("event_type") == "add_to_cart",
            3
        )
        .when(
            col("event_type") == "view",
            1
        )
        .when(
            col("event_type") == "remove_from_cart",
            -1
        )
        .otherwise(0)
    )
)


# ============================================================
# WRITE STREAM TO PARQUET
# ============================================================

query = (
    demand_stream
    .writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        OUTPUT_PATH
    )
    .option(
        "checkpointLocation",
        CHECKPOINT_PATH
    )
    .trigger(
        processingTime="10 seconds"
    )
    .start()
)


print("Clickstream streaming started successfully.")

query.awaitTermination()

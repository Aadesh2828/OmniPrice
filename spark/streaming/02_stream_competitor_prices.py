from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_timestamp
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType
)


# ============================================================
# OMNIPRICE - COMPETITOR PRICE STRUCTURED STREAMING
# ============================================================

APP_NAME = "OmniPrice_Competitor_Price_Streaming"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

KAFKA_TOPIC = "omniprice_competitor_prices"

OUTPUT_PATH = "/omniprice/streaming/competitor_prices"

CHECKPOINT_PATH = "/omniprice/checkpoints/competitor_prices"


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
print("OMNIPRICE COMPETITOR PRICE STRUCTURED STREAMING")
print("=" * 70)

print("Kafka Broker :", KAFKA_BOOTSTRAP_SERVERS)
print("Kafka Topic  :", KAFKA_TOPIC)
print("Output Path  :", OUTPUT_PATH)

print("=" * 70)


# ============================================================
# COMPETITOR PRICE JSON SCHEMA
# ============================================================

competitor_schema = StructType([
    StructField(
        "event_time",
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
        "competitor",
        StringType(),
        True
    ),

    StructField(
        "competitor_price",
        DoubleType(),
        True
    ),

    StructField(
        "availability",
        BooleanType(),
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
# CONVERT KAFKA VALUE TO STRING
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
            competitor_schema
        ).alias("data")
    )
    .select("data.*")
)


# ============================================================
# DATA CLEANING
# ============================================================

clean_stream = (
    parsed_stream
    .filter(
        col("product_id").isNotNull()
    )
    .filter(
        col("competitor_price").isNotNull()
    )
    .filter(
        col("competitor").isNotNull()
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
# WRITE TO PARQUET
# ============================================================

query = (
    clean_stream
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


print("Competitor price streaming started successfully.")

query.awaitTermination()

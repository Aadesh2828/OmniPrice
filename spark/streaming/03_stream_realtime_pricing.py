from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    current_timestamp,
    to_timestamp,
    when,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    BooleanType
)


# ============================================================
# OMNIPRICE - REAL-TIME PRICING STREAMING ENGINE
# ============================================================

APP_NAME = "OmniPrice_Real_Time_Pricing"

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"

CLICKSTREAM_TOPIC = "omniprice_clickstream"

COMPETITOR_TOPIC = "omniprice_competitor_prices"

OUTPUT_PATH = "/omniprice/streaming/real_time_pricing"

CHECKPOINT_PATH = "/omniprice/streaming/checkpoints/real_time_pricing"


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
print("OMNIPRICE REAL-TIME PRICING STREAMING ENGINE")
print("=" * 70)

print("Kafka Broker :", KAFKA_BOOTSTRAP_SERVERS)

print("Clickstream Topic :", CLICKSTREAM_TOPIC)

print("Competitor Topic :", COMPETITOR_TOPIC)

print("=" * 70)


# ============================================================
# CLICKSTREAM SCHEMA
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
# COMPETITOR PRICE SCHEMA
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
# READ CLICKSTREAM KAFKA TOPIC
# ============================================================

clickstream_raw = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option(
        "subscribe",
        CLICKSTREAM_TOPIC
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
# PARSE CLICKSTREAM
# ============================================================

clickstream = (
    clickstream_raw
    .selectExpr(
        "CAST(value AS STRING) AS json_value"
    )
    .select(
        from_json(
            col("json_value"),
            clickstream_schema
        ).alias("data")
    )
    .select("data.*")
    .filter(
        col("product_id").isNotNull()
    )
)


# ============================================================
# GENERATE DEMAND SCORE
# ============================================================

clickstream_signal = (
    clickstream
    .withColumn(
        "demand_score",
        when(
            col("event_type") == "purchase",
            lit(5)
        )
        .when(
            col("event_type") == "add_to_cart",
            lit(3)
        )
        .when(
            col("event_type") == "view",
            lit(1)
        )
        .when(
            col("event_type") == "remove_from_cart",
            lit(-1)
        )
        .otherwise(
            lit(0)
        )
    )
    .select(
        "product_id",
        "demand_score"
    )
)


# ============================================================
# READ COMPETITOR PRICE TOPIC
# ============================================================

competitor_raw = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BOOTSTRAP_SERVERS
    )
    .option(
        "subscribe",
        COMPETITOR_TOPIC
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
# PARSE COMPETITOR PRICES
# ============================================================

competitor_stream = (
    competitor_raw
    .selectExpr(
        "CAST(value AS STRING) AS json_value"
    )
    .select(
        from_json(
            col("json_value"),
            competitor_schema
        ).alias("data")
    )
    .select("data.*")
    .filter(
        col("product_id").isNotNull()
    )
)


# ============================================================
# GENERATE COMPETITOR PRICE SIGNAL
# ============================================================

competitor_signal = (
    competitor_stream
    .select(
        "product_id",
        "competitor",
        "competitor_price",
        "availability"
    )
)


# ============================================================
# COMBINE STREAMS USING STREAM-STREAM JOIN
# ============================================================

combined_stream = (
    clickstream_signal
    .join(
        competitor_signal,
        "product_id"
    )
)


# ============================================================
# DYNAMIC PRICING LOGIC
# ============================================================

pricing_stream = (
    combined_stream
    .withColumn(
        "pricing_action",
        when(
            (
                (col("demand_score") >= 5)
                &
                (col("competitor_price") > 0)
            ),
            lit("INCREASE_PRICE")
        )
        .when(
            (
                (col("demand_score") <= 0)
                &
                (col("competitor_price") > 0)
            ),
            lit("DECREASE_PRICE")
        )
        .otherwise(
            lit("MAINTAIN_PRICE")
        )
    )
    .withColumn(
        "processed_at",
        current_timestamp()
    )
)


# ============================================================
# WRITE REAL-TIME PRICING OUTPUT
# ============================================================

query = (
    pricing_stream
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


print("Real-time pricing engine started successfully.")

query.awaitTermination()

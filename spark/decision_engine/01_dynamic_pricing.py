from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import (
    col,
    avg,
    count,
    lit,
    when,
    coalesce,
    round as spark_round
)


def create_spark_session():

    return (
        SparkSession.builder
        .appName("OmniPrice-Dynamic-Pricing-Engine")
        .master("local[*]")
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy"
        )
        .getOrCreate()
    )


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel("WARN")

    print("=" * 70)
    print("OMNIPRICE DYNAMIC PRICING ENGINE")
    print("=" * 70)

    # ==========================================================
    # HDFS PATHS
    # ==========================================================

    demand_path = (
        "hdfs:///omniprice/processed/"
        "demand_analytics"
    )

    inventory_path = (
        "hdfs:///omniprice/processed/"
        "inventory_analytics"
    )

    clickstream_path = (
        "hdfs:///omniprice/streaming/"
        "clickstream"
    )

    competitor_path = (
        "hdfs:///omniprice/streaming/"
        "competitor_prices"
    )

    output_path = (
        "hdfs:///omniprice/analytics/"
        "pricing_recommendations"
    )

    # ==========================================================
    # 1. READ DEMAND ANALYTICS
    # ==========================================================

    print("\nReading demand analytics...")

    demand_df = (
        spark.read
        .parquet(demand_path)
    )

    print("\nDemand Analytics Schema:")

    demand_df.printSchema()

    # ==========================================================
    # 2. READ INVENTORY ANALYTICS
    # ==========================================================

    print("\nReading inventory analytics...")

    inventory_df = (
        spark.read
        .parquet(inventory_path)
    )

    print("\nInventory Analytics Schema:")

    inventory_df.printSchema()

    # ==========================================================
    # 3. READ CLICKSTREAM
    # ==========================================================

    print("\nReading clickstream data...")

    clickstream_df = None

    try:

        clickstream_df = (
            spark.read
            .parquet(clickstream_path)
        )

        print("\nClickstream Schema:")

        clickstream_df.printSchema()

    except Exception as e:

        print(
            "\nWARNING: Clickstream data "
            "could not be read."
        )

        print(str(e))

    # ==========================================================
    # 4. READ COMPETITOR PRICE STREAM
    # ==========================================================

    print(
        "\nReading competitor price data..."
    )

    competitor_df = None

    try:

        competitor_df = (
            spark.read
            .parquet(competitor_path)
        )

        print(
            "\nCompetitor Price Schema:"
        )

        competitor_df.printSchema()

    except Exception as e:

        print(
            "\nWARNING: Competitor price "
            "data could not be read."
        )

        print(str(e))

    # ==========================================================
    # 5. SELECT DEMAND FEATURES
    # ==========================================================

    demand = (

        demand_df

        .select(

            "product_id",

            "product_name",

            "category",

            "total_quantity_sold",

            "total_orders",

            "unique_customers",

            "total_revenue",

            "average_daily_demand",

            "average_selling_price",

            "demand_level"

        )

    )

    # ==========================================================
    # 6. SELECT INVENTORY FEATURES
    # ==========================================================

    inventory = (

        inventory_df

        .select(

            "product_id",

            "current_stock",

            "reorder_level",

            "lead_time_days",

            "stock_coverage_days",

            "stockout_risk",

            "recommended_reorder_quantity"

        )

    )

    # ==========================================================
    # 7. JOIN DEMAND AND INVENTORY
    # ==========================================================

    pricing_df = (

        demand

        .join(

            inventory,

            on="product_id",

            how="left"

        )

    )

    # ==========================================================
    # 8. PROCESS CLICKSTREAM
    # ==========================================================

    if clickstream_df is not None:

        print(
            "\nProcessing clickstream..."
        )

        clickstream_columns = (
            clickstream_df.columns
        )

        print(
            "Clickstream columns:",
            clickstream_columns
        )

        # Product ID must exist
        # in clickstream data

        if "product_id" in clickstream_columns:

            clickstream_agg = (

                clickstream_df

                .groupBy(
                    "product_id"
                )

                .agg(

                    count("*")
                    .alias(
                        "real_time_click_count"
                    )

                )

            )

            pricing_df = (

                pricing_df

                .join(

                    clickstream_agg,

                    on="product_id",

                    how="left"

                )

            )

        else:

            print(
                "WARNING: product_id not "
                "found in clickstream."
            )

            pricing_df = (

                pricing_df

                .withColumn(

                    "real_time_click_count",

                    lit(0)

                )

            )

    else:

        pricing_df = (

            pricing_df

            .withColumn(

                "real_time_click_count",

                lit(0)

            )

        )

    # ==========================================================
    # 9. HANDLE NULL CLICK COUNTS
    # ==========================================================

    pricing_df = (

        pricing_df

        .withColumn(

            "real_time_click_count",

            coalesce(

                col(
                    "real_time_click_count"
                ),

                lit(0)

            )

        )

    )

    # ==========================================================
    # 10. PROCESS COMPETITOR PRICES
    # ==========================================================

    if competitor_df is not None:

        print(
            "\nProcessing competitor prices..."
        )

        competitor_columns = (
            competitor_df.columns
        )

        print(
            "Competitor columns:",
            competitor_columns
        )

        if (

            "product_id"
            in competitor_columns

            and

            "competitor_price"
            in competitor_columns

        ):

            competitor_agg = (

                competitor_df

                .groupBy(
                    "product_id"
                )

                .agg(

                    avg(
                        "competitor_price"
                    )

                    .alias(
                        "average_competitor_price"
                    )

                )

            )

            pricing_df = (

                pricing_df

                .join(

                    competitor_agg,

                    on="product_id",

                    how="left"

                )

            )

        else:

            print(
                "WARNING: Required competitor "
                "columns not found."
            )

            pricing_df = (

                pricing_df

                .withColumn(

                    "average_competitor_price",

                    lit(None).cast(DoubleType())

                )

            )

    else:

        pricing_df = (

            pricing_df

            .withColumn(

                "average_competitor_price",

                lit(None).cast(DoubleType())

            )

        )

    # ==========================================================
    # 11. CALCULATE DEMAND SCORE
    # ==========================================================

    pricing_df = (

        pricing_df

        .withColumn(

            "demand_score",

            when(

                col("demand_level")
                == "High",

                lit(3)

            )

            .when(

                col("demand_level")
                == "Medium",

                lit(2)

            )

            .otherwise(

                lit(1)

            )

        )

    )

    # ==========================================================
    # 12. REAL-TIME DEMAND SIGNAL
    # ==========================================================

    pricing_df = (

        pricing_df

        .withColumn(

            "real_time_demand_signal",

            when(

                col(
                    "real_time_click_count"
                ) >= 100,

                lit("HIGH")

            )

            .when(

                col(
                    "real_time_click_count"
                ) >= 30,

                lit("MEDIUM")

            )

            .otherwise(

                lit("LOW")

            )

        )

    )

    # ==========================================================
    # 13. PRICING DECISION
    # ==========================================================

    pricing_df = (

        pricing_df

        .withColumn(

            "pricing_action",

            when(

                (

                    (
                        col(
                            "demand_level"
                        ) == "High"
                    )

                    &

                    (
                        col(
                            "stockout_risk"
                        ) == "HIGH"
                    )

                ),

                lit(
                    "INCREASE_PRICE"
                )

            )

            .when(

                (

                    (
                        col(
                            "real_time_demand_signal"
                        ) == "HIGH"
                    )

                    &

                    (
                        col(
                            "current_stock"
                        )
                        <= col(
                            "reorder_level"
                        )
                    )

                ),

                lit(
                    "INCREASE_PRICE"
                )

            )

            .when(

                (

                    (
                        col(
                            "demand_level"
                        ) == "Low"
                    )

                    &

                    (
                        col(
                            "current_stock"
                        ) > 500
                    )

                ),

                lit(
                    "DECREASE_PRICE"
                )

            )

            .when(

                (

                    col(
                        "average_competitor_price"
                    ).isNotNull()

                    &

                    (

                        col(
                            "average_competitor_price"
                        )

                        <

                        col(
                            "average_selling_price"
                        )

                    )

                ),

                lit(
                    "MATCH_COMPETITOR"
                )

            )

            .otherwise(

                lit(
                    "KEEP_PRICE"
                )

            )

        )

    )

    # ==========================================================
    # 14. PRICE ADJUSTMENT
    # ==========================================================

    pricing_df = (

        pricing_df

        .withColumn(

            "price_adjustment_percentage",

            when(

                col(
                    "pricing_action"
                )
                == "INCREASE_PRICE",

                lit(10.0)

            )

            .when(

                col(
                    "pricing_action"
                )
                == "DECREASE_PRICE",

                lit(-10.0)

            )

            .when(

                col(
                    "pricing_action"
                )
                == "MATCH_COMPETITOR",

                lit(-5.0)

            )

            .otherwise(

                lit(0.0)

            )

        )

    )

    # ==========================================================
    # 15. CALCULATE RECOMMENDED PRICE
    # ==========================================================

    pricing_df = (

        pricing_df

        .withColumn(

            "recommended_price",

            spark_round(

                col(
                    "average_selling_price"
                )

                *

                (

                    lit(1)

                    +

                    col(
                        "price_adjustment_percentage"
                    )

                    / lit(100)

                ),

                2

            )

        )

    )

    # ==========================================================
    # 16. FINAL OUTPUT
    # ==========================================================

    final_df = (

        pricing_df

        .select(

            "product_id",

            "product_name",

            "category",

            "average_selling_price",

            "recommended_price",

            "total_quantity_sold",

            "average_daily_demand",

            "demand_level",

            "current_stock",

            "reorder_level",

            "stockout_risk",

            "real_time_click_count",

            "real_time_demand_signal",

            "average_competitor_price",

            "pricing_action",

            "price_adjustment_percentage"

        )

    )

    # ==========================================================
    # 17. DISPLAY RESULTS
    # ==========================================================

    print(
        "\nSample Pricing Recommendations:"
    )

    final_df.show(
        20,
        truncate=False
    )

    # ==========================================================
    # 18. WRITE PARQUET
    # ==========================================================

    print(
        "\nWriting pricing recommendations..."
    )

    (

        final_df

        .write

        .mode("overwrite")

        .option(
            "compression",
            "snappy"
        )

        .parquet(
            output_path
        )

    )

    print("=" * 70)

    print(
        "DYNAMIC PRICING COMPLETED"
    )

    print(
        "Output:",
        output_path
    )

    print("=" * 70)

    spark.stop()


if __name__ == "__main__":

    main()

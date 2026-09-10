from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    lit,
    when,
    coalesce
)


def create_spark_session():

    return (
        SparkSession.builder
        .appName(
            "OmniPrice-Inventory-Optimization"
        )
        .master("local[*]")
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy"
        )
        .getOrCreate()
    )


def main():

    spark = create_spark_session()

    spark.sparkContext.setLogLevel(
        "WARN"
    )

    print("=" * 70)
    print(
        "OMNIPRICE INVENTORY OPTIMIZATION ENGINE"
    )
    print("=" * 70)

    # ==========================================================
    # HDFS PATHS
    # ==========================================================

    inventory_path = (
        "hdfs:///omniprice/processed/"
        "inventory_analytics"
    )

    clickstream_path = (
        "hdfs:///omniprice/streaming/"
        "clickstream"
    )

    output_path = (
        "hdfs:///omniprice/analytics/"
        "inventory_recommendations"
    )

    # ==========================================================
    # 1. READ INVENTORY ANALYTICS
    # ==========================================================

    print(
        "\nReading inventory analytics..."
    )

    inventory_df = (

        spark.read

        .parquet(
            inventory_path
        )

    )

    # ==========================================================
    # 2. READ CLICKSTREAM
    # ==========================================================

    try:

        clickstream_df = (

            spark.read

            .parquet(
                clickstream_path
            )

        )

        clickstream_available = True

    except Exception:

        print(
            "WARNING: Clickstream data "
            "not available."
        )

        clickstream_available = False

    # ==========================================================
    # 3. REAL-TIME CLICKSTREAM DEMAND
    # ==========================================================

    if clickstream_available:

        if "product_id" in clickstream_df.columns:

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

        else:

            clickstream_agg = None

    else:

        clickstream_agg = None

    # ==========================================================
    # 4. BASE INVENTORY DATA
    # ==========================================================

    inventory = (

        inventory_df

        .select(

            "product_id",

            "product_name",

            "category",

            "total_quantity_sold",

            "average_daily_demand",

            "demand_level",

            "initial_stock",

            "current_stock",

            "reorder_level",

            "lead_time_days",

            "stock_coverage_days",

            "stockout_risk",

            "recommended_reorder_quantity"

        )

    )

    # ==========================================================
    # 5. JOIN REAL-TIME DEMAND
    # ==========================================================

    if clickstream_agg is not None:

        inventory = (

            inventory

            .join(

                clickstream_agg,

                on="product_id",

                how="left"

            )

        )

    else:

        inventory = (

            inventory

            .withColumn(

                "real_time_click_count",

                lit(0)

            )

        )

    # ==========================================================
    # 6. HANDLE NULL
    # ==========================================================

    inventory = (

        inventory

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
    # 7. REAL-TIME DEMAND STATUS
    # ==========================================================

    inventory = (

        inventory

        .withColumn(

            "real_time_demand_status",

            when(

                col(
                    "real_time_click_count"
                ) >= 100,

                lit(
                    "HIGH"
                )

            )

            .when(

                col(
                    "real_time_click_count"
                ) >= 30,

                lit(
                    "MEDIUM"
                )

            )

            .otherwise(

                lit(
                    "LOW"
                )

            )

        )

    )

    # ==========================================================
    # 8. INVENTORY ACTION
    # ==========================================================

    inventory = (

        inventory

        .withColumn(

            "inventory_action",

            when(

                (

                    (
                        col(
                            "current_stock"
                        )
                        <= col(
                            "reorder_level"
                        )
                    )

                    &

                    (
                        col(
                            "real_time_demand_status"
                        )
                        == "HIGH"
                    )

                ),

                lit(
                    "URGENT_RESTOCK"
                )

            )

            .when(

                col(
                    "stockout_risk"
                )
                == "HIGH",

                lit(
                    "URGENT_RESTOCK"
                )

            )

            .when(

                col(
                    "stockout_risk"
                )
                == "MEDIUM",

                lit(
                    "RESTOCK_SOON"
                )

            )

            .when(

                col(
                    "current_stock"
                ) > 500,

                lit(
                    "OVERSTOCK"
                )

            )

            .otherwise(

                lit(
                    "NORMAL"
                )

            )

        )

    )

    # ==========================================================
    # 9. FINAL REORDER RECOMMENDATION
    # ==========================================================

    inventory = (

        inventory

        .withColumn(

            "final_reorder_quantity",

            when(

                col(
                    "inventory_action"
                )
                == "URGENT_RESTOCK",

                col(
                    "recommended_reorder_quantity"
                )

            )

            .when(

                col(
                    "inventory_action"
                )
                == "RESTOCK_SOON",

                col(
                    "recommended_reorder_quantity"
                )

            )

            .otherwise(

                lit(0)

            )

        )

    )

    # ==========================================================
    # 10. FINAL OUTPUT
    # ==========================================================

    final_df = (

        inventory

        .select(

            "product_id",

            "product_name",

            "category",

            "current_stock",

            "reorder_level",

            "lead_time_days",

            "stock_coverage_days",

            "stockout_risk",

            "average_daily_demand",

            "demand_level",

            "real_time_click_count",

            "real_time_demand_status",

            "inventory_action",

            "recommended_reorder_quantity",

            "final_reorder_quantity"

        )

    )

    # ==========================================================
    # 11. DISPLAY
    # ==========================================================

    print(
        "\nInventory Recommendations:"
    )

    final_df.show(
        20,
        truncate=False
    )

    # ==========================================================
    # 12. WRITE HDFS
    # ==========================================================

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
        "INVENTORY OPTIMIZATION COMPLETED"
    )

    print(
        "Output:",
        output_path
    )

    print("=" * 70)

    spark.stop()


if __name__ == "__main__":

    main()

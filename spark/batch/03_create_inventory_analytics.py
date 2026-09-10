import sys
import os


# ============================================================
# Add OmniPrice project root to Python path
# ============================================================

PROJECT_ROOT = os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../.."
    )
)

if PROJECT_ROOT not in sys.path:
    sys.path.insert(
        0,
        PROJECT_ROOT
    )


# ============================================================
# PySpark Imports
# ============================================================

from pyspark.sql import functions as F


# ============================================================
# OmniPrice Utility Import
# ============================================================

from spark.utils.spark_session import get_spark_session


# ============================================================
# OmniPrice - Phase 4
# Job 3: Inventory Analytics
# ============================================================


APP_NAME = "OmniPrice_Inventory_Analytics"


DEMAND_PATH = (
    "hdfs:///omniprice/processed/"
    "demand_analytics"
)


INVENTORY_PATH = (
    "hdfs:///omniprice/raw/"
    "inventory/inventory.csv"
)


INVENTORY_OUTPUT = (
    "hdfs:///omniprice/processed/"
    "inventory_analytics"
)


def main():

    spark = get_spark_session(
        APP_NAME
    )

    print("=" * 70)
    print("OMNIPRICE - INVENTORY ANALYTICS")
    print("=" * 70)


    # ========================================================
    # 1. Read Demand Analytics
    # ========================================================

    print(
        "\nReading Demand Analytics..."
    )

    demand = (
        spark.read
        .parquet(
            DEMAND_PATH
        )
    )


    # ========================================================
    # 2. Read Inventory
    # ========================================================

    print(
        "\nReading Inventory..."
    )

    inventory = (

        spark.read

        .option(
            "header",
            True
        )

        .option(
            "inferSchema",
            True
        )

        .csv(
            INVENTORY_PATH
        )
    )


    # ========================================================
    # 3. Standardize Inventory
    # ========================================================

    inventory = (

        inventory

        .select(

            F.col(
                "product_id"
            )
            .cast(
                "string"
            )
            .alias(
                "product_id"
            ),

            F.col(
                "initial_stock"
            )
            .cast(
                "integer"
            ),

            F.col(
                "current_stock"
            )
            .cast(
                "integer"
            ),

            F.col(
                "reorder_level"
            )
            .cast(
                "integer"
            ),

            F.col(
                "lead_time_days"
            )
            .cast(
                "integer"
            )
        )
    )


    # ========================================================
    # 4. Join Demand + Inventory
    # ========================================================

    print(
        "\nJoining demand and inventory..."
    )


    inventory_analytics = (

        demand.alias(
            "d"
        )

        .join(

            inventory.alias(
                "i"
            ),

            F.col(
                "d.product_id"
            )
            ==
            F.col(
                "i.product_id"
            ),

            "left"
        )

        .select(

            F.col(
                "d.product_id"
            ),

            F.col(
                "d.product_name"
            ),

            F.col(
                "d.category"
            ),

            F.col(
                "d.total_quantity_sold"
            ),

            F.col(
                "d.average_daily_demand"
            ),

            F.col(
                "d.demand_level"
            ),

            F.col(
                "i.initial_stock"
            ),

            F.col(
                "i.current_stock"
            ),

            F.col(
                "i.reorder_level"
            ),

            F.col(
                "i.lead_time_days"
            )
        )
    )


    # ========================================================
    # 5. Calculate Stock Coverage
    # ========================================================

    inventory_analytics = (

        inventory_analytics

        .withColumn(

            "stock_coverage_days",

            F.when(

                F.col(
                    "average_daily_demand"
                ) > 0,

                F.col(
                    "current_stock"
                )
                /
                F.col(
                    "average_daily_demand"
                )

            )

            .otherwise(
                F.lit(None)
            )
        )
    )


    # ========================================================
    # 6. Calculate Stockout Risk
    # ========================================================

    inventory_analytics = (

        inventory_analytics

        .withColumn(

            "stockout_risk",

            F.when(

                F.col(
                    "current_stock"
                ) <= 0,

                "STOCKOUT"

            )

            .when(

                F.col(
                    "stock_coverage_days"
                )
                <=
                F.col(
                    "lead_time_days"
                ),

                "HIGH"

            )

            .when(

                F.col(
                    "current_stock"
                )
                <=
                F.col(
                    "reorder_level"
                ),

                "MEDIUM"

            )

            .otherwise(
                "LOW"
            )
        )
    )


    # ========================================================
    # 7. Recommended Reorder Quantity
    # ========================================================

    inventory_analytics = (

        inventory_analytics

        .withColumn(

            "recommended_reorder_quantity",

            F.when(

                F.col(
                    "current_stock"
                )
                <
                F.col(
                    "reorder_level"
                ),

                F.greatest(

                    F.col(
                        "reorder_level"
                    )
                    -
                    F.col(
                        "current_stock"
                    ),

                    F.lit(0)
                )

            )

            .otherwise(
                F.lit(0)
            )
        )
    )


    # ========================================================
    # 8. Round Metrics
    # ========================================================

    inventory_analytics = (

        inventory_analytics

        .withColumn(

            "stock_coverage_days",

            F.round(
                "stock_coverage_days",
                2
            )
        )

        .withColumn(

            "average_daily_demand",

            F.round(
                "average_daily_demand",
                2
            )
        )
    )


    # ========================================================
    # 9. Write Parquet
    # ========================================================

    print(
        "\nWriting Inventory Analytics..."
    )


    (
        inventory_analytics

        .write

        .mode(
            "overwrite"
        )

        .option(
            "compression",
            "snappy"
        )

        .parquet(
            INVENTORY_OUTPUT
        )
    )


    # ========================================================
    # 10. Display Results
    # ========================================================

    print(
        "\nInventory Analytics Sample:"
    )

    (
        inventory_analytics

        .orderBy(
            F.asc(
                "stock_coverage_days"
            )
        )

        .show(
            20,
            truncate=False
        )
    )


    print(
        "\nInventory Analytics Schema:"
    )

    inventory_analytics.printSchema()


    print(
        "\nInventory Analytics completed successfully."
    )


    spark.stop()


if __name__ == "__main__":

    main()

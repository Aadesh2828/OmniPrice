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
# Job 2: Demand Analytics
# ============================================================


APP_NAME = "OmniPrice_Demand_Analytics"


MASTER_SALES_PATH = (
    "hdfs:///omniprice/processed/"
    "master_sales"
)


DEMAND_OUTPUT = (
    "hdfs:///omniprice/processed/"
    "demand_analytics"
)


def main():

    spark = get_spark_session(
        APP_NAME
    )

    print("=" * 70)
    print("OMNIPRICE - DEMAND ANALYTICS")
    print("=" * 70)


    # ========================================================
    # 1. Read Master Sales
    # ========================================================

    print("\nReading Master Sales...")

    master_sales = (
        spark.read
        .parquet(
            MASTER_SALES_PATH
        )
    )


    # ========================================================
    # 2. Product-Level Demand
    # ========================================================

    print(
        "\nCalculating product-level demand..."
    )


    demand_analytics = (

        master_sales

        .groupBy(
            "product_id",
            "product_name",
            "category"
        )

        .agg(

            F.sum(
                "quantity"
            )
            .alias(
                "total_quantity_sold"
            ),

            F.countDistinct(
                "invoice_no"
            )
            .alias(
                "total_orders"
            ),

            F.countDistinct(
                "customer_id"
            )
            .alias(
                "unique_customers"
            ),

            F.sum(
                "revenue"
            )
            .alias(
                "total_revenue"
            ),

            F.avg(
                "quantity"
            )
            .alias(
                "average_quantity_per_transaction"
            ),

            F.avg(
                "unit_price"
            )
            .alias(
                "average_selling_price"
            ),

            F.min(
                "sale_date"
            )
            .alias(
                "first_sale_date"
            ),

            F.max(
                "sale_date"
            )
            .alias(
                "last_sale_date"
            )
        )

        .withColumn(
            "total_revenue",
            F.round(
                "total_revenue",
                2
            )
        )

        .withColumn(
            "average_quantity_per_transaction",
            F.round(
                "average_quantity_per_transaction",
                2
            )
        )

        .withColumn(
            "average_selling_price",
            F.round(
                "average_selling_price",
                2
            )
        )
    )


    # ========================================================
    # 3. Calculate Active Days
    # ========================================================

    demand_analytics = (

        demand_analytics

        .withColumn(
            "active_days",

            F.datediff(
                F.col(
                    "last_sale_date"
                ),

                F.col(
                    "first_sale_date"
                )
            ) + 1
        )
    )


    # ========================================================
    # 4. Average Daily Demand
    # ========================================================

    demand_analytics = (

        demand_analytics

        .withColumn(

            "average_daily_demand",

            F.when(

                F.col(
                    "active_days"
                ) > 0,

                F.col(
                    "total_quantity_sold"
                )
                /
                F.col(
                    "active_days"
                )

            )

            .otherwise(
                F.lit(0)
            )
        )
    )


    # ========================================================
    # 5. Demand Classification
    # ========================================================

    demand_analytics = (

        demand_analytics

        .withColumn(

            "demand_level",

            F.when(

                F.col(
                    "average_daily_demand"
                ) >= 10,

                "HIGH"

            )

            .when(

                F.col(
                    "average_daily_demand"
                ) >= 3,

                "MEDIUM"

            )

            .otherwise(
                "LOW"
            )
        )
    )


    # ========================================================
    # 6. Write to Parquet
    # ========================================================

    print(
        "\nWriting Demand Analytics..."
    )


    (
        demand_analytics

        .write

        .mode(
            "overwrite"
        )

        .option(
            "compression",
            "snappy"
        )

        .parquet(
            DEMAND_OUTPUT
        )
    )


    # ========================================================
    # 7. Display Results
    # ========================================================

    print(
        "\nDemand Analytics Sample:"
    )

    (
        demand_analytics
        .orderBy(
            F.desc(
                "total_quantity_sold"
            )
        )
        .show(
            10,
            truncate=False
        )
    )


    print(
        "\nDemand Analytics Schema:"
    )

    demand_analytics.printSchema()


    print(
        "\nDemand Analytics completed successfully."
    )


    spark.stop()


if __name__ == "__main__":

    main()

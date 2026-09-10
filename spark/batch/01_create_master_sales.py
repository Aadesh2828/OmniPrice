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

from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    DoubleType,
    TimestampType
)


# ============================================================
# OmniPrice Utility Import
# ============================================================

from spark.utils.spark_session import get_spark_session

# ============================================================
# OmniPrice - Phase 4
# Job 1: Create Master Sales Dataset
# ============================================================


APP_NAME = "OmniPrice_Master_Sales_ETL"


# ------------------------------------------------------------
# HDFS Input Paths
# ------------------------------------------------------------

TRANSACTIONS_PATH = (
    "hdfs:///omniprice/raw/"
    "transactions/transactions.csv"
)

PRODUCT_CATALOG_PATH = (
    "hdfs:///omniprice/raw/"
    "product_catalog/product_catalog.csv"
)

INVENTORY_PATH = (
    "hdfs:///omniprice/raw/"
    "inventory/inventory.csv"
)


# ------------------------------------------------------------
# HDFS Output Path
# ------------------------------------------------------------

MASTER_SALES_OUTPUT = (
    "hdfs:///omniprice/processed/"
    "master_sales"
)


# ============================================================
# Main
# ============================================================

def main():

    spark = get_spark_session(
        APP_NAME
    )

    print("=" * 70)
    print("OMNIPRICE - MASTER SALES ETL")
    print("=" * 70)


    # ========================================================
    # 1. Define Transaction Schema
    # ========================================================

    transaction_schema = StructType([

        StructField(
            "invoice_no",
            StringType(),
            True
        ),

        StructField(
            "product_id",
            StringType(),
            True
        ),

        StructField(
            "description",
            StringType(),
            True
        ),

        StructField(
            "quantity",
            IntegerType(),
            True
        ),

        StructField(
            "invoice_date",
            TimestampType(),
            True
        ),

        StructField(
            "unit_price",
            DoubleType(),
            True
        ),

        StructField(
            "customer_id",
            StringType(),
            True
        ),

        StructField(
            "country",
            StringType(),
            True
        ),

        StructField(
            "revenue",
            DoubleType(),
            True
        )
    ])


    # ========================================================
    # 2. Read Transactions
    # ========================================================

    print("\nReading transactions...")

    transactions = (
        spark.read
        .option("header", True)
        .schema(transaction_schema)
        .csv(TRANSACTIONS_PATH)
    )

    print(
        "Transaction rows:",
        transactions.count()
    )


    # ========================================================
    # 3. Read Product Catalog
    # ========================================================

    print("\nReading product catalog...")

    product_catalog = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(PRODUCT_CATALOG_PATH)
    )

    print(
        "Product catalog rows:",
        product_catalog.count()
    )


    # ========================================================
    # 4. Read Inventory
    # ========================================================

    print("\nReading inventory...")

    inventory = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(INVENTORY_PATH)
    )

    print(
        "Inventory rows:",
        inventory.count()
    )


    # ========================================================
    # 5. Clean Transactions
    # ========================================================

    print("\nCleaning transaction data...")


    transactions = (
        transactions

        # Remove invalid product IDs
        .filter(
            F.col("product_id").isNotNull()
        )

        # Remove invalid quantities
        .filter(
            F.col("quantity") > 0
        )

        # Remove invalid prices
        .filter(
            F.col("unit_price") > 0
        )

        # Remove invalid revenue
        .filter(
            F.col("revenue") > 0
        )

        # Remove missing dates
        .filter(
            F.col("invoice_date").isNotNull()
        )
    )


    # ========================================================
    # 6. Standardize Product Catalog
    # ========================================================

    product_catalog = (
        product_catalog

        .select(
            F.col("product_id")
            .cast("string")
            .alias("product_id"),

            F.col("product_name")
            .cast("string")
            .alias("product_name"),

            F.col("category")
            .cast("string")
            .alias("category"),

            F.col("selling_price")
            .cast("double")
            .alias("catalog_selling_price")
        )

        .dropDuplicates(
            ["product_id"]
        )
    )


    # ========================================================
    # 7. Standardize Inventory
    # ========================================================

    inventory = (
        inventory

        .select(
            F.col("product_id")
            .cast("string")
            .alias("product_id"),

            F.col("initial_stock")
            .cast("integer")
            .alias("initial_stock"),

            F.col("current_stock")
            .cast("integer")
            .alias("current_stock"),

            F.col("reorder_level")
            .cast("integer")
            .alias("reorder_level"),

            F.col("lead_time_days")
            .cast("integer")
            .alias("lead_time_days")
        )

        .dropDuplicates(
            ["product_id"]
        )
    )


    # ========================================================
    # 8. Join Transactions + Product Catalog
    # ========================================================

    print("\nJoining transactions with product catalog...")


    master_sales = (
        transactions.alias("t")

        .join(
            product_catalog.alias("p"),
            F.col("t.product_id")
            == F.col("p.product_id"),
            "left"
        )

        .select(

            F.col("t.invoice_no"),

            F.col("t.product_id"),

            F.coalesce(
                F.col("p.product_name"),
                F.col("t.description")
            ).alias(
                "product_name"
            ),

            F.coalesce(
                F.col("p.category"),
                F.lit("OTHER")
            ).alias(
                "category"
            ),

            F.col("t.quantity"),

            F.col("t.invoice_date"),

            F.to_date(
                F.col("t.invoice_date")
            ).alias(
                "sale_date"
            ),

            F.col("t.unit_price"),

            F.col("p.catalog_selling_price"),

            F.col("t.customer_id"),

            F.col("t.country"),

            F.col("t.revenue")
        )
    )


    # ========================================================
    # 9. Add Derived Columns
    # ========================================================

    master_sales = (
        master_sales

        .withColumn(
            "year",
            F.year(
                F.col("invoice_date")
            )
        )

        .withColumn(
            "month",
            F.month(
                F.col("invoice_date")
            )
        )

        .withColumn(
            "day",
            F.dayofmonth(
                F.col("invoice_date")
            )
        )

        .withColumn(
            "day_of_week",
            F.dayofweek(
                F.col("invoice_date")
            )
        )

        .withColumn(
            "revenue",
            F.round(
                F.col("revenue"),
                2
            )
        )
    )


    # ========================================================
    # 10. Data Quality Checks
    # ========================================================

    print("\nRunning data quality checks...")


    invalid_quantity = (
        master_sales
        .filter(
            F.col("quantity") <= 0
        )
        .count()
    )


    invalid_revenue = (
        master_sales
        .filter(
            F.col("revenue") <= 0
        )
        .count()
    )


    print(
        "Invalid quantity records:",
        invalid_quantity
    )

    print(
        "Invalid revenue records:",
        invalid_revenue
    )


    # ========================================================
    # 11. Write Master Sales to Parquet
    # ========================================================

    print("\nWriting Master Sales dataset...")


    (
        master_sales

        .repartition(
            "year",
            "month"
        )

        .write

        .mode(
            "overwrite"
        )

        .partitionBy(
            "year",
            "month"
        )

        .option(
            "compression",
            "snappy"
        )

        .parquet(
            MASTER_SALES_OUTPUT
        )
    )


    print(
        "\nMaster Sales written to:"
    )

    print(
        MASTER_SALES_OUTPUT
    )


    # ========================================================
    # 12. Display Sample
    # ========================================================

    print("\nMaster Sales Sample:")

    (
        master_sales
        .show(
            10,
            truncate=False
        )
    )


    print("\nMaster Sales Schema:")

    master_sales.printSchema()


    print("\nMaster Sales ETL completed successfully.")


    spark.stop()


if __name__ == "__main__":

    main()

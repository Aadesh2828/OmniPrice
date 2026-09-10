#!/bin/bash
source ~/unset_jupyter.sh

# ============================================================
# OmniPrice - Phase 4
# PySpark Batch ETL Pipeline
# ============================================================

set -e


PROJECT_HOME="$HOME/omniprice"


echo ""
echo "============================================================"
echo "        OMNIPRICE PYSPARK BATCH ETL PIPELINE"
echo "============================================================"
echo ""


# ------------------------------------------------------------
# Step 1: Check HDFS
# ------------------------------------------------------------

echo "[1/5] Checking HDFS..."

if ! hdfs dfs -ls / > /dev/null 2>&1; then

    echo "ERROR: HDFS is not available."

    exit 1

fi

echo "HDFS is available."


# ------------------------------------------------------------
# Step 2: Check raw data
# ------------------------------------------------------------

echo ""
echo "[2/5] Checking raw HDFS data..."


hdfs dfs -test -e \
    /omniprice/raw/transactions/transactions.csv


hdfs dfs -test -e \
    /omniprice/raw/product_catalog/product_catalog.csv


hdfs dfs -test -e \
    /omniprice/raw/inventory/inventory.csv


echo "Raw datasets found."


# ------------------------------------------------------------
# Step 3: Create Master Sales
# ------------------------------------------------------------

echo ""
echo "[3/5] Running Master Sales ETL..."
echo ""


spark-submit \
    --master local[*] \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.parquet.compression.codec=snappy \
    "$PROJECT_HOME/spark/batch/01_create_master_sales.py"


echo ""
echo "Master Sales ETL completed."


# ------------------------------------------------------------
# Step 4: Create Demand Analytics
# ------------------------------------------------------------

echo ""
echo "[4/5] Running Demand Analytics..."
echo ""


spark-submit \
    --master local[*] \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.parquet.compression.codec=snappy \
    "$PROJECT_HOME/spark/batch/02_create_demand_analytics.py"


echo ""
echo "Demand Analytics completed."


# ------------------------------------------------------------
# Step 5: Create Inventory Analytics
# ------------------------------------------------------------

echo ""
echo "[5/5] Running Inventory Analytics..."
echo ""


spark-submit \
    --master local[*] \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.parquet.compression.codec=snappy \
    "$PROJECT_HOME/spark/batch/03_create_inventory_analytics.py"


echo ""
echo "Inventory Analytics completed."


echo ""
echo "============================================================"
echo "       OMNIPRICE BATCH ETL PIPELINE COMPLETED"
echo "============================================================"
echo ""

echo "Processed datasets created:"
echo ""

echo "1. Master Sales"
echo "   /omniprice/processed/master_sales"

echo ""

echo "2. Demand Analytics"
echo "   /omniprice/processed/demand_analytics"

echo ""

echo "3. Inventory Analytics"
echo "   /omniprice/processed/inventory_analytics"

echo ""

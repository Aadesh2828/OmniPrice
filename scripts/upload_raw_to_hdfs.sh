#!/bin/bash

# ============================================================
# OmniPrice - Phase 3
# Upload Prepared Data to HDFS Raw Layer
# ============================================================

set -e

PROJECT_HOME="$HOME/omniprice"

SOURCE_DIR="$PROJECT_HOME/data/prepared"

HDFS_ROOT="/omniprice/raw"


echo "=============================================="
echo "OmniPrice HDFS Raw Data Ingestion"
echo "=============================================="


# ------------------------------------------------------------
# 1. Check source files
# ------------------------------------------------------------

echo ""
echo "Checking source files..."


if [ ! -f "$SOURCE_DIR/transactions.csv" ]; then

    echo "ERROR: transactions.csv not found."

    exit 1

fi


if [ ! -f "$SOURCE_DIR/product_catalog.csv" ]; then

    echo "ERROR: product_catalog.csv not found."

    exit 1

fi


if [ ! -f "$SOURCE_DIR/inventory.csv" ]; then

    echo "ERROR: inventory.csv not found."

    exit 1

fi


echo "All source files found."


# ------------------------------------------------------------
# 2. Create HDFS directories
# ------------------------------------------------------------

echo ""
echo "Creating HDFS directories..."


hdfs dfs -mkdir -p \
    "$HDFS_ROOT/transactions"


hdfs dfs -mkdir -p \
    "$HDFS_ROOT/product_catalog"


hdfs dfs -mkdir -p \
    "$HDFS_ROOT/inventory"


# ------------------------------------------------------------
# 3. Upload Transactions
# ------------------------------------------------------------

echo ""
echo "Uploading transactions.csv..."


hdfs dfs -put -f \
    "$SOURCE_DIR/transactions.csv" \
    "$HDFS_ROOT/transactions/"


# ------------------------------------------------------------
# 4. Upload Product Catalog
# ------------------------------------------------------------

echo ""
echo "Uploading product_catalog.csv..."


hdfs dfs -put -f \
    "$SOURCE_DIR/product_catalog.csv" \
    "$HDFS_ROOT/product_catalog/"


# ------------------------------------------------------------
# 5. Upload Inventory
# ------------------------------------------------------------

echo ""
echo "Uploading inventory.csv..."


hdfs dfs -put -f \
    "$SOURCE_DIR/inventory.csv" \
    "$HDFS_ROOT/inventory/"


# ------------------------------------------------------------
# 6. Display HDFS structure
# ------------------------------------------------------------

echo ""
echo "HDFS Raw Data Structure:"
echo ""


hdfs dfs -ls -R "$HDFS_ROOT"


echo ""
echo "=============================================="
echo "HDFS RAW DATA INGESTION COMPLETED"
echo "=============================================="

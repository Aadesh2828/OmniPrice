#!/bin/bash

# ============================================================
# OmniPrice - HDFS Directory Structure Initialization
# ============================================================
#
# Purpose:
#   Creates the complete OmniPrice HDFS directory structure.
#
# Usage:
#   ./scripts/create_hdfs_structure.sh
#
# This script is safe to run multiple times because
# "mkdir -p" does not fail if directories already exist.
#
# ============================================================

set -e


# ------------------------------------------------------------
# Configuration
# ------------------------------------------------------------

HDFS_ROOT="/omniprice"


echo ""
echo "============================================================"
echo "       OMNIPRICE HDFS DIRECTORY INITIALIZATION"
echo "============================================================"
echo ""


# ------------------------------------------------------------
# Step 1: Check HDFS availability
# ------------------------------------------------------------

echo "[1/4] Checking HDFS availability..."

if ! hdfs dfs -ls / > /dev/null 2>&1; then

    echo ""
    echo "ERROR: HDFS is not available."
    echo ""
    echo "Please start Hadoop/HDFS first."
    echo ""
    echo "Then run this script again:"
    echo ""
    echo "    ./scripts/create_hdfs_structure.sh"
    echo ""

    exit 1

fi


echo "HDFS is available."
echo ""


# ------------------------------------------------------------
# Step 2: Create OmniPrice root directory
# ------------------------------------------------------------

echo "[2/4] Creating OmniPrice HDFS root..."

hdfs dfs -mkdir -p "$HDFS_ROOT"

echo "Created/verified:"
echo "    $HDFS_ROOT"
echo ""


# ------------------------------------------------------------
# Step 3: Create complete directory structure
# ------------------------------------------------------------

echo "[3/4] Creating OmniPrice directory structure..."

# ============================================================
# RAW DATA ZONE
# ============================================================

hdfs dfs -mkdir -p \
    "$HDFS_ROOT/raw/transactions"

hdfs dfs -mkdir -p \
    "$HDFS_ROOT/raw/product_catalog"

hdfs dfs -mkdir -p \
    "$HDFS_ROOT/raw/inventory"


# ============================================================
# PROCESSED DATA ZONE
# ============================================================

hdfs dfs -mkdir -p \
    "$HDFS_ROOT/processed/master_sales"

hdfs dfs -mkdir -p \
    "$HDFS_ROOT/processed/demand_analytics"

hdfs dfs -mkdir -p \
    "$HDFS_ROOT/processed/inventory_analytics"


# ============================================================
# STREAMING DATA ZONE
# ============================================================

hdfs dfs -mkdir -p \
    "$HDFS_ROOT/streaming/clickstream"

hdfs dfs -mkdir -p \
    "$HDFS_ROOT/streaming/competitor_prices"

# ============================================================
# CHECKPOINTS DATA ZONE
# ============================================================

hdfs dfs -mkdir -p \
    "$HDFS_ROOT/checkpoints/clickstream"

hdfs dfs -mkdir -p \
    "$HDFS_ROOT/checkpoints/competitor_prices"


# ============================================================
# ANALYTICS DATA ZONE
# ============================================================

hdfs dfs -mkdir -p \
    "$HDFS_ROOT/analytics/pricing_recommendations"

hdfs dfs -mkdir -p \
    "$HDFS_ROOT/analytics/inventory_recommendations"


echo "Directory structure created successfully."
echo ""


# ------------------------------------------------------------
# Step 4: Display final structure
# ------------------------------------------------------------

echo "[4/4] Verifying HDFS directory structure..."
echo ""

hdfs dfs -ls -R "$HDFS_ROOT"


echo ""
echo "============================================================"
echo "       OMNIPRICE HDFS INITIALIZATION COMPLETE"
echo "============================================================"
echo ""

echo "HDFS Root:"
echo "    $HDFS_ROOT"
echo ""

echo "Raw Data:"
echo "    $HDFS_ROOT/raw/transactions"
echo "    $HDFS_ROOT/raw/product_catalog"
echo "    $HDFS_ROOT/raw/inventory"
echo ""

echo "Processed Data:"
echo "    $HDFS_ROOT/processed/master_sales"
echo "    $HDFS_ROOT/processed/demand_analytics"
echo "    $HDFS_ROOT/processed/inventory_analytics"
echo ""

echo "Streaming Data:"
echo "    $HDFS_ROOT/streaming/clickstream"
echo "    $HDFS_ROOT/streaming/competitor_prices"
echo ""

echo "Checkpoints Data:"
echo "    $HDFS_ROOT/checkpoints/clickstream"
echo "    $HDFS_ROOT/checkpoints/competitor_prices"
echo ""

echo "Analytics Data:"
echo "    $HDFS_ROOT/analytics/pricing_recommendations"
echo "    $HDFS_ROOT/analytics/inventory_recommendations"
echo ""

echo "You can now upload raw data or run PySpark jobs."
echo ""

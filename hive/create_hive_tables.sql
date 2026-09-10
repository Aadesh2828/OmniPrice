CREATE DATABASE IF NOT EXISTS omniprice;

USE omniprice;

-- ============================================================
-- 1. MASTER ANALYTICS
-- ============================================================

DROP TABLE IF EXISTS master_sales;

CREATE EXTERNAL TABLE master_sales
(
    invoice_no STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    quantity INT,
    invoice_date TIMESTAMP,
    sale_date DATE,
    unit_price DOUBLE,
    catalog_selling_price DOUBLE,
    customer_id STRING,
    country STRING,
    revenue DOUBLE,
    day INT,
    day_of_week INT
)
PARTITIONED BY
(
    year INT,
    month INT
)
STORED AS PARQUET
LOCATION '/omniprice/processed/master_sales';

-- ============================================================
-- 2. DEMAND ANALYTICS
-- ============================================================

DROP TABLE IF EXISTS demand_analytics;

CREATE EXTERNAL TABLE demand_analytics
(
    product_id STRING,
    product_name STRING,
    category STRING,
    total_quantity_sold BIGINT,
    total_orders BIGINT,
    unique_customers BIGINT,
    total_revenue DOUBLE,
    average_quantity_per_transaction DOUBLE,
    average_selling_price DOUBLE,
    first_sale_date DATE,
    last_sale_date DATE,
    active_days INT,
    average_daily_demand DOUBLE,
    demand_level STRING
)
STORED AS PARQUET
LOCATION '/omniprice/processed/demand_analytics';


-- ============================================================
-- 3. INVENTORY ANALYTICS
-- ============================================================

DROP TABLE IF EXISTS inventory_analytics;

CREATE EXTERNAL TABLE inventory_analytics
(
    product_id STRING,
    product_name STRING,
    category STRING,
    total_quantity_sold BIGINT,
    average_daily_demand DOUBLE,
    demand_level STRING,
    initial_stock INT,
    current_stock INT,
    reorder_level INT,
    lead_time_days INT,
    stock_coverage_days DOUBLE,
    stockout_risk STRING,
    recommended_reorder_quantity INT
)
STORED AS PARQUET
LOCATION '/omniprice/processed/inventory_analytics';


-- ============================================================
-- 4. PRICING RECOMMENDATIONS
-- ============================================================

DROP TABLE IF EXISTS pricing_recommendations;

CREATE EXTERNAL TABLE pricing_recommendations
(
    product_id STRING,
    product_name STRING,
    category STRING,
    average_selling_price DOUBLE,
    recommended_price DOUBLE,
    total_quantity_sold BIGINT,
    average_daily_demand DOUBLE,
    demand_level STRING,
    current_stock INT,
    reorder_level INT,
    stockout_risk STRING,
    real_time_click_count BIGINT,
    real_time_demand_signal STRING,
    average_competitor_price DOUBLE,
    pricing_action STRING,
    price_adjustment_percentage DOUBLE
)
STORED AS PARQUET
LOCATION '/omniprice/analytics/pricing_recommendations';


-- ============================================================
-- 5. INVENTORY RECOMMENDATIONS
-- ============================================================

DROP TABLE IF EXISTS inventory_recommendations;

CREATE EXTERNAL TABLE inventory_recommendations
(
    product_id STRING,
    product_name STRING,
    category STRING,
    current_stock INT,
    reorder_level INT,
    lead_time_days INT,
    stock_coverage_days DOUBLE,
    stockout_risk STRING,
    average_daily_demand DOUBLE,
    demand_level STRING,
    real_time_click_count BIGINT,
    real_time_demand_status STRING,
    inventory_action STRING,
    recommended_reorder_quantity INT,
    final_reorder_quantity INT
)
STORED AS PARQUET
LOCATION '/omniprice/analytics/inventory_recommendations';

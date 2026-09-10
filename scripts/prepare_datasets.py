import os
import numpy as np
import pandas as pd


# ============================================================
# OmniPrice - Phase 2
# Online Retail Dataset Preparation
# ============================================================

SOURCE_FILE = "/home/talentum/omniprice/data/source/OnlineRetail.xlsx"

TRANSACTIONS_OUTPUT = "/home/talentum/omniprice/data/prepared/transactions.csv"
PRODUCT_CATALOG_OUTPUT = "/home/talentum/omniprice/data/prepared/product_catalog.csv"
INVENTORY_OUTPUT = "/home/talentum/omniprice/data/prepared/inventory.csv"
QUALITY_REPORT_OUTPUT = "/home/talentum/omniprice/data/prepared/data_quality_report.csv"


# ============================================================
# Helper Functions
# ============================================================

def classify_product(description):
    """
    Classify products into broad categories using
    deterministic keyword rules.

    Products that do not match any rule are classified as OTHER.
    """

    if pd.isna(description):
        return "OTHER"

    text = str(description).upper()

    category_rules = {
        "BAG": [
            "BAG",
            "PURSE",
            "TOTE",
            "SHOPPER"
        ],

        "MUG": [
            "MUG",
            "CUP",
            "COFFEE CUP",
            "TEA CUP"
        ],

        "CANDLE": [
            "CANDLE",
            "VOTIVE",
            "TEALIGHT"
        ],

        "KITCHEN": [
            "KITCHEN",
            "PLATE",
            "BOWL",
            "SPOON",
            "FORK",
            "KNIFE",
            "CUTLERY",
            "JUG",
            "TEAPOT"
        ],

        "DECORATION": [
            "FRAME",
            "MIRROR",
            "ORNAMENT",
            "DECORATION",
            "WALL",
            "SIGN",
            "HANGING"
        ],

        "TOY": [
            "TOY",
            "GAME",
            "DOLL",
            "PUZZLE"
        ],

        "STATIONERY": [
            "PEN",
            "PENCIL",
            "NOTEBOOK",
            "NOTEBOOK",
            "PAPER",
            "CARD",
            "STATIONERY"
        ],

        "TEXTILE": [
            "CUSHION",
            "PILLOW",
            "TOWEL",
            "BLANKET",
            "FABRIC"
        ],

        "GIFT": [
            "GIFT",
            "PRESENT",
            "WRAP"
        ]
    }

    for category, keywords in category_rules.items():

        for keyword in keywords:

            if keyword in text:
                return category

    return "OTHER"


def generate_inventory(product_demand, product_catalog):
    """
    Generate a synthetic inventory dataset using
    historical demand patterns.

    Inventory values are derived from actual historical
    sales and are not random independent product records.
    """

    inventory = product_demand.copy()

    # --------------------------------------------------------
    # Average daily demand
    # --------------------------------------------------------

    inventory["average_daily_demand"] = (
        inventory["total_quantity_sold"]
        / inventory["active_days"].replace(0, np.nan)
    )

    inventory["average_daily_demand"] = (
        inventory["average_daily_demand"]
        .fillna(0)
        .clip(lower=0)
    )

    # --------------------------------------------------------
    # Lead time
    #
    # Synthetic assumption:
    # 3 to 14 days depending on product characteristics.
    #
    # We derive a deterministic value from product_id so
    # repeated executions produce consistent results.
    # --------------------------------------------------------

    inventory["lead_time_days"] = (
        inventory["product_id"]
        .astype(str)
        .apply(
            lambda x: (sum(ord(char) for char in x) % 12) + 3
        )
    )

    # --------------------------------------------------------
    # Safety stock
    #
    # Approximation:
    # 50% of demand expected during lead time.
    # --------------------------------------------------------

    inventory["safety_stock"] = np.ceil(
        inventory["average_daily_demand"]
        * inventory["lead_time_days"]
        * 0.5
    )

    # --------------------------------------------------------
    # Reorder Level
    #
    # Demand during lead time + safety stock
    # --------------------------------------------------------

    inventory["reorder_level"] = np.ceil(
        (
            inventory["average_daily_demand"]
            * inventory["lead_time_days"]
        )
        + inventory["safety_stock"]
    )

    # --------------------------------------------------------
    # Initial Stock
    #
    # Approximate 30 days of demand + safety stock
    # Minimum 1 unit for products with historical sales.
    # --------------------------------------------------------

    inventory["initial_stock"] = np.ceil(
        inventory["average_daily_demand"] * 30
        + inventory["safety_stock"]
    )

    inventory["initial_stock"] = (
        inventory["initial_stock"]
        .clip(lower=1)
        .astype(int)
    )

    # --------------------------------------------------------
    # Simulated historical stock consumption
    #
    # We use historical quantity sold as consumption.
    # Current stock cannot become negative.
    # --------------------------------------------------------

    inventory["current_stock"] = (
        inventory["initial_stock"]
        - inventory["total_quantity_sold"]
    )

    inventory["current_stock"] = (
        inventory["current_stock"]
        .clip(lower=0)
        .astype(int)
    )

    # --------------------------------------------------------
    # Select final columns
    # --------------------------------------------------------

    inventory = inventory[
        [
            "product_id",
            "initial_stock",
            "current_stock",
            "reorder_level",
            "lead_time_days"
        ]
    ]

    return inventory


# ============================================================
# Main Pipeline
# ============================================================

def main():

    print("=" * 70)
    print("OMNIPRICE - DATA PREPARATION PIPELINE")
    print("=" * 70)

    # --------------------------------------------------------
    # 1. Check source file
    # --------------------------------------------------------

    if not os.path.exists(SOURCE_FILE):

        raise FileNotFoundError(
            f"Source dataset not found: {SOURCE_FILE}"
        )

    print("\n[1/10] Loading OnlineRetail.xlsx...")

    df = pd.read_excel(
        SOURCE_FILE,
        engine="openpyxl"
    )

    print(
        f"Loaded {len(df):,} rows "
        f"and {len(df.columns)} columns."
    )

    # --------------------------------------------------------
    # 2. Standardize column names
    # --------------------------------------------------------

    print("\n[2/10] Standardizing column names...")

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(" ", "_")
    )

    print("Columns:")
    print(df.columns.tolist())

    # --------------------------------------------------------
    # 3. Rename source columns
    # --------------------------------------------------------

    print("\n[3/10] Renaming columns...")

    rename_mapping = {
        "invoiceno": "invoice_no",
        "stockcode": "product_id",
        "description": "description",
        "quantity": "quantity",
        "invoicedate": "invoice_date",
        "unitprice": "unit_price",
        "customerid": "customer_id",
        "country": "country"
    }

    df = df.rename(
        columns=rename_mapping
    )

    required_columns = [
        "invoice_no",
        "product_id",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "country"
    ]

    missing_columns = [
        column
        for column in required_columns
        if column not in df.columns
    ]

    if missing_columns:

        raise ValueError(
            "Required columns are missing: "
            + str(missing_columns)
        )

    # --------------------------------------------------------
    # 4. Data type conversion
    # --------------------------------------------------------

    print("\n[4/10] Converting data types...")

    df["invoice_date"] = pd.to_datetime(
        df["invoice_date"],
        errors="coerce"
    )

    df["quantity"] = pd.to_numeric(
        df["quantity"],
        errors="coerce"
    )

    df["unit_price"] = pd.to_numeric(
        df["unit_price"],
        errors="coerce"
    )

    # --------------------------------------------------------
    # 5. Data cleaning
    # --------------------------------------------------------

    print("\n[5/10] Cleaning transaction data...")

    initial_rows = len(df)

    # Remove duplicate records
    df = df.drop_duplicates()

    # Remove rows with missing critical fields
    df = df.dropna(
        subset=[
            "invoice_no",
            "product_id",
            "quantity",
            "invoice_date",
            "unit_price"
        ]
    )

    # Remove cancelled invoices
    #
    # In the Online Retail dataset, cancelled invoices
    # generally start with "C".
    #

    df["invoice_no"] = (
        df["invoice_no"]
        .astype(str)
        .str.strip()
    )

    df = df[
        ~df["invoice_no"]
        .str.upper()
        .str.startswith("C")
    ]

    # Keep only positive quantities
    df = df[
        df["quantity"] > 0
    ]

    # Keep only positive prices
    df = df[
        df["unit_price"] > 0
    ]

    # Remove invalid product IDs
    df["product_id"] = (
        df["product_id"]
        .astype(str)
        .str.strip()
    )

    df = df[
        df["product_id"] != ""
    ]

    # --------------------------------------------------------
    # 6. Calculate revenue
    # --------------------------------------------------------

    print("\n[6/10] Calculating revenue...")

    df["revenue"] = (
        df["quantity"]
        * df["unit_price"]
    )

    # --------------------------------------------------------
    # 7. Prepare transactions dataset
    # --------------------------------------------------------

    print("\n[7/10] Creating transactions.csv...")

    transaction_columns = [
        "invoice_no",
        "product_id",
        "description",
        "quantity",
        "invoice_date",
        "unit_price",
        "country",
        "revenue"
    ]

    # Add customer_id only if available
    if "customer_id" in df.columns:

        transaction_columns.insert(
            7,
            "customer_id"
        )

    transactions = df[
        transaction_columns
    ].copy()

    transactions = transactions.sort_values(
        by="invoice_date"
    )

    # --------------------------------------------------------
    # 8. Create Product Catalog
    # --------------------------------------------------------

    print("\n[8/10] Creating product_catalog.csv...")

    product_catalog = (
        df[
            [
                "product_id",
                "description",
                "unit_price"
            ]
        ]
        .dropna(
            subset=[
                "product_id",
                "description"
            ]
        )
        .groupby(
            "product_id",
            as_index=False
        )
        .agg(
            product_name=(
                "description",
                "first"
            ),
            selling_price=(
                "unit_price",
                "median"
            )
        )
    )

    product_catalog["category"] = (
        product_catalog[
            "product_name"
        ]
        .apply(
            classify_product
        )
    )

    product_catalog = product_catalog[
        [
            "product_id",
            "product_name",
            "category",
            "selling_price"
        ]
    ]

    # --------------------------------------------------------
    # 9. Calculate historical demand
    # --------------------------------------------------------

    print("\n[9/10] Calculating historical demand...")

    demand = (
        df
        .groupby(
            "product_id",
            as_index=False
        )
        .agg(
            total_quantity_sold=(
                "quantity",
                "sum"
            ),
            first_sale_date=(
                "invoice_date",
                "min"
            ),
            last_sale_date=(
                "invoice_date",
                "max"
            )
        )
    )

    demand["active_days"] = (
        (
            demand["last_sale_date"]
            - demand["first_sale_date"]
        )
        .dt.days
        + 1
    )

    # --------------------------------------------------------
    # 10. Generate Inventory
    # --------------------------------------------------------

    print("\n[10/10] Creating inventory.csv...")

    inventory = generate_inventory(
        demand,
        product_catalog
    )

    # --------------------------------------------------------
    # Create output directories
    # --------------------------------------------------------

    os.makedirs(
        "data/prepared",
        exist_ok=True
    )

    # --------------------------------------------------------
    # Save outputs
    # --------------------------------------------------------

    transactions.to_csv(
        TRANSACTIONS_OUTPUT,
        index=False
    )

    product_catalog.to_csv(
        PRODUCT_CATALOG_OUTPUT,
        index=False
    )

    inventory.to_csv(
        INVENTORY_OUTPUT,
        index=False
    )

    # --------------------------------------------------------
    # Create Data Quality Report
    # --------------------------------------------------------

    quality_report = pd.DataFrame(
        {
            "metric": [
                "initial_rows",
                "final_transaction_rows",
                "removed_rows",
                "unique_products",
                "unique_customers",
                "unique_countries",
                "total_quantity",
                "total_revenue",
                "min_invoice_date",
                "max_invoice_date"
            ],

            "value": [
                initial_rows,
                len(transactions),
                initial_rows - len(transactions),
                transactions[
                    "product_id"
                ].nunique(),

                (
                    transactions[
                        "customer_id"
                    ].nunique()
                    if "customer_id"
                    in transactions.columns
                    else "N/A"
                ),

                transactions[
                    "country"
                ].nunique(),

                transactions[
                    "quantity"
                ].sum(),

                transactions[
                    "revenue"
                ].sum(),

                transactions[
                    "invoice_date"
                ].min(),

                transactions[
                    "invoice_date"
                ].max()
            ]
        }
    )

    quality_report.to_csv(
        QUALITY_REPORT_OUTPUT,
        index=False
    )

    # --------------------------------------------------------
    # Final Summary
    # --------------------------------------------------------

    print("\n" + "=" * 70)
    print("DATA PREPARATION COMPLETED")
    print("=" * 70)

    print(
        f"\nTransactions : "
        f"{len(transactions):,} rows"
    )

    print(
        f"Products     : "
        f"{len(product_catalog):,}"
    )

    print(
        f"Inventory    : "
        f"{len(inventory):,}"
    )

    print(
        f"\nTotal Revenue: "
        f"{transactions['revenue'].sum():,.2f}"
    )

    print("\nOutput files:")

    print(
        f"1. {TRANSACTIONS_OUTPUT}"
    )

    print(
        f"2. {PRODUCT_CATALOG_OUTPUT}"
    )

    print(
        f"3. {INVENTORY_OUTPUT}"
    )

    print(
        f"4. {QUALITY_REPORT_OUTPUT}"
    )

    print("\n" + "=" * 70)


if __name__ == "__main__":
    main()

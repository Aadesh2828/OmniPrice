import pandas as pd

SOURCE_FILE = "/home/talentum/omniprice/data/source/OnlineRetail.xlsx"


def main():

    print("=" * 70)
    print("OMNIPRICE - ONLINE RETAIL DATASET INSPECTION")
    print("=" * 70)

    print("\nLoading dataset...")

    df = pd.read_excel(SOURCE_FILE, engine="openpyxl")

    print("\n1. Dataset Shape")
    print("-" * 70)
    print(f"Rows    : {df.shape[0]:,}")
    print(f"Columns : {df.shape[1]}")

    print("\n2. Column Names")
    print("-" * 70)

    for column in df.columns:
        print(column)

    print("\n3. Data Types")
    print("-" * 70)
    print(df.dtypes)

    print("\n4. First 5 Rows")
    print("-" * 70)
    print(df.head())

    print("\n5. Missing Values")
    print("-" * 70)
    print(df.isnull().sum())

    print("\n6. Duplicate Rows")
    print("-" * 70)
    print(f"Duplicates: {df.duplicated().sum():,}")

    print("\n7. Unique Products")
    print("-" * 70)

    if "StockCode" in df.columns:
        print(df["StockCode"].nunique())

    print("\n8. Unique Customers")
    print("-" * 70)

    if "CustomerID" in df.columns:
        print(df["CustomerID"].nunique())

    print("\n9. Countries")
    print("-" * 70)

    if "Country" in df.columns:
        print(df["Country"].nunique())

    print("\n10. Date Range")
    print("-" * 70)

    if "InvoiceDate" in df.columns:

        dates = pd.to_datetime(
            df["InvoiceDate"],
            errors="coerce"
        )

        print("Minimum:", dates.min())
        print("Maximum:", dates.max())

    print("\n" + "=" * 70)
    print("INSPECTION COMPLETED")
    print("=" * 70)


if __name__ == "__main__":
    main()

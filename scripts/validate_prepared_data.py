import pandas as pd


def main():

    print("=" * 70)
    print("OMNIPRICE - PREPARED DATA VALIDATION")
    print("=" * 70)

    transactions = pd.read_csv(
        "/home/talentum/omniprice/data/prepared/transactions.csv"
    )

    product_catalog = pd.read_csv(
        "/home/talentum/omniprice/data/prepared/product_catalog.csv"
    )

    inventory = pd.read_csv(
        "/home/talentum/omniprice/data/prepared/inventory.csv"
    )

    # --------------------------------------------------------
    # Unique product IDs
    # --------------------------------------------------------

    transaction_products = set(
        transactions["product_id"]
    )

    catalog_products = set(
        product_catalog["product_id"]
    )

    inventory_products = set(
        inventory["product_id"]
    )

    # --------------------------------------------------------
    # Check 1
    # --------------------------------------------------------

    missing_catalog = (
        transaction_products
        - catalog_products
    )

    # --------------------------------------------------------
    # Check 2
    # --------------------------------------------------------

    missing_inventory = (
        transaction_products
        - inventory_products
    )

    print("\nTransaction rows:")
    print(
        f"{len(transactions):,}"
    )

    print("\nUnique transaction products:")
    print(
        f"{len(transaction_products):,}"
    )

    print("\nProduct catalog products:")
    print(
        f"{len(catalog_products):,}"
    )

    print("\nInventory products:")
    print(
        f"{len(inventory_products):,}"
    )

    print("\nProducts missing from catalog:")
    print(
        len(missing_catalog)
    )

    print("\nProducts missing from inventory:")
    print(
        len(missing_inventory)
    )

    # --------------------------------------------------------
    # Validate duplicates
    # --------------------------------------------------------

    print("\nDuplicate Product IDs in Catalog:")
    print(
        product_catalog[
            "product_id"
        ].duplicated().sum()
    )

    print("\nDuplicate Product IDs in Inventory:")
    print(
        inventory[
            "product_id"
        ].duplicated().sum()
    )

    # --------------------------------------------------------
    # Validate negative values
    # --------------------------------------------------------

    print("\nNegative transaction quantities:")
    print(
        (
            transactions["quantity"] < 0
        ).sum()
    )

    print("\nNegative unit prices:")
    print(
        (
            transactions["unit_price"] < 0
        ).sum()
    )

    print("\nNegative current inventory:")
    print(
        (
            inventory["current_stock"] < 0
        ).sum()
    )

    # --------------------------------------------------------
    # Final status
    # --------------------------------------------------------

    if (
        len(missing_catalog) == 0
        and len(missing_inventory) == 0
        and product_catalog[
            "product_id"
        ].duplicated().sum() == 0
        and inventory[
            "product_id"
        ].duplicated().sum() == 0
        and (
            transactions["quantity"] < 0
        ).sum() == 0
        and (
            transactions["unit_price"] < 0
        ).sum() == 0
        and (
            inventory["current_stock"] < 0
        ).sum() == 0
    ):

        print("\n" + "=" * 70)
        print("VALIDATION SUCCESSFUL")
        print("=" * 70)

    else:

        print("\n" + "=" * 70)
        print("VALIDATION FAILED")
        print("Review the results above.")
        print("=" * 70)


if __name__ == "__main__":
    main()

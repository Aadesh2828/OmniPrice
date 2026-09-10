# ============================================================
# OmniPrice Product Loader
# ============================================================

import os
import pandas as pd


# ------------------------------------------------------------
# Locate Project Root
# ------------------------------------------------------------

CURRENT_DIR = os.path.dirname(
    os.path.abspath(__file__)
)

PROJECT_ROOT = os.path.abspath(
    os.path.join(
        CURRENT_DIR,
        "../.."
    )
)


PRODUCT_CATALOG_PATH = os.path.join(
    PROJECT_ROOT,
    "data",
    "prepared",
    "product_catalog.csv"
)


def load_products():

    """
    Load product catalog generated from OnlineRetail.xlsx.

    Returns
    -------
    list
        List of product dictionaries.
    """

    if not os.path.exists(
        PRODUCT_CATALOG_PATH
    ):

        raise FileNotFoundError(
            "Product catalog not found at: "
            + PRODUCT_CATALOG_PATH
        )


    df = pd.read_csv(
        PRODUCT_CATALOG_PATH
    )


    if df.empty:

        raise ValueError(
            "Product catalog is empty."
        )


    print(
        "Product catalog loaded successfully."
    )

    print(
        "Product count:",
        len(df)
    )


    return df.to_dict(
        orient="records"
    )


if __name__ == "__main__":

    products = load_products()

    print()

    print(
        "First 5 products:"
    )

    for product in products[:5]:

        print(
            product
        )

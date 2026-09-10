# ============================================================
# OmniPrice Synthetic Event Generator
# ============================================================

import random
import uuid

from datetime import datetime


# ============================================================
# Clickstream Configuration
# ============================================================

CLICKSTREAM_EVENTS = [

    "view",

    "search",

    "add_to_cart",

    "wishlist",

    "purchase"

]


DEVICES = [

    "Desktop",

    "Mobile",

    "Tablet"

]


COUNTRIES = [

    "United Kingdom",

    "France",

    "Germany",

    "Spain",

    "Portugal",

    "Netherlands",

    "Belgium",

    "Ireland",

    "Switzerland"

]


# ============================================================
# Fictional Competitors
# ============================================================

COMPETITORS = [

    "ShopSphere",

    "PriceNest",

    "RetailHub",

    "NovaCart",

    "DealDeck",

    "MegaMartX",

    "SmartCart",

    "QuickBuy",

    "BuyBasket",

    "PrimeChoice"

]


# ============================================================
# Timestamp Generator
# ============================================================

def generate_timestamp():

    return datetime.utcnow().isoformat()


# ============================================================
# Session ID Generator
# ============================================================

def generate_session_id():

    return (
        "SESSION_"
        + str(
            uuid.uuid4()
        )[:12]
    )


# ============================================================
# User ID Generator
# ============================================================

def generate_user_id():

    return (
        "USER_"
        + str(
            random.randint(
                10000,
                99999
            )
        )
    )


# ============================================================
# Clickstream Event Type
# ============================================================

def generate_clickstream_event():

    return random.choice(
        CLICKSTREAM_EVENTS
    )


# ============================================================
# Device Generator
# ============================================================

def generate_device():

    return random.choice(
        DEVICES
    )


# ============================================================
# Country Generator
# ============================================================

def generate_country():

    return random.choice(
        COUNTRIES
    )


# ============================================================
# Competitor Generator
# ============================================================

def generate_competitor():

    return random.choice(
        COMPETITORS
    )


# ============================================================
# Availability Generator
# ============================================================

def generate_availability():

    return random.choice(

        [

            True,

            True,

            True,

            False

        ]

    )


# ============================================================
# Competitor Price Generator
# ============================================================

def generate_competitor_price(
    base_price
):

    """
    Generate competitor price
    within approximately +/-15%
    of the original product price.
    """

    try:

        base_price = float(
            base_price
        )

    except (
        TypeError,
        ValueError
    ):

        base_price = 10.0


    if base_price <= 0:

        base_price = 10.0


    variation = random.uniform(

        -0.15,

        0.15

    )


    competitor_price = (

        base_price

        *

        (1 + variation)

    )


    return round(

        competitor_price,

        2

    )


# ============================================================
# Generate Clickstream Event
# ============================================================

def generate_clickstream_event_data(
    product
):

    event = {

        "event_time":
            generate_timestamp(),

        "session_id":
            generate_session_id(),

        "user_id":
            generate_user_id(),

        "product_id":
            str(
                product[
                    "product_id"
                ]
            ),

        "product_description":
            str(
                product.get(
                    "product_name",
                    ""
                )
            ),

        "event_type":
            generate_clickstream_event(),

        "device":
            generate_device(),

        "country":
            generate_country()

    }


    return event


# ============================================================
# Generate Competitor Price Event
# ============================================================

def generate_competitor_price_event(
    product
):

    base_price = product.get(

        "BasePrice",

        10.0

    )


    event = {

        "event_time":
            generate_timestamp(),

        "product_id":
            str(
                product[
                    "product_id"
                ]
            ),

        "product_description":
            str(
                product.get(
                    "product_name",
                    ""
                )
            ),

        "competitor":
            generate_competitor(),

        "competitor_price":
            generate_competitor_price(
                base_price
            ),

        "availability":
            generate_availability()

    }


    return event

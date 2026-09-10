# ============================================================
# OmniPrice Kafka Configuration
# ============================================================

# Kafka broker address
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"


# ------------------------------------------------------------
# Kafka Topics
# ------------------------------------------------------------

CLICKSTREAM_TOPIC = "omniprice_clickstream"

COMPETITOR_PRICE_TOPIC = "omniprice_competitor_prices"


# ------------------------------------------------------------
# Product Catalog
# ------------------------------------------------------------

PRODUCT_CATALOG_PATH = (
    "/home/talentum/omniprice/data/prepared/product_catalog.csv"
)


# ------------------------------------------------------------
# Streaming Intervals
# ------------------------------------------------------------

# Clickstream events per second
CLICKSTREAM_INTERVAL = 1


# Competitor price event interval in seconds
COMPETITOR_PRICE_INTERVAL = 2

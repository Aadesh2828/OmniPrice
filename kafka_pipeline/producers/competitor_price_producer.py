# ============================================================
# OmniPrice Competitor Price Kafka Producer
# ============================================================

import json
import random
import time


from kafka import KafkaProducer


from kafka_pipeline.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    COMPETITOR_PRICE_TOPIC,
    COMPETITOR_PRICE_INTERVAL
)


from kafka_pipeline.utils.product_loader import (

    load_products

)


from kafka_pipeline.utils.event_generator import (

    generate_competitor_price_event

)


# ============================================================
# Create Kafka Producer
# ============================================================

def create_producer():

    return KafkaProducer(

        bootstrap_servers=
            KAFKA_BOOTSTRAP_SERVERS,

        value_serializer=lambda value:

            json.dumps(
                value
            ).encode(
                "utf-8"
            )

    )


# ============================================================
# Main Producer
# ============================================================

def main():

    print(
        "=" * 70
    )

    print(
        "OMNIPRICE COMPETITOR PRICE PRODUCER"
    )

    print(
        "=" * 70
    )


    products = load_products()


    producer = create_producer()


    print()

    print(
        "Kafka Broker:",
        KAFKA_BOOTSTRAP_SERVERS
    )


    print(
        "Kafka Topic:",
        COMPETITOR_PRICE_TOPIC
    )


    print(
        "Products:",
        len(products)
    )


    print()

    print(
        "Starting competitor price generation..."
    )


    try:

        while True:

            product = random.choice(
                products
            )


            event = (

                generate_competitor_price_event(

                    product

                )

            )


            future = producer.send(

                COMPETITOR_PRICE_TOPIC,

                value=event

            )


            metadata = future.get(

                timeout=10

            )


            print(

                "[COMPETITOR PRICE] "

                "Partition="

                + str(

                    metadata.partition

                )

                +

                " Offset="

                + str(

                    metadata.offset

                )

                +

                " Event="

                + json.dumps(

                    event

                )

            )


            producer.flush()


            time.sleep(

                COMPETITOR_PRICE_INTERVAL

            )


    except KeyboardInterrupt:

        print()

        print(

            "Competitor price producer stopped."

        )


    finally:

        producer.close()


if __name__ == "__main__":

    main()

# ============================================================
# OmniPrice Clickstream Kafka Producer
# ============================================================

import json
import random
import time


from kafka import KafkaProducer


from kafka_pipeline.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    CLICKSTREAM_TOPIC,
    CLICKSTREAM_INTERVAL
)


from kafka_pipeline.utils.product_loader import (

    load_products

)


from kafka_pipeline.utils.event_generator import (

    generate_clickstream_event_data

)


# ============================================================
# Kafka Producer
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
        "OMNIPRICE CLICKSTREAM PRODUCER"
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
        CLICKSTREAM_TOPIC
    )


    print(
        "Products:",
        len(products)
    )


    print()

    print(
        "Starting clickstream generation..."
    )


    try:

        while True:

            product = random.choice(
                products
            )


            event = (
                generate_clickstream_event_data(
                    product
                )
            )


            future = producer.send(

                CLICKSTREAM_TOPIC,

                value=event

            )


            metadata = future.get(
                timeout=10
            )


            print(

                "[CLICKSTREAM] "

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

                CLICKSTREAM_INTERVAL

            )


    except KeyboardInterrupt:

        print()

        print(
            "Clickstream producer stopped."
        )


    finally:

        producer.close()


if __name__ == "__main__":

    main()

# ============================================================
# OmniPrice Competitor Price Kafka Consumer
# ============================================================

import json


from kafka import KafkaConsumer


from kafka_pipeline.config import (

    KAFKA_BOOTSTRAP_SERVERS,

    COMPETITOR_PRICE_TOPIC

)


def main():

    print(
        "=" * 70
    )

    print(
        "OMNIPRICE COMPETITOR PRICE CONSUMER"
    )

    print(
        "=" * 70
    )


    consumer = KafkaConsumer(

        COMPETITOR_PRICE_TOPIC,

        bootstrap_servers=
            KAFKA_BOOTSTRAP_SERVERS,

        auto_offset_reset=
            "earliest",

        enable_auto_commit=
            True,

        group_id=
            "omniprice-competitor-consumer",

        value_deserializer=
            lambda value:

                json.loads(

                    value.decode(
                        "utf-8"
                    )

                )

    )


    print()

    print(

        "Listening to:",

        COMPETITOR_PRICE_TOPIC

    )


    print()


    try:

        for message in consumer:

            print(

                "[COMPETITOR PRICE EVENT]"

            )


            print(

                json.dumps(

                    message.value,

                    indent=4

                )

            )


            print(

                "Partition:",

                message.partition

            )


            print(

                "Offset:",

                message.offset

            )


            print(

                "-" * 60

            )


    except KeyboardInterrupt:

        print(

            "Consumer stopped."

        )


    finally:

        consumer.close()


if __name__ == "__main__":

    main()

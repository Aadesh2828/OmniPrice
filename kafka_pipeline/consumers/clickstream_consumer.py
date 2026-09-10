# ============================================================
# OmniPrice Clickstream Kafka Consumer
# ============================================================

import json


from kafka import KafkaConsumer


from kafka_pipeline.config import (

    KAFKA_BOOTSTRAP_SERVERS,

    CLICKSTREAM_TOPIC

)


def main():

    print(
        "=" * 70
    )

    print(
        "OMNIPRICE CLICKSTREAM CONSUMER"
    )

    print(
        "=" * 70
    )


    consumer = KafkaConsumer(

        CLICKSTREAM_TOPIC,

        bootstrap_servers=
            KAFKA_BOOTSTRAP_SERVERS,

        auto_offset_reset=
            "earliest",

        enable_auto_commit=
            True,

        group_id=
            "omniprice-clickstream-consumer",

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
        CLICKSTREAM_TOPIC
    )


    print()


    try:

        for message in consumer:

            print(

                "[CLICKSTREAM EVENT]"

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

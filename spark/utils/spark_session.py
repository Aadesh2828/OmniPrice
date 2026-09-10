from pyspark.sql import SparkSession


def get_spark_session(app_name):

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config(
            "spark.sql.parquet.compression.codec",
            "snappy"
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true"
        )
        .config(
            "spark.sql.shuffle.partitions",
            "8"
        )
        .getOrCreate()
    )

    return spark

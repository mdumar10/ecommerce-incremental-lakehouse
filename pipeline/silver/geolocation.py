from pyspark.sql.functions import trim


def transform_geolocation(df):

    return (
        df
        .withColumn("geolocation_city", trim("geolocation_city"))
        .withColumn("geolocation_state", trim("geolocation_state"))
        .dropDuplicates()
    )


def run_geolocation(spark):

    bronze_table = "ecommerce.bronze.geolocation"
    silver_table = "ecommerce.silver.geolocation"

    df = spark.table(bronze_table)

    df = transform_geolocation(df)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )


run_geolocation(spark)
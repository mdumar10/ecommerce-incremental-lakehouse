from pyspark.sql.functions import trim


def transform_sellers(df):

    return (
        df
        .withColumn("seller_city", trim("seller_city"))
        .withColumn("seller_state", trim("seller_state"))
        .dropDuplicates(["seller_id"])
    )


def run_sellers(spark):

    bronze_table = "ecommerce.bronze.sellers"
    silver_table = "ecommerce.silver.sellers"

    df = spark.table(bronze_table)

    df = transform_sellers(df)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )


run_sellers(spark)
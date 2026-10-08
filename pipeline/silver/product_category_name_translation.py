from pyspark.sql.functions import trim


def transform_product_category_name_translation(df):

    return (
        df
        .withColumn(
            "product_category_name",
            trim("product_category_name")
        )
        .withColumn(
            "product_category_name_english",
            trim("product_category_name_english")
        )
        .dropDuplicates(["product_category_name"])
    )


def run_product_category_name_translation(spark):

    bronze_table = "ecommerce.bronze.product_category_name_translation"
    silver_table = "ecommerce.silver.product_category_name_translation"

    df = spark.table(bronze_table)

    df = transform_product_category_name_translation(df)

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )


run_product_category_name_translation(spark)
from pyspark.sql.functions import col, trim, when

from pipeline.silver.utils import (
    write_quarantine
)


def transform_products(df):

    return (
        df
        .withColumn(
            "product_category_name",
            trim("product_category_name")
        )
        .dropDuplicates(["product_id"])
    )


def check_products(df):

    return (
        when(
            col("product_id").isNull(),
            "missing_product_id"
        )
        .when(
            col("product_weight_g").isNull(),
            "missing_product_weight"
        )
        .when(
            col("product_length_cm").isNull(),
            "missing_product_length"
        )
        .when(
            col("product_height_cm").isNull(),
            "missing_product_height"
        )
        .when(
            col("product_width_cm").isNull(),
            "missing_product_width"
        )
        .when(
            col("product_weight_g") <= 0,
            "invalid_product_weight"
        )
        .when(
            col("product_length_cm") <= 0,
            "invalid_product_length"
        )
        .when(
            col("product_height_cm") <= 0,
            "invalid_product_height"
        )
        .when(
            col("product_width_cm") <= 0,
            "invalid_product_width"
        )
    )


def run_products(spark):

    bronze_table = "ecommerce.bronze.products"
    silver_table = "ecommerce.silver.products"

    df = spark.table(bronze_table)

    df = transform_products(df)

    check = check_products(df)

    invalid = (
        df
        .filter(check.isNotNull())
        .withColumn("quarantine_reason", check)
    )

    valid = df.filter(check.isNull())

    write_quarantine(
        spark,
        invalid,
        "ecommerce.quarantine.products",
        "product_id"
    )

    (
        valid.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(silver_table)
    )


run_products(spark)
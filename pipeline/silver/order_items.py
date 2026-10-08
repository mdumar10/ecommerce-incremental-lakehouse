from delta.tables import DeltaTable
from pyspark.sql.functions import col, when

from pipeline.silver.utils import (
    table_exists,
    get_current_watermark,
    write_quarantine
)


def transform_order_items(df):

    return (
        df
        .dropDuplicates(["order_id", "order_item_id"])
    )


def check_order_items(df):

    return (
        when(
            col("order_id").isNull(),
            "missing_order_id"
        )
        .when(
            col("order_item_id").isNull(),
            "missing_order_item_id"
        )
        .when(
            col("product_id").isNull(),
            "missing_product_id"
        )
        .when(
            col("seller_id").isNull(),
            "missing_seller_id"
        )
        .when(
            col("shipping_limit_date").isNull(),
            "missing_shipping_limit_date"
        )
        .when(
            col("price").isNull(),
            "missing_price"
        )
        .when(
            col("freight_value").isNull(),
            "missing_freight_value"
        )
        .when(
            col("price") < 0,
            "negative_price"
        )
        .when(
            col("freight_value") < 0,
            "negative_freight_value"
        )
    )


def run_order_items(spark):

    bronze_table = "ecommerce.bronze.order_items"
    silver_table = "ecommerce.silver.order_items"

    silver_exists = table_exists(spark, silver_table)

    if not silver_exists:

        df = spark.table(bronze_table)

    else:

        watermark = get_current_watermark(
            spark,
            silver_table
        )

        df = (
            spark.table(bronze_table)
            .filter(col("updated_at") >= watermark)
        )

    # Normal transformation
    df = transform_order_items(df)

    # Order items quality rules
    check = check_order_items(df)

    # Separate invalid and valid rows
    invalid = (
        df
        .filter(check.isNotNull())
        .withColumn("quarantine_reason", check)
    )

    valid = df.filter(check.isNull())

    # Invalid rows → Quarantine
    write_quarantine(
        spark,
        invalid,
        "ecommerce.quarantine.order_items",
        """
        target.order_id = source.order_id
        AND target.order_item_id = source.order_item_id
        """
    )

    # Valid rows → Silver
    if not silver_exists:

        (
            valid.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(silver_table)
        )

    else:

        target = DeltaTable.forName(
            spark,
            silver_table
        )

        (
            target.alias("target")
            .merge(
                valid.alias("source"),
                """
                target.order_id = source.order_id
                AND target.order_item_id = source.order_item_id
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


run_order_items(spark)
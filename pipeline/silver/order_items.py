from delta.tables import DeltaTable
from pyspark.sql.functions import col

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
        (col("order_id").isNull()) |
        (col("order_item_id").isNull()) |
        (col("product_id").isNull()) |
        (col("seller_id").isNull()) |
        (col("shipping_limit_date").isNull()) |
        (col("price").isNull()) |
        (col("freight_value").isNull()) |
        (col("price") < 0) |
        (col("freight_value") < 0)
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

    df = transform_order_items(df)

    check = check_order_items(df)

    invalid = df.filter(check)

    valid = df.filter(~check)

    write_quarantine(
        spark,
        invalid,
        "ecommerce.quarantine.order_items",
        """
        target.order_id = source.order_id
        AND target.order_item_id = source.order_item_id
        """
    )

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
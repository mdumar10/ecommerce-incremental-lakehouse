from delta.tables import DeltaTable
from pyspark.sql.functions import col, trim, when

from pipeline.silver.utils import (
    table_exists,
    get_current_watermark,
    write_quarantine
)


def transform_orders(df):

    return (
        df
        .withColumn("order_status", trim("order_status"))
        .dropDuplicates(["order_id"])
    )


def check_orders(df):

    return (
        when(
            (col("order_status") == "delivered") &
            col("order_delivered_customer_date").isNull(),
            "delivered_order_missing_delivery_date"
        )
        .when(
            col("order_delivered_carrier_date") <
            col("order_purchase_timestamp"),
            "carrier_before_purchase"
        )
        .when(
            col("order_delivered_customer_date") <
            col("order_delivered_carrier_date"),
            "delivery_before_carrier"
        )
    )


def run_orders(spark):

    bronze_table = "ecommerce.bronze.orders"
    silver_table = "ecommerce.silver.orders"

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
    df = transform_orders(df)


    check = check_orders(df)

    # Separate valid and invalid rows
    invalid = (
        df
        .filter(check.isNotNull())
        .withColumn("quarantine_reason", check)
    )

    valid = df.filter(check.isNull())

    # Invalid rows → Quarantine
    # Invalid rows → Quarantine
    write_quarantine(
        spark,
        invalid,
        "ecommerce.quarantine.orders",
        "target.order_id = source.order_id"
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
                "target.order_id = source.order_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


run_orders(spark)


















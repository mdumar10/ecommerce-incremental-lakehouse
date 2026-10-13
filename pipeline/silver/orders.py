
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

    # A missing primary key makes the order unusable.
    # Timestamp anomalies alone do not invalidate the order.
    return when(
        col("order_id").isNull(),
        "missing_order_id"
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

    df = transform_orders(df)

    check = check_orders(df)

    invalid = (
        df
        .filter(check.isNotNull())
        .withColumn("quarantine_reason", check)
    )

    valid = df.filter(check.isNull())

    write_quarantine(
        spark,
        invalid,
        "ecommerce.quarantine.orders",
        "target.order_id <=> source.order_id"
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
                "target.order_id = source.order_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

if __name__ == "__main__":
    run_orders(spark)


















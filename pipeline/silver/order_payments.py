from delta.tables import DeltaTable
from pyspark.sql.functions import col, trim, when

from pipeline.silver.utils import (
    table_exists,
    get_current_watermark,
    write_quarantine
)


def transform_order_payments(df):

    return (
        df
        .withColumn("payment_type", trim("payment_type"))
        .dropDuplicates(["order_id", "payment_sequential"])
    )


def check_order_payments(df):

    return (
        when(
            col("payment_installments") <= 0,
            "invalid_payment_installments"
        )
    )


def run_order_payments(spark):

    bronze_table = "ecommerce.bronze.order_payments"
    silver_table = "ecommerce.silver.order_payments"

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
    df = transform_order_payments(df)

    # Payment quality rules
    check = check_order_payments(df)

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
        "ecommerce.quarantine.order_payments",
        """
        target.order_id = source.order_id
        AND target.payment_sequential = source.payment_sequential
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
                AND target.payment_sequential = source.payment_sequential
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


run_order_payments(spark)
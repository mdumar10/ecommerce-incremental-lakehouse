from delta.tables import DeltaTable
from pyspark.sql.functions import col, trim, when

from pipeline.silver.utils import (
    table_exists,
    get_current_watermark,
    write_quarantine
)


def transform_customers(df):

    return (
        df
        .withColumn("customer_city", trim("customer_city"))
        .withColumn("customer_state", trim("customer_state"))
        .dropDuplicates(["customer_id"])
    )


def check_customers(df):

    return (
        when(
            col("customer_id").isNull(),
            "missing_customer_id"
        )
        .when(
            (col("customer_zip_code_prefix") < 0) |
            (col("customer_zip_code_prefix") > 99999),
            "invalid_zip_code"
        )
        .when(
            col("customer_state").isNull() |
            (trim("customer_state") == ""),
            "missing_customer_state"
        )
    )


def run_customers(spark):

    bronze_table = "ecommerce.bronze.customers"
    silver_table = "ecommerce.silver.customers"

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
    df = transform_customers(df)

    # Customer quality rules
    check = check_customers(df)

    # Separate valid and invalid rows
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
        "ecommerce.quarantine.customers",
        "customer_id"
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
                "target.customer_id = source.customer_id"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


run_customers(spark)
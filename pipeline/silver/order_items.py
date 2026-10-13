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

if __name__ == "__main__":
    run_order_items(spark)


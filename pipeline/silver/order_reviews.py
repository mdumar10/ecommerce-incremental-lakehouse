from delta.tables import DeltaTable
from pyspark.sql.functions import col, trim, when

from pipeline.silver.utils import (
    table_exists,
    get_current_watermark,
    write_quarantine
)


def transform_order_reviews(df):

    return (
        df
        .withColumn("review_comment_title", trim("review_comment_title"))
        .withColumn("review_comment_message", trim("review_comment_message"))
        .dropDuplicates(["review_id", "order_id"])
    )


def check_order_reviews(df):

    return (
        when(
            (col("review_score") < 1) |
            (col("review_score") > 5),
            "invalid_review_score"
        )
        .when(
            col("review_answer_timestamp") <
            col("review_creation_date"),
            "answer_before_creation"
        )
    )


def run_order_reviews(spark):

    bronze_table = "ecommerce.bronze.order_reviews"
    silver_table = "ecommerce.silver.order_reviews"

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
    df = transform_order_reviews(df)

    # Review quality rules
    check = check_order_reviews(df)

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
        "ecommerce.quarantine.order_reviews",
        """
        target.review_id = source.review_id
        AND target.order_id = source.order_id
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
                target.review_id = source.review_id
                AND target.order_id = source.order_id
                """
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )


run_order_reviews(spark)
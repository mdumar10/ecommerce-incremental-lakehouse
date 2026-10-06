from delta.tables import DeltaTable
from pyspark.sql.functions import max


def table_exists(spark, table_name):

    return spark.catalog.tableExists(table_name)


def get_current_watermark(
    spark,
    table_name,
    watermark_column="updated_at"
):

    return (
        spark.table(table_name)
        .select(max(watermark_column))
        .first()[0]
    )


def write_quarantine(
    spark,
    df,
    table_name,
    key_column
):

    if not df.take(1):
        return

    if not table_exists(spark, table_name):

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(table_name)
        )

    else:

        target = DeltaTable.forName(
            spark,
            table_name
        )

        (
            target.alias("target")
            .merge(
                df.alias("source"),
                f"target.{key_column} = source.{key_column}"
            )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
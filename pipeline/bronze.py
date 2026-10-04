

import yaml
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from pipeline.connection import (
    get_postgres_connection,
    read_postgres_table
)


def load_config(config):
    with open(config, "r") as file:
        return yaml.safe_load(file)


def table_exists(spark, table_name):
    try:
        spark.table(table_name).limit(1).collect()
        return True
    except Exception as e:
        if "TABLE_OR_VIEW_NOT_FOUND" in str(e):
            return False
        raise


def get_current_watermark(spark, table_name, watermark_column):
    existing_df = spark.table(table_name)

    current_watermark = (
        existing_df
        .select(F.max(watermark_column))
        .first()[0]
    )

    return current_watermark


def build_source_query(
    source_schema,
    table_name,
    watermark_column,
    watermark
):
    return f"""
        (
            SELECT *
            FROM {source_schema}.{table_name}
            WHERE {watermark_column} >= '{watermark}'
        ) AS source
    """


def merge_incremental_data(
    spark,
    df,
    bronze_table,
    keys
):
    target = DeltaTable.forName(
        spark,
        bronze_table
    )

    conditions = [
        f"target.`{key}` = source.`{key}`"
        for key in keys
    ]

    merge_condition = " AND ".join(conditions)

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            merge_condition
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def run_bronze(spark, dbutils, config):

    config = load_config(config)

    jdbc_url, properties = get_postgres_connection(dbutils)

    source_schema = config["source_schema"]
    bronze_schema = config["bronze_schema"]

    for table_name, settings in config["tables"].items():

        load_type = settings["load_type"]
        keys = settings["keys"]
        watermark_column = settings.get("watermark_column")

        bronze_table = f"{bronze_schema}.{table_name}"

        if  not table_exists(spark, bronze_table):

            df = read_postgres_table(
                spark,
                f"{source_schema}.{table_name}",
                jdbc_url,
                properties
            )

            (
                df.write
                .format("delta")
                .mode("overwrite")
                .saveAsTable(bronze_table)
            )

        elif load_type == "incremental":

            watermark = get_current_watermark(
                spark,
                bronze_table,
                watermark_column
            )

            query = build_source_query(
                source_schema,
                table_name,
                watermark_column,
                watermark
            )

            df = read_postgres_table(
                spark,
                query,
                jdbc_url,
                properties
            )

            merge_incremental_data(
                spark,
                df,
                bronze_table,
                keys
            )

        else:
            pass



config = "../config/tables.yml"


run_bronze(
    spark,
    dbutils,
    config
)



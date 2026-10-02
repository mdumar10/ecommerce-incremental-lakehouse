from pathlib import Path

import yaml
from delta.tables import DeltaTable
from pyspark.sql import functions as F

from pipeline.connection import (
    get_postgres_connection,
    read_postgres_table
)


CONFIG_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "tables.yml"
)


def load_config():
    with open(CONFIG_PATH, "r") as file:
        return yaml.safe_load(file)


def table_exists(spark, table_name):

    return spark.catalog.tableExists(table_name)


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


def write_incremental_bronze(
    spark,
    df,
    bronze_table,
    keys,
    watermark_column
):

    if not table_exists(spark, bronze_table):

        (
            df.write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(bronze_table)
        )

        return

    match_conditions = [
        f"target.`{key}` = source.`{key}`"
        for key in keys
    ]

    match_conditions.append(
        f"target.`{watermark_column}` = "
        f"source.`{watermark_column}`"
    )

    merge_condition = " AND ".join(match_conditions)

    target = DeltaTable.forName(
        spark,
        bronze_table
    )

    (
        target.alias("target")
        .merge(
            df.alias("source"),
            merge_condition
        )
        .whenNotMatchedInsertAll()
        .execute()
    )


def write_static_bronze(
    df,
    bronze_table
):

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .saveAsTable(bronze_table)
    )


def run_bronze(spark, dbutils):

    config = load_config()

    source_schema = config["source_schema"]
    bronze_schema = config["bronze_schema"]

    spark.sql(
        f"CREATE SCHEMA IF NOT EXISTS {bronze_schema}"
    )

    jdbc_url, properties = get_postgres_connection(
        dbutils
    )

    for table_name, settings in config["tables"].items():

        load_type = settings["load_type"]
        keys = settings["keys"]
        watermark_column = settings.get(
            "watermark_column"
        )

        bronze_table = (
            f"{bronze_schema}.{table_name}"
        )

        print(
            f"Starting Bronze load: {table_name}"
        )

        if load_type == "incremental":

            if table_exists(
                spark,
                bronze_table
            ):

                current_watermark = (
                    spark.read
                    .table(bronze_table)
                    .select(
                        F.max(
                            F.col(watermark_column)
                        ).alias("watermark")
                    )
                    .first()["watermark"]
                )

            else:
                current_watermark = None

            if current_watermark is None:

                df = read_postgres_table(
                    spark,
                    f"{source_schema}.{table_name}",
                    jdbc_url,
                    properties
                )

            else:

                query = build_source_query(
                    source_schema,
                    table_name,
                    watermark_column,
                    current_watermark
                )

                df = read_postgres_table(
                    spark,
                    query,
                    jdbc_url,
                    properties
                )

            if df.isEmpty():

                print(
                    f"No new changes: {table_name}"
                )

                continue

            rows_read = df.count()

            write_incremental_bronze(
                spark,
                df,
                bronze_table,
                keys,
                watermark_column
            )

            print(
                f"Bronze complete: {table_name} "
                f"| rows read = {rows_read}"
            )

        elif load_type == "static":

            if table_exists(
                spark,
                bronze_table
            ):

                print(
                    f"Static table already loaded: "
                    f"{table_name}"
                )

                continue

            df = read_postgres_table(
                spark,
                f"{source_schema}.{table_name}",
                jdbc_url,
                properties
            )

            if df.isEmpty():

                print(
                    f"No data found: {table_name}"
                )

                continue

            rows_read = df.count()

            write_static_bronze(
                df,
                bronze_table
            )

            print(
                f"Bronze complete: {table_name} "
                f"| rows read = {rows_read}"
            )

        else:

            raise ValueError(
                f"Unknown load_type '{load_type}' "
                f"for table '{table_name}'"
            )

    print("Bronze pipeline completed.")
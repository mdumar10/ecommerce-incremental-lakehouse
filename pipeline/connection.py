from pyspark.sql import DataFrame


def get_postgres_connection(dbutils):

    host = dbutils.secrets.get(
        scope="supabase",
        key="SUPABASE_HOST"
    )

    port = dbutils.secrets.get(
        scope="supabase",
        key="SUPABASE_PORT"
    )

    database = dbutils.secrets.get(
        scope="supabase",
        key="SUPABASE_DATABASE"
    )

    user = dbutils.secrets.get(
        scope="supabase",
        key="SUPABASE_USER"
    )

    password = dbutils.secrets.get(
        scope="supabase",
        key="SUPABASE_PASSWORD"
    )

    jdbc_url = (
        f"jdbc:postgresql://{host}:{port}/{database}"
        "?sslmode=require"
    )

    properties = {
        "user": user,
        "password": password,
        "driver": "org.postgresql.Driver"
    }

    return jdbc_url, properties


def read_postgres_table(
    spark,
    table_name: str,
    jdbc_url: str,
    properties: dict
) -> DataFrame:

    return (
        spark.read
        .format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table_name)
        .options(**properties)
        .load()
    )
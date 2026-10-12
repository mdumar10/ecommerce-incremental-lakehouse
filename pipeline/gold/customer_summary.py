def run_customer_summary(spark):

    spark.sql("CREATE SCHEMA IF NOT EXISTS ecommerce.gold")

    # Aggregate orders to one row per customer_id
    customer_order_summary = spark.sql("""
        SELECT
            customer_id,
            COUNT(*) AS order_count,
            CAST(
                SUM(total_product_value) AS DECIMAL(18, 2)
            ) AS total_product_value,
            MIN(order_purchase_timestamp) AS first_order_timestamp,
            MAX(order_purchase_timestamp) AS last_order_timestamp
        FROM ecommerce.gold.fact_orders
        GROUP BY customer_id
    """)

    customer_order_summary.createOrReplaceTempView(
        "customer_order_summary"
    )

    # Attach the order metrics to the customer records
    customer_summary = spark.sql("""
        SELECT
            c.customer_id,
            c.customer_unique_id,
            c.customer_zip_code_prefix,
            c.customer_city,
            c.customer_state,

            COALESCE(o.order_count, 0) AS order_count,
            CAST(
                COALESCE(o.total_product_value, 0)
                AS DECIMAL(18, 2)
            ) AS total_product_value,

            o.first_order_timestamp,
            o.last_order_timestamp

        FROM ecommerce.silver.customers c

        LEFT JOIN customer_order_summary o
            ON c.customer_id = o.customer_id
    """)

    (
        customer_summary.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("ecommerce.gold.customer_summary")
    )


run_customer_summary(spark)
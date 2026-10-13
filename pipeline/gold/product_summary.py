def run_product_summary(spark):

    spark.sql("CREATE SCHEMA IF NOT EXISTS ecommerce.gold")

    # Aggregate sales metrics to one row per product_id
    product_sales_summary = spark.sql("""
        SELECT
            product_id,
            COUNT(*) AS order_line_count,
            COUNT(DISTINCT order_id) AS order_count,
            CAST(SUM(price) AS DECIMAL(18, 2)) AS total_product_value,
            CAST(SUM(freight_value) AS DECIMAL(18, 2)) AS total_freight_value
        FROM ecommerce.silver.order_items
        GROUP BY product_id
    """)

    product_sales_summary.createOrReplaceTempView(
        "product_sales_summary"
    )

    # Attach sales metrics to the product records
    product_summary = spark.sql("""
        SELECT
            p.product_id,
            p.product_category_name,
            p.product_photos_qty,
            p.product_weight_g,
            p.product_length_cm,
            p.product_height_cm,
            p.product_width_cm,

            COALESCE(s.order_line_count, 0) AS order_line_count,
            COALESCE(s.order_count, 0) AS order_count,
            CAST(
                COALESCE(s.total_product_value, 0)
                AS DECIMAL(18, 2)
            ) AS total_product_value,
            CAST(
                COALESCE(s.total_freight_value, 0)
                AS DECIMAL(18, 2)
            ) AS total_freight_value

        FROM ecommerce.silver.products p

        LEFT JOIN product_sales_summary s
            ON p.product_id = s.product_id
    """)

    (
        product_summary.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("ecommerce.gold.product_summary")
    )

if __name__ == "__main__":
    run_product_summary(spark)
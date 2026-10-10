def run_fact_orders(spark):

    spark.sql("CREATE SCHEMA IF NOT EXISTS ecommerce.gold")

    #  order items: one row per order
    item_summary = spark.sql("""
        SELECT
            order_id,
            COUNT(*) AS item_line_count,
            CAST(SUM(price) AS DECIMAL(18, 2)) AS total_product_value,
            CAST(SUM(freight_value) AS DECIMAL(18, 2)) AS total_freight_value
        FROM ecommerce.silver.order_items
        GROUP BY order_id
    """)

    item_summary.createOrReplaceTempView("item_summary")

    #  payments: one row per order
    payment_summary = spark.sql("""
        SELECT
            order_id,
            COUNT(*) AS payment_count,
            CAST(SUM(payment_value) AS DECIMAL(18, 2)) AS total_payment_value
        FROM ecommerce.silver.order_payments
        GROUP BY order_id
    """)

    payment_summary.createOrReplaceTempView("payment_summary")

    # reviews: one row per order
    review_summary = spark.sql("""
        SELECT
            order_id,
            COUNT(*) AS review_count,
            ROUND(AVG(review_score), 2) AS average_review_score
        FROM ecommerce.silver.order_reviews
        GROUP BY order_id
    """)

    review_summary.createOrReplaceTempView("review_summary")




    fact_orders = spark.sql("""
        SELECT
            o.order_id,
            o.customer_id,
            o.order_status,
            o.order_purchase_timestamp,
            o.order_approved_at,
            o.order_delivered_carrier_date,
            o.order_delivered_customer_date,
            o.order_estimated_delivery_date,

            COALESCE(i.item_line_count, 0) AS item_line_count,
            i.total_product_value,
            i.total_freight_value,

            COALESCE(p.payment_count, 0) AS payment_count,
            p.total_payment_value,

            COALESCE(r.review_count, 0) AS review_count,
            r.average_review_score

        FROM ecommerce.silver.orders o

        LEFT JOIN item_summary i
            ON o.order_id = i.order_id

        LEFT JOIN payment_summary p
            ON o.order_id = p.order_id

        LEFT JOIN review_summary r
            ON o.order_id = r.order_id
    """)  
    (
        fact_orders.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable("ecommerce.gold.fact_orders")
    )


run_fact_orders(spark)


















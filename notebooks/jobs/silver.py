
# Databricks notebook source

from pathlib import Path
import sys

PROJECT_ROOT = Path.cwd().resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.silver.customers import run_customers
from pipeline.silver.orders import run_orders
from pipeline.silver.order_items import run_order_items
from pipeline.silver.order_payments import run_order_payments
from pipeline.silver.order_reviews import run_order_reviews
from pipeline.silver.products import run_products
from pipeline.silver.sellers import run_sellers
from pipeline.silver.geolocation import run_geolocation
from pipeline.silver.product_category_name_translation import (
    run_product_category_name_translation
)

run_customers(spark)
run_orders(spark)
run_order_items(spark)
run_order_payments(spark)
run_order_reviews(spark)
run_products(spark)
run_sellers(spark)
run_geolocation(spark)
run_product_category_name_translation(spark)

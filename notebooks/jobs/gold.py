# Databricks notebook source

from pathlib import Path
import sys

PROJECT_ROOT = Path.cwd().resolve().parents[1]

if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.gold.fact_orders import run_fact_orders
from pipeline.gold.customer_summary import run_customer_summary
from pipeline.gold.product_summary import run_product_summary


run_fact_orders(spark)
run_customer_summary(spark)
run_product_summary(spark)
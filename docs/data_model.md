# Data Model

The Gold layer contains three analytical tables. It is a set of denormalized data marts, not a traditional star schema or snowflake schema.

## 1. fact_orders

**Grain:** One row per `order_id`.

This table starts from Silver orders and adds order-level metrics calculated from order items, payments and reviews.

Before joining, each child table is aggregated to one row per order. This avoids duplicate orders caused by one-to-many joins.

It includes order details, product value, freight value, payment totals and review metrics.

## 2. customer_summary

**Grain:** One row per `customer_id`.

This table combines Silver customer details with metrics calculated from `fact_orders`, including order count, total product value, first order date and last order date.

Customers without orders are retained, with their order count and total product value set to zero.

## 3. product_summary

**Grain:** One row per `product_id`.

This table combines Silver product details with sales metrics calculated from Silver order items. It includes order-line count, distinct order count, product value and freight value.

Products without matching order items are retained, with their sales counts and totals set to zero.

## Loading strategy

All three Gold tables use full refresh. On each pipeline run, the tables are recalculated and written using overwrite mode.

Bronze ingestion can be incremental, but Gold is rebuilt from the current Silver data on every run.

## Validation

After the incremental test, I checked that each table's row count matched its distinct key count:

- `fact_orders`: `order_id`
- `customer_summary`: `customer_id`
- `product_summary`: `product_id`

These checks passed after the test run.

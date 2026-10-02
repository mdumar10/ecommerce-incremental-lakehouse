import os
import random
import uuid
from datetime import datetime, timedelta
from decimal import Decimal

import psycopg2
import yaml
from dotenv import load_dotenv


# ============================================================
# ENVIRONMENT
# ============================================================

load_dotenv()


# ============================================================
# CONFIGURATION
# ============================================================

def load_config():
    with open("config/config.yaml", "r") as f:
        return yaml.safe_load(f)


CONFIG = load_config()["generator"]

ORDERS_PER_RUN          = CONFIG["orders_per_run"]
EXISTING_CUSTOMER_ORDERS = CONFIG["existing_customer_orders"]
NEW_CUSTOMER_ORDERS     = CONFIG["new_customer_orders"]


# ============================================================
# DATABASE CONNECTION
# ============================================================

def get_connection():
    return psycopg2.connect(
        host=os.getenv("SUPABASE_HOST"),
        port=os.getenv("SUPABASE_PORT"),
        database=os.getenv("SUPABASE_DATABASE"),
        user=os.getenv("SUPABASE_USER"),
        password=os.getenv("SUPABASE_PASSWORD"),
    )


# ============================================================
# HELPER : Generate a 32-char hex ID (matches Olist format)
# ============================================================

def generate_id():
    return uuid.uuid4().hex


# ============================================================
# FETCH : Get existing customers from database
# ============================================================

def get_existing_customers(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT
                customer_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state
            FROM customers
            """
        )
        return cursor.fetchall()


# ============================================================
# HELPER : Extract zip/city/state from customer list
# ============================================================

def get_locations(customers):
    return [
        (c[1], c[2], c[3])
        for c in customers
    ]


# ============================================================
# FETCH : Get valid product/seller pairs from order_items
# ============================================================

def get_product_seller_pairs(conn):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT DISTINCT
                product_id,
                seller_id
            FROM order_items
            WHERE product_id IS NOT NULL
              AND seller_id IS NOT NULL
            """
        )
        return cursor.fetchall()


# ============================================================
# INSERT : Create one new customer
# ============================================================

def create_customer(conn, locations):
    customer_id        = generate_id()
    customer_unique_id = generate_id()
    zip_code, city, state = random.choice(locations)
    now = datetime.now()

    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO customers (
                customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                customer_id,
                customer_unique_id,
                zip_code,
                city,
                state,
                now,
                now,
            ),
        )

    return customer_id


# ============================================================
# INSERT : Create one new order
# ============================================================

def create_order(conn, customer_id):
    order_id      = generate_id()
    purchase_time = datetime.now()

    order_status = random.choice(["created", "processing", "approved"])

    # only approved/processing orders get an approval time
    approved_at = None
    if order_status in {"approved", "processing"}:
        approved_at = purchase_time + timedelta(minutes=random.randint(5, 120))

    estimated_delivery = purchase_time + timedelta(days=random.randint(7, 25))

    now = datetime.now()

    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO orders (
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp,
                order_approved_at,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                order_estimated_delivery_date,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                order_id,
                customer_id,
                order_status,
                purchase_time,
                approved_at,
                None,
                None,
                estimated_delivery,
                now,
                now,
            ),
        )

    return order_id


# ============================================================
# INSERT : Create 1-3 items for an order
#          Returns total value so payment can match
# ============================================================

def create_order_items(conn, order_id, product_seller_pairs):
    num_items   = random.randint(1, 3)
    total_value = Decimal("0.00")
    items       = []

    for item_num in range(1, num_items + 1):
        product_id, seller_id = random.choice(product_seller_pairs)

        price         = Decimal(str(round(random.uniform(20, 1000), 2)))
        freight_value = Decimal(str(round(random.uniform(5, 80), 2)))
        shipping_date = datetime.now() + timedelta(days=random.randint(1, 5))
        now           = datetime.now()

        items.append((
            order_id,
            item_num,
            product_id,
            seller_id,
            shipping_date,
            price,
            freight_value,
            now,
            now,
        ))

        total_value += price + freight_value

    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO order_items (
                order_id,
                order_item_id,
                product_id,
                seller_id,
                shipping_limit_date,
                price,
                freight_value,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            items,
        )

    return total_value


# ============================================================
# INSERT : Create one payment for an order
# ============================================================

def create_payment(conn, order_id, payment_value):
    payment_type = random.choice(["credit_card", "boleto", "voucher", "debit_card"])

    # credit card can have multiple installments, others are always 1
    if payment_type == "credit_card":
        installments = random.randint(1, 6)
    else:
        installments = 1

    now = datetime.now()

    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO order_payments (
                order_id,
                payment_sequential,
                payment_type,
                payment_installments,
                payment_value,
                created_at,
                updated_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                order_id,
                1,
                payment_type,
                installments,
                payment_value,
                now,
                now,
            ),
        )


# ============================================================
# UPDATE : Move some existing orders to the next status
# ============================================================

def update_existing_orders(conn, number_of_orders):
    # status flow: created → approved → processing → shipped → delivered
    next_status = {
        "created":    random.choice(["approved", "processing"]),
        "approved":   "processing",
        "processing": "shipped",
        "shipped":    "delivered",
    }

    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT order_id, order_status
            FROM orders
            WHERE order_status IN ('created', 'approved', 'processing', 'shipped')
            ORDER BY RANDOM()
            LIMIT %s
            """,
            (number_of_orders,),
        )
        orders = cursor.fetchall()

        for order_id, current_status in orders:
            new_status = next_status.get(current_status, current_status)
            now        = datetime.now()

            cursor.execute(
                """
                UPDATE orders
                SET order_status = %s,
                    updated_at   = %s
                WHERE order_id = %s
                """,
                (new_status, now, order_id),
            )

    return len(orders)


# ============================================================
# HELPER : Create one full order (order + items + payment)
# ============================================================

def generate_order(conn, customer_id, product_seller_pairs):
    order_id    = create_order(conn, customer_id)
    total_value = create_order_items(conn, order_id, product_seller_pairs)
    create_payment(conn, order_id, total_value)
    return order_id


# ============================================================
# MAIN : Run the generator
# ============================================================

def run_generator():

    # sanity check — config values must add up
    if EXISTING_CUSTOMER_ORDERS + NEW_CUSTOMER_ORDERS != ORDERS_PER_RUN:
        raise ValueError("existing_customer_orders + new_customer_orders must equal orders_per_run")

    conn = get_connection()

    try:
        # load reference data once
        existing_customers   = get_existing_customers(conn)
        locations            = get_locations(existing_customers)
        product_seller_pairs = get_product_seller_pairs(conn)

        if len(existing_customers) < EXISTING_CUSTOMER_ORDERS:
            raise ValueError("Not enough existing customers in database.")

        if not product_seller_pairs:
            raise ValueError("No valid product/seller pairs found.")

        generated_orders = []
        new_customer_ids = []

        # orders for existing customers
        selected_customers = random.sample(existing_customers, EXISTING_CUSTOMER_ORDERS)

        for customer in selected_customers:
            customer_id = customer[0]
            order_id    = generate_order(conn, customer_id, product_seller_pairs)
            generated_orders.append(order_id)

        # orders for new customers
        for _ in range(NEW_CUSTOMER_ORDERS):
            customer_id = create_customer(conn, locations)
            new_customer_ids.append(customer_id)
            order_id    = generate_order(conn, customer_id, product_seller_pairs)
            generated_orders.append(order_id)

        # move some existing orders forward in status
        updated_count = update_existing_orders(conn, number_of_orders=10)

        conn.commit()

        print("=" * 60)
        print("GENERATOR RUN COMPLETE")
        print("=" * 60)
        print(f"New customers created  : {len(new_customer_ids)}")
        print(f"New orders created     : {len(generated_orders)}")
        print(f"Existing orders updated: {updated_count}")
        print("=" * 60)

    except Exception:
        conn.rollback()
        raise

    finally:
        conn.close()


# ============================================================
# ENTRY POINT
# ============================================================

if __name__ == "__main__":
    run_generator()
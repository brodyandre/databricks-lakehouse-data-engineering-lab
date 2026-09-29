"""Validate generated synthetic retail datasets."""

from __future__ import annotations

import csv
from pathlib import Path

DATA_DIR = Path("data/sample")


def read_csv(name: str) -> list[dict[str, str]]:
    """Read a CSV dataset from the sample directory."""
    path = DATA_DIR / f"{name}.csv"

    with path.open(encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def main() -> None:
    """Validate record counts, relationships and intentional quality issues."""
    customers = read_csv("customers")
    products = read_csv("products")
    orders = read_csv("orders")
    order_items = read_csv("order_items")
    payments = read_csv("payments")
    shipments = read_csv("shipments")

    customer_ids = {row["customer_id"] for row in customers}
    product_ids = {row["product_id"] for row in products}
    order_ids = {row["order_id"] for row in orders}

    orphan_orders = [row for row in orders if row["customer_id"] not in customer_ids]

    orphan_order_items_orders = [row for row in order_items if row["order_id"] not in order_ids]

    orphan_order_items_products = [
        row for row in order_items if row["product_id"] not in product_ids
    ]

    orphan_payments = [row for row in payments if row["order_id"] not in order_ids]

    orphan_shipments = [row for row in shipments if row["order_id"] not in order_ids]

    blank_customer_emails = [row for row in customers if not row["email"].strip()]

    invalid_item_quantities = [row for row in order_items if int(row["quantity"]) <= 0]

    cancelled_order_ids = {row["order_id"] for row in orders if row["order_status"] == "cancelled"}

    shipment_order_ids = {row["order_id"] for row in shipments}

    cancelled_orders_with_shipments = cancelled_order_ids & shipment_order_ids

    print("=== DATASET COUNTS ===")
    print(f"customers: {len(customers):,}")
    print(f"products: {len(products):,}")
    print(f"orders: {len(orders):,}")
    print(f"order_items: {len(order_items):,}")
    print(f"payments: {len(payments):,}")
    print(f"shipments: {len(shipments):,}")

    print()
    print("=== REFERENTIAL INTEGRITY ===")
    print(f"orders without customer: {len(orphan_orders)}")
    print(f"order_items without order: {len(orphan_order_items_orders)}")
    print(f"order_items without product: {len(orphan_order_items_products)}")
    print(f"payments without order: {len(orphan_payments)}")
    print(f"shipments without order: {len(orphan_shipments)}")

    print()
    print("=== INTENTIONAL DATA QUALITY ISSUES ===")
    print(f"customers with blank email: {len(blank_customer_emails)}")
    print(f"order_items with quantity <= 0: {len(invalid_item_quantities)}")

    print()
    print("=== BUSINESS RULES ===")
    print(f"cancelled orders: {len(cancelled_order_ids)}")
    print(f"cancelled orders with shipment: {len(cancelled_orders_with_shipments)}")


if __name__ == "__main__":
    main()

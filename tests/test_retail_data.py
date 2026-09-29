"""Tests for the synthetic retail dataset."""

import csv
from pathlib import Path

DATA_DIR = Path("data/sample")


def read_csv(name: str) -> list[dict[str, str]]:
    """Read a sample CSV dataset."""
    path = DATA_DIR / f"{name}.csv"

    with path.open(encoding="utf-8", newline="") as file:
        return list(csv.DictReader(file))


def test_expected_dataset_counts() -> None:
    customers = read_csv("customers")
    products = read_csv("products")
    orders = read_csv("orders")
    order_items = read_csv("order_items")
    payments = read_csv("payments")
    shipments = read_csv("shipments")

    assert len(customers) == 500
    assert len(products) == 100
    assert len(orders) == 2_000
    assert len(order_items) == 5_930
    assert len(payments) == 2_000
    assert len(shipments) == 1_814


def test_orders_reference_existing_customers() -> None:
    customers = read_csv("customers")
    orders = read_csv("orders")

    customer_ids = {row["customer_id"] for row in customers}

    orphan_orders = [row for row in orders if row["customer_id"] not in customer_ids]

    assert orphan_orders == []


def test_order_items_reference_existing_orders_and_products() -> None:
    products = read_csv("products")
    orders = read_csv("orders")
    order_items = read_csv("order_items")

    product_ids = {row["product_id"] for row in products}
    order_ids = {row["order_id"] for row in orders}

    orphan_orders = [row for row in order_items if row["order_id"] not in order_ids]

    orphan_products = [row for row in order_items if row["product_id"] not in product_ids]

    assert orphan_orders == []
    assert orphan_products == []


def test_payments_reference_existing_orders() -> None:
    orders = read_csv("orders")
    payments = read_csv("payments")

    order_ids = {row["order_id"] for row in orders}

    orphan_payments = [row for row in payments if row["order_id"] not in order_ids]

    assert orphan_payments == []


def test_shipments_reference_existing_orders() -> None:
    orders = read_csv("orders")
    shipments = read_csv("shipments")

    order_ids = {row["order_id"] for row in orders}

    orphan_shipments = [row for row in shipments if row["order_id"] not in order_ids]

    assert orphan_shipments == []


def test_cancelled_orders_have_no_shipments() -> None:
    orders = read_csv("orders")
    shipments = read_csv("shipments")

    cancelled_order_ids = {row["order_id"] for row in orders if row["order_status"] == "cancelled"}

    shipment_order_ids = {row["order_id"] for row in shipments}

    assert cancelled_order_ids.isdisjoint(shipment_order_ids)


def test_intentional_customer_email_quality_issues_exist() -> None:
    customers = read_csv("customers")

    blank_emails = [row for row in customers if not row["email"].strip()]

    assert len(blank_emails) == 5


def test_intentional_order_item_quantity_quality_issues_exist() -> None:
    order_items = read_csv("order_items")

    invalid_quantities = [row for row in order_items if int(row["quantity"]) <= 0]

    assert len(invalid_quantities) == 59

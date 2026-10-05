"""Generate deterministic synthetic retail datasets for the Lakehouse lab."""

from __future__ import annotations

import argparse
import csv
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path

from faker import Faker

DEFAULT_SEED = 42
DEFAULT_CUSTOMERS = 500
DEFAULT_PRODUCTS = 100
DEFAULT_ORDERS = 2_000

ORDER_STATUSES = ("completed", "shipped", "processing", "cancelled")
PAYMENT_METHODS = ("credit_card", "pix", "debit_card", "bank_slip")
PRODUCT_CATEGORIES = (
    "electronics",
    "home",
    "books",
    "sports",
    "beauty",
    "fashion",
)

BRAZILIAN_STATES = (
    "SP",
    "RJ",
    "MG",
    "PR",
    "SC",
    "RS",
    "BA",
    "PE",
    "GO",
    "DF",
)


def write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    """Write rows to a CSV file."""
    if not rows:
        raise ValueError(f"No rows supplied for {path.name}")

    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def random_datetime(
    rng: random.Random,
    start: datetime,
    end: datetime,
) -> datetime:
    """Return a random UTC datetime between start and end."""
    delta_seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=rng.randint(0, delta_seconds))


def generate_customers(
    faker: Faker,
    rng: random.Random,
    count: int,
) -> list[dict[str, object]]:
    """Generate synthetic customers."""
    customers = []

    for index in range(1, count + 1):
        created_at = random_datetime(
            rng,
            datetime(2024, 1, 1, tzinfo=UTC),
            datetime(2026, 8, 31, tzinfo=UTC),
        )

        customers.append(
            {
                "customer_id": f"CUST-{index:06d}",
                "full_name": faker.name(),
                "email": faker.unique.email(),
                "state": rng.choice(BRAZILIAN_STATES),
                "created_at": created_at.isoformat(),
            }
        )

    return customers


def generate_products(
    faker: Faker,
    rng: random.Random,
    count: int,
) -> list[dict[str, object]]:
    """Generate synthetic products."""
    products = []

    for index in range(1, count + 1):
        products.append(
            {
                "product_id": f"PROD-{index:05d}",
                "product_name": faker.catch_phrase(),
                "category": rng.choice(PRODUCT_CATEGORIES),
                "unit_price": round(rng.uniform(10.0, 2_500.0), 2),
                "active": rng.random() > 0.05,
            }
        )

    return products


def generate_orders(
    rng: random.Random,
    customers: list[dict[str, object]],
    count: int,
) -> list[dict[str, object]]:
    """Generate synthetic customer orders."""
    orders = []

    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = datetime(2026, 9, 1, tzinfo=UTC)

    for index in range(1, count + 1):
        customer = rng.choice(customers)
        order_timestamp = random_datetime(rng, start, end)

        orders.append(
            {
                "order_id": f"ORD-{index:07d}",
                "customer_id": customer["customer_id"],
                "order_timestamp": order_timestamp.isoformat(),
                "order_status": rng.choices(
                    ORDER_STATUSES,
                    weights=(65, 15, 10, 10),
                    k=1,
                )[0],
            }
        )

    return orders


def generate_order_items(
    rng: random.Random,
    orders: list[dict[str, object]],
    products: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Generate line items for orders."""
    items = []
    item_id = 1

    for order in orders:
        item_count = rng.randint(1, 5)

        selected_products = rng.sample(
            products,
            k=min(item_count, len(products)),
        )

        for product in selected_products:
            quantity = rng.randint(1, 5)
            unit_price = float(product["unit_price"])

            items.append(
                {
                    "order_item_id": f"ITEM-{item_id:08d}",
                    "order_id": order["order_id"],
                    "product_id": product["product_id"],
                    "quantity": quantity,
                    "unit_price": unit_price,
                    "line_amount": round(quantity * unit_price, 2),
                }
            )

            item_id += 1

    return items


def generate_payments(
    rng: random.Random,
    orders: list[dict[str, object]],
    order_items: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Generate one payment record per order."""
    totals: dict[str, float] = {}

    for item in order_items:
        order_id = str(item["order_id"])
        totals[order_id] = totals.get(order_id, 0.0) + float(item["line_amount"])

    payments = []

    for index, order in enumerate(orders, start=1):
        order_id = str(order["order_id"])
        order_timestamp = datetime.fromisoformat(str(order["order_timestamp"]))

        payments.append(
            {
                "payment_id": f"PAY-{index:07d}",
                "order_id": order_id,
                "payment_method": rng.choice(PAYMENT_METHODS),
                "payment_amount": round(totals[order_id], 2),
                "payment_timestamp": (
                    order_timestamp + timedelta(minutes=rng.randint(1, 120))
                ).isoformat(),
            }
        )

    return payments


def generate_shipments(
    rng: random.Random,
    orders: list[dict[str, object]],
) -> list[dict[str, object]]:
    """Generate shipment records for eligible orders."""
    shipments = []
    shipment_id = 1

    for order in orders:
        if order["order_status"] == "cancelled":
            continue

        order_timestamp = datetime.fromisoformat(str(order["order_timestamp"]))
        shipped_at = order_timestamp + timedelta(hours=rng.randint(4, 72))
        delivered_at = shipped_at + timedelta(days=rng.randint(1, 10))

        shipments.append(
            {
                "shipment_id": f"SHIP-{shipment_id:07d}",
                "order_id": order["order_id"],
                "shipped_at": shipped_at.isoformat(),
                "delivered_at": delivered_at.isoformat(),
                "delivery_status": "delivered",
            }
        )

        shipment_id += 1

    return shipments


def inject_quality_issues(
    rng: random.Random,
    customers: list[dict[str, object]],
    order_items: list[dict[str, object]],
) -> None:
    """Inject deterministic data quality issues for later pipeline validation."""
    customer_issue_count = max(1, len(customers) // 100)
    item_issue_count = max(1, len(order_items) // 100)

    for customer in rng.sample(customers, k=customer_issue_count):
        customer["email"] = ""

    for item in rng.sample(order_items, k=item_issue_count):
        item["quantity"] = 0
        item["line_amount"] = 0.0


def generate_dataset(
    output_dir: Path,
    seed: int,
    customer_count: int,
    product_count: int,
    order_count: int,
) -> dict[str, int]:
    """Generate the complete synthetic retail dataset."""
    rng = random.Random(seed)

    faker = Faker("pt_BR")
    Faker.seed(seed)
    faker.seed_instance(seed)

    customers = generate_customers(faker, rng, customer_count)
    products = generate_products(faker, rng, product_count)
    orders = generate_orders(rng, customers, order_count)
    order_items = generate_order_items(rng, orders, products)
    payments = generate_payments(rng, orders, order_items)
    shipments = generate_shipments(rng, orders)

    inject_quality_issues(rng, customers, order_items)

    datasets = {
        "customers": customers,
        "products": products,
        "orders": orders,
        "order_items": order_items,
        "payments": payments,
        "shipments": shipments,
    }

    for name, rows in datasets.items():
        write_csv(output_dir / f"{name}.csv", rows)

    return {name: len(rows) for name, rows in datasets.items()}


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(
        description="Generate synthetic retail data for the Databricks Lakehouse lab."
    )

    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("data/sample"),
    )
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--customers", type=int, default=DEFAULT_CUSTOMERS)
    parser.add_argument("--products", type=int, default=DEFAULT_PRODUCTS)
    parser.add_argument("--orders", type=int, default=DEFAULT_ORDERS)

    return parser.parse_args()


def main() -> None:
    """Run dataset generation."""
    args = parse_args()

    counts = generate_dataset(
        output_dir=args.output_dir,
        seed=args.seed,
        customer_count=args.customers,
        product_count=args.products,
        order_count=args.orders,
    )

    print("Synthetic retail dataset generated successfully.")

    for dataset, count in counts.items():
        print(f"{dataset}: {count:,} rows")


if __name__ == "__main__":
    main()

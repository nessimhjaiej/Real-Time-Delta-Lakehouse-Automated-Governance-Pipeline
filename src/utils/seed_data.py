import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

OUTPUT_PATH = Path("data/raw_transactions/orders_batch.csv")


def generate_mock_orders(num_rows: int = 10000) -> None:
    """Generates a large dataset of realistic batch orders on local disk."""
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    products = [(1, 25.50), (2, 120.00), (3, 15.99), (4, 45.00), (5, 8.99)]
    start_date = datetime(2026, 9, 1)

    with open(OUTPUT_PATH, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "invoice_id",
                "customer_id",
                "customer_name",
                "email",
                "ip_address",
                "product_id",
                "quantity",
                "unit_price",
                "timestamp",
            ]
        )

        for i in range(1000, 1000 + num_rows):
            prod_id, price = random.choice(products)
            dt = start_date + timedelta(minutes=random.randint(0, 12000))
            cust_num = random.randint(100, 999)
            writer.writerow(
                [
                    f"INV-{i}",
                    f"CUST-{cust_num}",
                    f"User_{cust_num}",
                    f"user_{cust_num}@example.com",
                    f"192.168.{random.randint(1, 255)}.{random.randint(1, 255)}",
                    prod_id,
                    random.randint(1, 5),
                    price,
                    dt.isoformat() + "Z",
                ]
            )

    print(f"Successfully generated {num_rows} orders in {OUTPUT_PATH}")


if __name__ == "__main__":
    generate_mock_orders(10000)

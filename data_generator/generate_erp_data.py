"""
Synthetic ERP Data Generator
Generates sample orders, suppliers, and inventory records.
Intentionally injects invalid records to test dynamic data quality rules and quarantine routing.
"""

import json
import os
import random
from datetime import datetime, timedelta

def generate_sample_erp_data(output_dir="data/sample_erp"):
    os.makedirs(output_dir, exist_ok=True)
    random.seed(42)

    orders = []
    base_date = datetime(2024, 1, 1)

    for i in range(1, 201):
        is_corrupt = random.random() < 0.08  # 8% dirty records
        order_date = base_date + timedelta(days=random.randint(0, 90))

        if is_corrupt and random.random() < 0.5:
            order_id = None  # Violation: Null primary key
        else:
            order_id = f"ORD-{10000 + i}"

        if is_corrupt and random.random() >= 0.5:
            total_amount = -1 * round(random.uniform(50.0, 500.0), 2)  # Violation: Negative amount
        else:
            total_amount = round(random.uniform(50.0, 2500.0), 2)

        order = {
            "OrderID": order_id,
            "CustomerCode": f"CUST-{random.randint(100, 150)}",
            "SupplierID": f"SUP-{random.randint(1, 10)}",
            "OrderDate": order_date.strftime("%Y-%m-%d"),
            "TotalAmount": total_amount,
            "OrderStatus": random.choice(["Delivered", "Shipped", "Pending", "Cancelled"]),
            "Region": random.choice(["North America", "Europe", "Asia Pacific", "Latin America"])
        }
        orders.append(order)

    # Save as JSON (can be read by spark directly or converted)
    orders_file = os.path.join(output_dir, "orders.json")
    with open(orders_file, "w") as f:
        json.dump(orders, f, indent=2)

    print(f"Generated {len(orders)} sample ERP orders (including deliberate quality defects) at: {orders_file}")
    return orders_file

if __name__ == "__main__":
    generate_sample_erp_data()

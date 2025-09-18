# src/generate_data.py

import random
from datetime import datetime, timedelta
from pathlib import Path
import pandas as pd

# Define output folder
RAW = Path(__file__).resolve().parents[1] / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

def make_sales(start_date: str, days: int, rows_per_day: int) -> pd.DataFrame:
    categories = [
        ("Electronics", ["Phones", "Laptops", "Accessories"]),
        ("Apparel", ["T-Shirts", "Jeans", "Shoes"]),
        ("Home", ["Furniture", "Kitchen", "Decor"]),
        ("Grocery", ["Snacks", "Beverages", "Produce"]),
    ]
    regions = ["North", "South", "East", "West"]
    rows = []
    current_date = datetime.strptime(start_date, "%Y-%m-%d")

    for _ in range(days):
        for _ in range(rows_per_day):
            category, subcategories = random.choice(categories)
            subcategory = random.choice(subcategories)
            region = random.choice(regions)
            customer_id = random.randint(1, 200)
            product_id = random.randint(1000, 1999)
            price = round(random.uniform(5, 1200), 2)
            quantity = random.randint(1, 5)
            discount = random.choice([0, 0, 0, 0.05, 0.1, 0.15])
            order_id = f"O{current_date.strftime('%Y%m%d')}{random.randint(10000, 99999)}"

            rows.append({
                "order_id": order_id,
                "order_date": current_date.strftime("%Y-%m-%d"),
                "customer_id": customer_id,
                "product_id": product_id,
                "category": category,
                "subcategory": subcategory,
                "region": region,
                "price": price,
                "quantity": quantity,
                "discount": discount,
            })
        current_date += timedelta(days=1)

    return pd.DataFrame(rows)

if __name__ == "__main__":
    df_q1 = make_sales("2024-01-01", days=45, rows_per_day=8)  # ~360 rows
    df_q2 = make_sales("2024-02-15", days=45, rows_per_day=8)  # ~360 rows

    df_q1.to_csv(RAW / "sales_2024_q1.csv", index=False)
    df_q2.to_csv(RAW / "sales_2024_q2.csv", index=False)

    print("✅ Sales data generated:")
    print(f"- {len(df_q1)} rows in sales_2024_q1.csv")
    print(f"- {len(df_q2)} rows in sales_2024_q2.csv")

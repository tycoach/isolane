"""
examples/ecommerce/seed.py

End-to-end seed script for the isolane e-commerce example.

Generates realistic data for three brands (brand_a, brand_b, brand_c)
and pushes it to the isolane Redis Streams work queues.

Each brand gets:
  - 200 customers  (~5% duplicates to trigger quarantine)
  - 500 orders     (~3% null customer_id to trigger quarantine)
  - 100 products   (clean data — no intentional errors)
"""

import argparse
import json
import os
import random
import string
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

import redis


# ── Brand data ────────────────────────────────────────────────────

BRANDS = {
    "brand_a": {
        "name":       "Apex Store",
        "categories": ["Electronics", "Gaming", "Accessories"],
        "cities":     ["Lagos", "Abuja", "Port Harcourt", "Kano", "Ibadan"],
        "tiers":      ["bronze", "silver", "gold", "platinum"],
    },
    "brand_b": {
        "name":       "Blue Label",
        "categories": ["Fashion", "Footwear", "Bags", "Jewellery"],
        "cities":     ["London", "Manchester", "Birmingham", "Leeds", "Glasgow"],
        "tiers":      ["standard", "premium", "vip"],
    },
    "brand_c": {
        "name":       "Cedar Market",
        "categories": ["Groceries", "Beverages", "Household", "Fresh Produce"],
        "cities":     ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix"],
        "tiers":      ["basic", "plus", "family"],
    },
}

ORDER_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled", "returned"]
PROMO_CODES    = ["SAVE10", "WELCOME20", "FLASH50", None, None, None]  # None = no promo


# ── Generators ────────────────────────────────────────────────────

def random_email(brand: str, index: int) -> str:
    domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com"]
    username = f"user{index}_{brand[:2]}"
    return f"{username}@{random.choice(domains)}"


def random_phone() -> Optional[str]:
    if random.random() < 0.15:  # 15% have no phone
        return None
    return f"+{random.randint(1, 99)}{random.randint(1000000000, 9999999999)}"


def random_date(days_back: int = 365) -> str:
    delta = timedelta(days=random.randint(0, days_back))
    dt    = datetime.now(timezone.utc) - delta
    return dt.isoformat()


def generate_customers(brand: str, count: int, duplicate_rate: float = 0.05) -> list[dict]:
    """
    Generate customer records with realistic duplicates.
    duplicate_rate controls what fraction of records are duplicate customer_ids.
    """
    brand_cfg = BRANDS[brand]
    base_ids  = [f"CUST-{brand[:2].upper()}-{i:05d}" for i in range(1, count + 1)]
    records   = []

    for i, cid in enumerate(base_ids):
        record = {
            "customer_id":  cid,
            "email":        random_email(brand, i),
            "city":         random.choice(brand_cfg["cities"]),
            "phone":        random_phone(),
            "loyalty_tier": random.choice(brand_cfg["tiers"] + [None]),
            "signup_date":  random_date(730),
            "brand":        brand_cfg["name"],
        }
        records.append(record)

    # Inject duplicates — same customer_id, different email
    duplicate_count = int(count * duplicate_rate)
    for _ in range(duplicate_count):
        original = random.choice(records[:count // 2])
        duplicate = dict(original)
        duplicate["email"] = f"dup_{duplicate['email']}"
        records.append(duplicate)

    random.shuffle(records)
    return records


def generate_orders(
    brand: str,
    count: int,
    customer_ids: list[str],
    null_customer_rate: float = 0.03,
) -> list[dict]:
    """
    Generate order records.
    null_customer_rate controls orphaned orders (no customer_id).
    """
    brand_cfg = BRANDS[brand]
    records   = []

    for i in range(count):
        # Inject orphaned orders
        if random.random() < null_customer_rate:
            customer_id = None
        else:
            customer_id = random.choice(customer_ids)

        amount   = round(random.uniform(5.0, 2000.0), 2)
        promo    = random.choice(PROMO_CODES)
        discount = round(amount * random.uniform(0.05, 0.20), 2) if promo else None

        record = {
            "order_id":        f"ORD-{brand[:2].upper()}-{i:06d}",
            "customer_id":     customer_id,
            "total_amount":    amount,
            "discount_amount": discount,
            "promo_code":      promo,
            "status":          random.choice(ORDER_STATUSES),
            "category":        random.choice(brand_cfg["categories"]),
            "created_at":      random_date(90),
            "brand":           BRANDS[brand]["name"],
        }
        records.append(record)

    return records


def generate_products(brand: str, count: int) -> list[dict]:
    """Generate product catalogue records — intentionally clean data."""
    brand_cfg = BRANDS[brand]
    records   = []

    for i in range(count):
        price = round(random.uniform(1.0, 500.0), 2)
        record = {
            "product_id":   f"PROD-{brand[:2].upper()}-{i:04d}",
            "name":         f"{brand_cfg['name']} Product {i:04d}",
            "category":     random.choice(brand_cfg["categories"]),
            "price":        price,
            "stock_count":  random.randint(0, 1000),
            "description":  f"Product description for item {i}" if random.random() > 0.1 else None,
            "image_url":    f"https://cdn.{brand}.com/products/{i}.jpg" if random.random() > 0.05 else None,
            "created_at":   random_date(180),
        }
        records.append(record)

    return records


# ── Redis push ────────────────────────────────────────────────────

def get_redis() -> redis.Redis:
    return redis.Redis(
        host             = os.environ.get("REDIS_HOST", "localhost"),
        port             = int(os.environ.get("REDIS_PORT", 6379)),
        decode_responses = True,
    )


def push_batch(
    r:           redis.Redis,
    namespace:   str,
    pipeline_id: str,
    records:     list[dict],
    batch_size:  int = 100,
    dry_run:     bool = False,
) -> int:
    """
    Push records to the namespace work queue in batches.
    Returns the number of batches pushed.
    """
    stream_key   = f"{namespace}.work-queue"
    batches      = [records[i:i+batch_size] for i in range(0, len(records), batch_size)]
    batches_sent = 0

    for batch_idx, batch in enumerate(batches):
        batch_offset = f"ecommerce-{namespace}-{pipeline_id}-batch-{batch_idx:04d}"
        fields = {
            "pipeline_id":   pipeline_id,
            "batch_offset":  batch_offset,
            "records_json":  json.dumps(batch),
            "_claim_count":  "0",
        }

        if dry_run:
            print(
                f"  [dry-run] Would push {len(batch)} {pipeline_id} records "
                f"to {stream_key} (batch {batch_idx + 1}/{len(batches)})"
            )
        else:
            r.xadd(stream_key, fields)
            print(
                f"  [seed] {namespace}.{pipeline_id} | "
                f"batch {batch_idx + 1}/{len(batches)} | "
                f"{len(batch)} records → {stream_key}"
            )

        batches_sent += 1

    return batches_sent


# ── Main ─────────────────────────────────────────────────────────

def seed_brand(
    r:          redis.Redis,
    brand:      str,
    n_customers: int,
    n_orders:    int,
    n_products:  int,
    dry_run:     bool,
) -> dict:
    print(f"\n{'='*60}")
    print(f"  Seeding brand: {brand} ({BRANDS[brand]['name']})")
    print(f"{'='*60}")

    # Generate data
    customers = generate_customers(brand, n_customers)
    customer_ids = [
        c["customer_id"] for c in customers if c["customer_id"] is not None
    ]
    orders   = generate_orders(brand, n_orders, customer_ids)
    products = generate_products(brand, n_products)

    print(f"\n  Generated:")
    print(f"    {len(customers)} customer records ({int(n_customers * 0.05)} duplicates)")
    print(f"    {len(orders)} order records ({int(n_orders * 0.03)} orphaned)")
    print(f"    {len(products)} product records (clean)")

    if not dry_run:
        # Verify stream exists
        try:
            r.xlen(f"{brand}.work-queue")
        except redis.exceptions.ResponseError:
            print(f"\n  ⚠️  Stream {brand}.work-queue not found.")
            print(f"     Run: make tenant NS={brand} TEAM={brand}-team")
            return {}

    print(f"\n  Pushing to Redis Streams:")
    results = {}

    for pipeline_id, records in [
        ("customers", customers),
        ("orders",    orders),
        ("products",  products),
    ]:
        batches = push_batch(r, brand, pipeline_id, records, dry_run=dry_run)
        results[pipeline_id] = {
            "records": len(records),
            "batches": batches,
        }

    return results


def main():
    parser = argparse.ArgumentParser(
        description="Seed isolane with e-commerce example data"
    )
    parser.add_argument(
        "--brand",
        choices=["brand_a", "brand_b", "brand_c", "all"],
        default="all",
        help="Which brand to seed (default: all)",
    )
    parser.add_argument("--customers", type=int, default=200,
                        help="Number of customer records per brand (default: 200)")
    parser.add_argument("--orders",    type=int, default=500,
                        help="Number of order records per brand (default: 500)")
    parser.add_argument("--products",  type=int, default=100,
                        help="Number of product records per brand (default: 100)")
    parser.add_argument("--dry-run",   action="store_true",
                        help="Show what would be pushed without pushing")
    args = parser.parse_args()

    brands = (
        list(BRANDS.keys()) if args.brand == "all"
        else [args.brand]
    )

    print(f"\nisolane e-commerce example seed")
    print(f"{'─'*40}")
    print(f"  Brands:    {', '.join(brands)}")
    print(f"  Customers: {args.customers} per brand")
    print(f"  Orders:    {args.orders} per brand")
    print(f"  Products:  {args.products} per brand")
    print(f"  Dry run:   {args.dry_run}")

    r = get_redis() if not args.dry_run else None

    total_records = 0
    total_batches = 0

    for brand in brands:
        results = seed_brand(
            r            = r,
            brand        = brand,
            n_customers  = args.customers,
            n_orders     = args.orders,
            n_products   = args.products,
            dry_run      = args.dry_run,
        )
        for pipeline, stats in results.items():
            total_records += stats.get("records", 0)
            total_batches += stats.get("batches", 0)

    print(f"\n{'='*60}")
    print(f"  Seed complete")
    print(f"  Total records pushed: {total_records}")
    print(f"  Total batches pushed: {total_batches}")
    print(f"\n  Watch workers process:")
    print(f"    docker compose logs go-worker -f")
    print(f"\n  Check results in DB:")
    print(f"    make shell-db")
    for brand in brands:
        print(f"    SELECT COUNT(*) FROM {brand}_active.customers;")
    print(f"\n  Open Grafana:")
    print(f"    http://localhost:3000")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
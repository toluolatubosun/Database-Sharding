"""
Seed dummy data across the cluster. Always starts from a clean slate.

  - 50 products on global_db
  - 100 users spread across shards via the consistent hash ring
  - 250 reviews distributed evenly (5 per product, 2-3 per user)

Usage:
    python scripts/seed.py
"""

import os
import random
import string
import sys
import uuid
from decimal import Decimal

from sqlalchemy import create_engine, delete, func
from sqlalchemy.engine import Engine
from sqlmodel import Session, select

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from configs.config import CONFIGS
from sharding.ring import ConsistentHashRing
from models.user import User
from models.product import Product
from models.review import Review
from libraries.redis import redis_client


NUM_PRODUCTS = 50
NUM_USERS = 100
NUM_REVIEWS = 250
RANDOM_SEED = 42


FIRST_NAMES = [
    "Ada", "Bob", "Cat", "Dan", "Eve", "Fox", "Gus", "Hal", "Ivy", "Jay",
    "Kit", "Lou", "Max", "Nia", "Ona", "Pia", "Quin", "Ray", "Sam", "Tia",
    "Uma", "Val", "Wes", "Xan", "Yon", "Zia",
]

LAST_NAMES = [
    "Smith", "Jones", "Brown", "Davis", "Wilson", "Miller", "Taylor",
    "Anderson", "Thomas", "Jackson", "White", "Harris", "Martin", "Garcia",
    "Lewis", "Walker", "Hall", "Allen", "Young", "King", "Wright",
]

COLORS = [
    "Burgundy", "Navy", "Olive", "Charcoal", "Ivory", "Sienna",
    "Teal", "Crimson", "Slate", "Amber",
]

ITEMS = [
    "Shirt", "Jacket", "Hoodie", "Cap", "Backpack",
    "Mug", "Bottle", "Tote", "Notebook", "Wallet",
    "Socks", "Scarf", "Gloves", "Belt", "Shoes",
]

SIZES = ["XS", "S", "M", "L", "XL", "XXL"]

DESCRIPTION_TEMPLATES = [
    "A premium {item} in classic {color}, available in size {size}.",
    "Our best-selling {color} {item}, sized {size} for everyday wear.",
    "Handcrafted {color} {item} ({size}) -- built to last.",
    "The {color} {item} you've been looking for. Size {size}, ready to ship.",
    "Limited edition {color} {item} in {size}.",
]

REVIEW_TITLES = [
    "Love it", "Great purchase", "Not bad", "Could be better", "Five stars",
    "Solid", "Meets expectations", "Highly recommend", "Has its flaws",
]

REVIEW_TEMPLATES = [
    "The {color} is exactly what I wanted. Size {size} fits perfectly.",
    "Solid {item}. The {color} is more vibrant in person.",
    "I bought the {size} and it's a bit tight. Otherwise great.",
    "Quality {item}. The {color} is gorgeous.",
    "Wouldn't recommend the {size} -- runs small. Loving the {color} though.",
    "Five stars for this {color} {item}.",
    "Decent {item} for the price. {color} is true to the photo.",
]


def list_engines() -> dict[str, Engine]:
    engines = {name: create_engine(url) for name, url in CONFIGS["SHARD_URLS"].items()}
    engines["global"] = create_engine(CONFIGS["GLOBAL_DB_URL"])
    return engines


def make_ring() -> ConsistentHashRing:
    ring = ConsistentHashRing()
    for shard_name in CONFIGS["SHARD_URLS"]:
        ring.add_node(shard_name)
    return ring


def reset(engines: dict[str, Engine]) -> None:
    print("Resetting...")
    for name, engine in engines.items():
        with Session(engine) as session:
            if name == "global":
                session.execute(delete(Product))
            else:
                session.execute(delete(Review))
                session.execute(delete(User))
            session.commit()

    flushed = 0
    for key in redis_client.scan_iter("email:*"):
        redis_client.delete(key)
        flushed += 1
    print(f"  {flushed} Redis email keys flushed.")


def seed_products(global_engine: Engine) -> dict[uuid.UUID, dict]:
    """Insert products and return a {product_id -> spec} map for later use."""
    print(f"\nSeeding {NUM_PRODUCTS} products on global...")
    products = []
    specs = []
    for _ in range(NUM_PRODUCTS):
        spec = {
            "color": random.choice(COLORS),
            "item": random.choice(ITEMS),
            "size": random.choice(SIZES),
        }
        products.append(Product(
            name=f"{spec['color']} {spec['item']} {spec['size']}",
            price=Decimal(random.randint(199, 9999)) / 100,
            description=random.choice(DESCRIPTION_TEMPLATES).format(**spec),
        ))
        specs.append(spec)

    with Session(global_engine) as session:
        session.add_all(products)
        session.commit()
        return {p.id: s for p, s in zip(products, specs)}


def seed_users(engines: dict[str, Engine], ring: ConsistentHashRing) -> list[tuple[uuid.UUID, str]]:
    print(f"\nSeeding {NUM_USERS} users across shards...")
    per_shard: dict[str, list[User]] = {name: [] for name in CONFIGS["SHARD_URLS"]}
    user_to_shard: list[tuple[uuid.UUID, str]] = []

    for _ in range(NUM_USERS):
        user_id = uuid.uuid4()
        first = random.choice(FIRST_NAMES)
        last = random.choice(LAST_NAMES)
        suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=4))
        email = f"{first.lower()}.{last.lower()}.{suffix}@gmail.com"
        shard = ring.get_node(str(user_id))

        per_shard[shard].append(User(id=user_id, name=f"{first} {last}", email=email))
        user_to_shard.append((user_id, shard))
        redis_client.set(f"email:{email}", str(user_id), nx=True)

    for shard, users in per_shard.items():
        with Session(engines[shard]) as session:
            session.add_all(users)
            session.commit()
        print(f"  {shard}: {len(users)} users")

    return user_to_shard


def seed_reviews(
    engines: dict[str, Engine],
    user_to_shard: list[tuple[uuid.UUID, str]],
    spec_by_product: dict[uuid.UUID, dict],
) -> None:
    print(f"\nSeeding {NUM_REVIEWS} reviews (evenly across products and users)...")
    product_ids = list(spec_by_product.keys())

    # Each product appears exactly NUM_REVIEWS/NUM_PRODUCTS times.
    product_slots = product_ids * (NUM_REVIEWS // NUM_PRODUCTS)

    # Each user appears the same baseline number of times, with the
    # remainder distributed randomly.
    user_slots = (
        user_to_shard * (NUM_REVIEWS // NUM_USERS)
        + random.sample(user_to_shard, NUM_REVIEWS % NUM_USERS)
    )

    random.shuffle(product_slots)
    random.shuffle(user_slots)

    per_shard: dict[str, list[Review]] = {name: [] for name in CONFIGS["SHARD_URLS"]}
    for product_id, (user_id, shard) in zip(product_slots, user_slots):
        spec = spec_by_product[product_id]
        per_shard[shard].append(Review(
            user_id=user_id,
            product_id=product_id,
            title=random.choice(REVIEW_TITLES),
            content=random.choice(REVIEW_TEMPLATES).format(**spec),
            rating=random.randint(1, 5),
        ))

    for shard, reviews in per_shard.items():
        with Session(engines[shard]) as session:
            session.add_all(reviews)
            session.commit()
        print(f"  {shard}: {len(reviews)} reviews")


def print_summary(engines: dict[str, Engine]) -> None:
    print("\nRow counts:")
    for name, engine in engines.items():
        with Session(engine) as session:
            if name == "global":
                products = session.scalar(select(func.count(Product.id)))
                print(f"  {name:10} products={products}")
            else:
                users = session.scalar(select(func.count(User.id)))
                reviews = session.scalar(select(func.count(Review.id)))
                print(f"  {name:10} users={users}   reviews={reviews}")


def main():
    random.seed(RANDOM_SEED)
    engines = list_engines()
    ring = make_ring()

    reset(engines)
    spec_by_product = seed_products(engines["global"])
    user_to_shard = seed_users(engines, ring)
    seed_reviews(engines, user_to_shard, spec_by_product)

    print_summary(engines)


if __name__ == "__main__":
    main()

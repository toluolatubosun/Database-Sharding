"""
Cleanup script for after the CUTOVER phase.

Once CUTOVER is active, _ring includes the new shard, and some rows on the
old shards no longer route to them (they live on the new shard now). This
script deletes those stale rows so old shards stop returning duplicates on
fan-out reads.

Steps:
  1. Delete reviews whose user_id no longer routes to the shard they're on.
  2. Delete users whose id no longer routes to the shard they're on.

Order matters: reviews first, then users (FK constraint).
Idempotent, safe to re-run; DELETE on an already-gone row is a no-op.

Usage:
    python scripts/resharding_cutover.py
"""

import os
import sys

from sqlalchemy import delete
from sqlmodel import select

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from configs.config import CONFIGS
from database import router as shard_router
from models.user import User
from models.review import Review


BATCH_SIZE = 500


def main():
    reshard = CONFIGS["DB_RESHARDING"]
    operation = reshard["OPERATION"]
    phase = reshard["DB_RESHARDING_PHASE"]
    if operation != "ADD" or phase != "CUTOVER":
        print(
            f"ABORT: cleanup must run with OPERATION=ADD and DB_RESHARDING_PHASE=CUTOVER "
            f"(got OPERATION={operation!r}, PHASE={phase!r}).",
            file=sys.stderr,
        )
        sys.exit(2)

    all_shards = shard_router.all_shards()  # includes the new shard during CUTOVER
    print(f"Cleaning up stale rows across {all_shards}\n")

    # Step 1 -- reviews first, they FK to users
    print("[1/2] Deleting stale reviews...")
    total_reviews = 0
    for shard in all_shards:
        total_reviews += _delete_stale_reviews(shard)
    print(f":::> total: {total_reviews} reviews\n")

    # Step 2 -- users second, now safe with no FK references
    print("[2/2] Deleting stale users...")
    total_users = 0
    for shard in all_shards:
        total_users += _delete_stale_users(shard)
    print(f":::> total: {total_users} users\n")

    print("Cleanup complete. Promote the new shard into SHARD_URLS and flip MIGRATION_PHASE to IDLE.")


def _belongs_on(key, shard: str) -> bool:
    """A row belongs on `shard` if its key routes there under the current ring."""
    return shard_router.shard_for(str(key), "READ") == shard


def _delete_stale_users(shard: str) -> int:
    """Scan users on `shard`; delete those whose id no longer routes here."""
    scanned = 0
    deleted = 0
    last_id = None
    while True:
        with shard_router.session(shard) as session:
            stmt = select(User).order_by(User.id).limit(BATCH_SIZE)
            if last_id is not None:
                stmt = stmt.where(User.id > last_id)
            batch = list(session.exec(stmt).all())

        if not batch:
            break
        scanned += len(batch)

        stale_ids = [user.id for user in batch if not _belongs_on(user.id, shard)]
        if stale_ids:
            with shard_router.session(shard) as session:
                session.execute(delete(User).where(User.id.in_(stale_ids)))
                session.commit()
            deleted += len(stale_ids)

        last_id = batch[-1].id
        print(f"\r :::> {shard}  scanned={scanned} deleted={deleted}  ", end="", flush=True)

    print(f"\r :::> {shard}  scanned={scanned} deleted={deleted}")
    return deleted


def _delete_stale_reviews(shard: str) -> int:
    """Scan reviews on `shard`; delete those whose user_id no longer routes here."""
    scanned = 0
    deleted = 0
    last_id = None
    while True:
        with shard_router.session(shard) as session:
            stmt = select(Review).order_by(Review.id).limit(BATCH_SIZE)
            if last_id is not None:
                stmt = stmt.where(Review.id > last_id)
            batch = list(session.exec(stmt).all())

        if not batch:
            break
        scanned += len(batch)

        stale_ids = [review.id for review in batch if not _belongs_on(review.user_id, shard)]
        if stale_ids:
            with shard_router.session(shard) as session:
                session.execute(delete(Review).where(Review.id.in_(stale_ids)))
                session.commit()
            deleted += len(stale_ids)

        last_id = batch[-1].id
        print(f"\r :::> {shard}  scanned={scanned} deleted={deleted}  ", end="", flush=True)

    print(f"\r :::> {shard}  scanned={scanned} deleted={deleted}")
    return deleted


if __name__ == "__main__":
    main()

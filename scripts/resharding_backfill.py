"""
Backfill historical data to the new shard during DUAL_WRITE.

Steps:
  1. Copy every user from old shards whose user_id now routes to the new shard.
  2. Copy every review for those users (must run after users for FK ordering).

Schema migration on the new shard is a separate prior step
run `scripts/migrate.py` while MIGRATION_PHASE=ALLOW_MIGRATIONS before flipping to DUAL_WRITE.

Idempotent, safe to re-run if interrupted.
Uses ON CONFLICT DO NOTHING so rows already mirrored by live dual-writes are left untouched.

Usage:
    python scripts/resharding_backfill.py
"""

import os
import sys

from sqlalchemy.dialects.postgresql import insert as pg_insert
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
    if operation != "ADD" or phase != "DUAL_WRITE":
        print(
            f"ABORT: backfill must run with OPERATION=ADD and DB_RESHARDING_PHASE=DUAL_WRITE "
            f"(got OPERATION={operation!r}, PHASE={phase!r}).",
            file=sys.stderr,
        )
        sys.exit(2)

    new_shard = reshard["TARGET_SHARD_NAME"]
    source_shards = shard_router.all_shards()  # excludes the new shard during DUAL_WRITE

    print(f"Backfilling {new_shard} from {source_shards}\n")

    # Step 1 -- users first, reviews FK them
    print("[1/2] Copying moving users...")
    total_users = 0
    for source in source_shards:
        total_users += _copy_moving_users(source, new_shard)
    print(f":::> total: {total_users} users\n")

    # Step 2 -- reviews for the users we just copied
    print("[2/2] Copying reviews for moving users...")
    total_reviews = 0
    for source in source_shards:
        total_reviews += _copy_moving_reviews(source, new_shard)
    print(f":::> total: {total_reviews} reviews\n")

    print("Backfill complete. Flip MIGRATION_PHASE to CUTOVER once you're ready.")


def _is_moving_to(key, target_shard: str) -> bool:
    """A key is moving to target_shard iff shard_for(..., "WRITE") returns [current, target]."""
    shards = shard_router.shard_for(str(key), "WRITE")
    return len(shards) == 2 and shards[1] == target_shard


def _copy_moving_users(source_shard: str, target_shard: str) -> int:
    """Stream users from source in batches; copy the ones now routing to target."""
    scanned = 0
    copied = 0
    last_id = None
    while True:
        with shard_router.session(source_shard) as src:
            stmt = select(User).order_by(User.id).limit(BATCH_SIZE)
            if last_id is not None:
                stmt = stmt.where(User.id > last_id)
            batch = list(src.exec(stmt).all())

        if not batch:
            break
        scanned += len(batch)

        moving = [user for user in batch if _is_moving_to(user.id, target_shard)]
        if moving:
            with shard_router.session(target_shard) as tgt:
                for user in moving:
                    stmt = pg_insert(User).values(**user.model_dump()).on_conflict_do_nothing(index_elements=["id"])
                    tgt.execute(stmt)
                tgt.commit()
            copied += len(moving)

        last_id = batch[-1].id
        print(f"\r :::> {source_shard} -> {target_shard}  scanned={scanned} copied={copied}  ", end="", flush=True)

    print(f"\r :::> {source_shard} -> {target_shard}  scanned={scanned} copied={copied}")
    return copied


def _copy_moving_reviews(source_shard: str, target_shard: str) -> int:
    """Stream reviews from source in batches; copy the ones whose user is moving to target."""
    scanned = 0
    copied = 0
    last_id = None
    while True:
        with shard_router.session(source_shard) as src:
            stmt = select(Review).order_by(Review.id).limit(BATCH_SIZE)
            if last_id is not None:
                stmt = stmt.where(Review.id > last_id)
            batch = list(src.exec(stmt).all())

        if not batch:
            break
        scanned += len(batch)

        moving = [review for review in batch if _is_moving_to(review.user_id, target_shard)]
        if moving:
            with shard_router.session(target_shard) as tgt:
                for review in moving:
                    stmt = pg_insert(Review).values(**review.model_dump()).on_conflict_do_nothing(index_elements=["id"])
                    tgt.execute(stmt)
                tgt.commit()
            copied += len(moving)

        last_id = batch[-1].id
        print(f"\r :::> {source_shard} -> {target_shard}  scanned={scanned} copied={copied}  ", end="", flush=True)

    print(f"\r :::> {source_shard} -> {target_shard}  scanned={scanned} copied={copied}")
    return copied


if __name__ == "__main__":
    main()

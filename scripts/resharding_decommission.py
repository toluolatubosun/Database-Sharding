"""
Drain a doomed shard during the REMOVE / DUAL_WRITE phase.

Mirror of resharding_backfill.py but reversed in shape:
  - backfill : many sources  -> one destination (the incoming shard)
  - drain    : one source     -> many destinations (the rest of the cluster)

Every row on the doomed shard moves -- consistent hashing redistributes the
doomed shard's slice across the remaining shards. shard_for(key, "WRITE")
returns [doomed, destination] for each row, so we use the second entry as
the new home.

Steps:
  1. Copy every user off the doomed shard to its new home.
  2. Copy every review for those users (after users, FK ordering).

Idempotent -- ON CONFLICT DO NOTHING so re-runs are safe.

Usage:
    python scripts/resharding_decommission.py
"""

import os
import sys
from collections import defaultdict

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
    if operation != "REMOVE" or phase != "DUAL_WRITE":
        print(
            f"ABORT: decommission must run with OPERATION=REMOVE and DB_RESHARDING_PHASE=DUAL_WRITE "
            f"(got OPERATION={operation!r}, PHASE={phase!r}).",
            file=sys.stderr,
        )
        sys.exit(2)

    doomed_shard = reshard["TARGET_SHARD_NAME"]

    # Refuse to drain the last shard -- removing it would empty the ring and break every request.
    if len(CONFIGS["SHARD_URLS"]) <= 1:
        print(
            f"ABORT: refusing to decommission the last shard -- SHARD_URLS has {list(CONFIGS['SHARD_URLS'])}.",
            file=sys.stderr,
        )
        sys.exit(2)

    print(f"Draining {doomed_shard} across the remaining shards\n")

    # Step 1 -- users first, reviews FK them
    print("[1/2] Draining users...")
    user_counts = _drain(doomed_shard, User, key_attr="id")
    for destination, count in sorted(user_counts.items()):
        print(f"      -> {destination}: {count} users")
    print(f":::> total: {sum(user_counts.values())} users\n")

    # Step 2 -- reviews follow their user
    print("[2/2] Draining reviews...")
    review_counts = _drain(doomed_shard, Review, key_attr="user_id")
    for destination, count in sorted(review_counts.items()):
        print(f"      -> {destination}: {count} reviews")
    print(f":::> total: {sum(review_counts.values())} reviews\n")

    print(
        f"Decommission complete. Flip MIGRATION_PHASE to CUTOVER, restart the backend, "
        f"then stop {doomed_shard}."
    )


def _drain(source_shard: str, model: type, key_attr: str) -> dict[str, int]:
    """Stream rows from source in batches; route each to its new home via shard_for(WRITE)."""
    scanned = 0
    copied_per_destination: dict[str, int] = defaultdict(int)
    last_id = None
    while True:
        with shard_router.session(source_shard) as src:
            stmt = select(model).order_by(model.id).limit(BATCH_SIZE)
            if last_id is not None:
                stmt = stmt.where(model.id > last_id)
            batch = list(src.exec(stmt).all())

        if not batch:
            break
        scanned += len(batch)

        # Bucket the batch by destination so we open one session per destination, not per row.
        # shard_for during REMOVE DUAL_WRITE returns [doomed, destination]; take the last entry.
        by_destination: dict[str, list] = defaultdict(list)
        for row in batch:
            destination = shard_router.shard_for(str(getattr(row, key_attr)), "WRITE")[-1]
            by_destination[destination].append(row)

        for destination, rows in by_destination.items():
            with shard_router.session(destination) as tgt:
                for row in rows:
                    stmt = pg_insert(model).values(**row.model_dump()).on_conflict_do_nothing(index_elements=["id"])
                    tgt.execute(stmt)
                tgt.commit()
            copied_per_destination[destination] += len(rows)

        last_id = batch[-1].id
        print(
            f"\r :::> draining {source_shard}  scanned={scanned}  copied={sum(copied_per_destination.values())}  ",
            end="",
            flush=True,
        )

    print(f"\r :::> draining {source_shard}  scanned={scanned}  copied={sum(copied_per_destination.values())}")
    return dict(copied_per_destination)


if __name__ == "__main__":
    main()

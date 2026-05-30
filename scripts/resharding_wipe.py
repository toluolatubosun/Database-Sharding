"""
Wipe the doomed shard after CUTOVER.

Once CUTOVER is active, the doomed shard is no longer in the router's ring
and traffic ignores it -- but its tables and rows are still physically there.
This script runs `alembic downgrade base` against the doomed shard to drop
every table and the alembic_version row, leaving an empty database ready to
be torn down.

Run order for REMOVE:
    DUAL_WRITE  -> resharding_decommission.py  (move data off)
    CUTOVER     -> resharding_wipe.py          (drop tables on the doomed shard)
                -> docker compose stop <shard> (tear the container down)

Usage:
    python scripts/resharding_wipe.py
"""

import os
import sys
import subprocess

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from configs.config import CONFIGS


def main():
    reshard = CONFIGS["DB_RESHARDING"]
    operation = reshard["OPERATION"]
    phase = reshard["DB_RESHARDING_PHASE"]
    if operation != "REMOVE" or phase != "CUTOVER":
        print(
            f"ABORT: wipe must run with OPERATION=REMOVE and DB_RESHARDING_PHASE=CUTOVER "
            f"(got OPERATION={operation!r}, PHASE={phase!r}).",
            file=sys.stderr,
        )
        sys.exit(2)

    doomed_shard = reshard["TARGET_SHARD_NAME"]
    if doomed_shard not in CONFIGS["SHARD_URLS"]:
        print(
            f"ABORT: {doomed_shard!r} not found in SHARD_URLS -- did you already remove it from config?",
            file=sys.stderr,
        )
        sys.exit(2)

    doomed_url = CONFIGS["SHARD_URLS"][doomed_shard]
    print(f"Wiping {doomed_shard} via `alembic downgrade base`...")

    env = {**os.environ, "DATABASE_URL": doomed_url}
    result = subprocess.run(
        ["alembic", "downgrade", "base"],
        env=env,
        capture_output=True,
        text=True,
        timeout=300,
    )
    if result.returncode != 0:
        print(result.stderr, file=sys.stderr)
        sys.exit(1)

    print(f":::> {doomed_shard} is now empty (no tables, no rows, no alembic_version).")
    print(f"     Next: docker compose stop {doomed_shard}, remove it from SHARD_URLS, set MIGRATION_PHASE=IDLE.")


if __name__ == "__main__":
    main()

"""
Parallel migration runner across all shards + global DB.

Usage:
    python scripts/migrate.py upgrade head
    python scripts/migrate.py upgrade head --timeout 600
    python scripts/migrate.py downgrade -1

The script:
  1. Reads alembic_version on every DB and aborts if they disagree.
  2. Runs `alembic <args>` against each DB in parallel, with a per-DB
     timeout, by setting DATABASE_URL in the child's environment.
  3. Re-reads alembic_version and prints a status board.
  4. Exits 0 only if every DB converged to the same non-empty version.
"""

import argparse
import os
import sys
import subprocess
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional

from sqlalchemy import create_engine, text
from sqlalchemy.exc import ProgrammingError

# Make `configs.config` importable when run as `python scripts/migrate.py`.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from configs.config import CONFIGS


DEFAULT_TIMEOUT = 300


@dataclass
class DBTarget:
    name: str
    url: str


@dataclass
class MigrationResult:
    target: DBTarget
    success: bool
    state: str                       # version hash, "uninitialized", or "empty"
    error_excerpt: Optional[str] = None


def list_targets() -> list[DBTarget]:
    targets = [DBTarget(name=name, url=url) for name, url in CONFIGS["SHARD_URLS"].items()]
    targets.append(DBTarget(name="global", url=CONFIGS["GLOBAL_DB_URL"]))

    # Include the incoming shard once resharding is in flight so its schema
    # can be brought to parity before DUAL_WRITE flips on. 
    # Only relevant for ADD, for REMOVE the target is already in SHARD_URLS above.
    reshard = CONFIGS["DB_RESHARDING"]
    if reshard["OPERATION"] == "ADD" and reshard["DB_RESHARDING_PHASE"] in ("ALLOW_MIGRATIONS", "DUAL_WRITE", "CUTOVER"):
        targets.append(DBTarget(name=reshard["TARGET_SHARD_NAME"], url=reshard["TARGET_SHARD_URL"]))
    return targets


def read_state(target: DBTarget) -> str:
    """Classify a DB as uninitialized / empty / <version_hash>."""
    engine = create_engine(target.url)
    try:
        with engine.connect() as conn:
            try:
                row = conn.execute(text("SELECT version_num FROM alembic_version")).fetchone()
            except ProgrammingError:
                return "uninitialized"
            return row[0] if row else "empty"
    finally:
        engine.dispose()


FRESH_STATES = {"uninitialized", "empty"}


def assert_no_drift(target_states: dict[str, str], when: str) -> None:
    """Abort if DBs are at two or more different non-fresh versions.

    Fresh DBs (uninitialized / empty) are allowed to coexist with one versioned cluster
    That's exactly the resharding onboarding case where a brand-new shard joins existing ones and will catch up during this migration run. 
    Real drift is two distinct non-fresh versions.
    """
    non_fresh_versions = {state for state in target_states.values() if state not in FRESH_STATES}
    if len(non_fresh_versions) <= 1:
        return
    print(f"\nDRIFT DETECTED ({when}):", file=sys.stderr)
    for name, state in target_states.items():
        print(f"  {name:10} {state}", file=sys.stderr)
    print("\nAll DBs must be in the same state before migrating. Aborting.", file=sys.stderr)
    sys.exit(2)


def run_one(target: DBTarget, alembic_args: list[str], timeout: int) -> MigrationResult:
    """Shell out to `alembic <args>` for a single DB with DATABASE_URL set."""
    env = {**os.environ, "DATABASE_URL": target.url}
    try:
        proc = subprocess.run(
            ["alembic", *alembic_args],
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        final = read_state(target)
        if proc.returncode != 0:
            tail = "\n".join(proc.stderr.strip().splitlines()[-15:])
            return MigrationResult(target, False, final, tail)
        return MigrationResult(target, True, final)
    except subprocess.TimeoutExpired:
        final = read_state(target)
        return MigrationResult(target, False, final, f"TIMEOUT after {timeout}s")
    except Exception as e:
        final = read_state(target)
        return MigrationResult(target, False, final, repr(e))


def print_report(results: list[MigrationResult]) -> bool:
    """Print the status board. Return True if every DB converged to the same non-empty version."""
    print("\n=== MIGRATION RESULTS ===")
    versions_of_success = set()
    failures: list[str] = []
    for r in results:
        if r.success:
            print(f"  {r.target.name:10} OK    {r.state}")
            versions_of_success.add(r.state)
        else:
            print(f"  {r.target.name:10} FAIL  at {r.state}")
            for line in (r.error_excerpt or "").splitlines():
                print(f"                {line}")
            failures.append(r.target.name)

    print()

    converged = (
        not failures
        and len(versions_of_success) == 1
        and next(iter(versions_of_success)) not in ("uninitialized", "empty")
    )

    if failures:
        target_version = next(iter(versions_of_success), "(no version)")
        print(f"WARNING: DRIFT DETECTED -- {', '.join(failures)} did not converge")
        print(f"   Other DBs are at: {target_version}")
        print(f"   Do NOT deploy code expecting the new schema until resolved.")
        print(f"   Fix the failure above and re-run, or roll the others back.")
    elif len(versions_of_success) > 1:
        print(f"WARNING: DBs ended on different versions: {sorted(versions_of_success)}")
    elif converged:
        print(f"OK: All DBs converged to {next(iter(versions_of_success))}")
    else:
        # All "success" but everyone landed on uninitialized/empty -- alembic no-op.
        print(f"WARNING: alembic ran without errors but no version was set: {sorted(versions_of_success)}")

    return converged


def main():
    parser = argparse.ArgumentParser(
        description="Parallel migration runner across all DBs. "
                    "Unknown args are forwarded to alembic.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=DEFAULT_TIMEOUT,
        help=f"Per-DB timeout in seconds (default: {DEFAULT_TIMEOUT})",
    )
    args, alembic_args = parser.parse_known_args()

    if not alembic_args:
        parser.error("Pass alembic args, e.g.: python scripts/migrate.py upgrade head")

    targets = list_targets()

    # Pre-migration drift check 
    print(f"Checking pre-migration state across {len(targets)} DBs...")
    pre = {t.name: read_state(t) for t in targets}
    for name, state in pre.items():
        print(f"  {name:10} {state}")
    assert_no_drift(pre, when="before migration")

    # Run alembic in parallel 
    print(f"\nRunning `alembic {' '.join(alembic_args)}` on {len(targets)} DBs in parallel...")
    results: list[MigrationResult] = []
    with ThreadPoolExecutor(max_workers=len(targets)) as pool:
        futures = {pool.submit(run_one, t, alembic_args, args.timeout): t for t in targets}
        for fut in as_completed(futures):
            results.append(fut.result())
    results.sort(key=lambda r: r.target.name)

    # Status board + exit code 
    converged = print_report(results)
    sys.exit(0 if converged else 1)


if __name__ == "__main__":
    main()

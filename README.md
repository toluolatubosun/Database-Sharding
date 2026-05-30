# Database Sharding Demo

A FastAPI + Postgres sandbox that demonstrates database sharding: global database, consistent hashing, colocation, cross-shard fan-out reads, and online resharding via dual-write.

> A did Deep dive and walkthrough of the various concepts implemented here, [Medium article](). I would recommend reading that first if you're new to database sharding, so the code is more understandable.

## Quick start

```bash
docker compose up -d
docker compose exec backend python scripts/migrate.py upgrade head
docker compose exec backend python scripts/seed.py
```

The API is at `http://localhost:8000`. pgweb tabs per DB:

| DB         | Port |
|------------|------|
| shard_0    | 8081 |
| shard_1    | 8082 |
| shard_2    | 8083 |
| global     | 8084 |
| shard_3    | 8085 (migration profile only) |

## Where things live

### `sharding/`
- **`ring.py`** — `ConsistentHashRing` with virtual nodes. MD5 hash + `bisect` for shard lookup. Run directly (`python sharding/ring.py`) for a distribution + resharding demo.
- **`router.py`** — `ShardRouter`. Holds engines for every shard + global, owns the ring(s), exposes `shard_for(key, "READ"|"WRITE")`, `session(name)`, `all_shards()`. Reads `CONFIGS["DB_RESHARDING"]` at init time and wires up a second `_new_ring` when phase is `DUAL_WRITE`.

### `database.py`
Module-level `ShardRouter` singleton + `get_session_shard(name)` facade for FastAPI handlers.

### `models/`
- **`user.py`** — sharded by `id`. Email is `unique` per shard; cluster-wide uniqueness is enforced via Redis SETNX.
- **`review.py`** — sharded by `user_id` (colocates with `User`). FK to `user.id` enforced at the shard level. No FK to `product` (different DB).
- **`product.py`** — lives on the **global** DB, unsharded.

### `routers/`
- **`user.py`** — `POST /users` claims email in Redis, then writes the user to every shard returned by `shard_for(..., "WRITE")` (one shard normally, two during DUAL_WRITE for moving keys). Releases the claim on any DB error.
- **`review.py`** — `POST /reviews` validates the product on global, then writes to the WRITE shard list. `GET /products/{id}/reviews` fans out to `all_shards()` in parallel via `ThreadPoolExecutor`, does a local `User`+`Review` JOIN per shard, then merge-sorts on `(created_at, id)` with cursor pagination.
- **`product.py`** — straightforward CRUD on the global DB.

### `libraries/redis.py`
Redis client used for cross-shard email uniqueness (SETNX claim + lazy reconciliation).

### `scripts/`
- **`migrate.py`** — parallel alembic runner. Drift check before + after, status board, non-zero exit on divergence. Each DB runs `alembic` in a child process with `DATABASE_URL` overridden.
- **`seed.py`** — deterministic seed (50 products, 100 users, 250 reviews). Builds its own ring to assign users → shards before insert.
- **`resharding_backfill.py`** — `ADD` flow, runs during `DUAL_WRITE`. Keyset-paginates every old shard and copies the rows whose key now routes to the new shard. Idempotent via `ON CONFLICT DO NOTHING`.
- **`resharding_cutover.py`** — `ADD` flow, runs during `CUTOVER`. Scans every shard and deletes rows whose key no longer routes there. Reviews first, then users (FK order). Idempotent.
- **`resharding_decommission.py`** — `REMOVE` flow, runs during `DUAL_WRITE`. Drains the doomed shard, routing each row to its new home via `_new_ring`. Mirror of backfill (one source → many destinations). Idempotent.
- **`resharding_wipe.py`** — `REMOVE` flow, runs during `CUTOVER`. Runs `alembic downgrade base` on the doomed shard to drop all its tables, leaving it empty and ready to tear down.

### `alembic/`
Single revision history shared by every DB. `env.py` reads `DATABASE_URL` from the env (set by `migrate.py`) and falls back to the global DB URL for autogen.

### `configs/config.py`
Environment-keyed configs. `SHARD_URLS` is the live ring topology; `DB_RESHARDING` is the migration control block (`OPERATION`, `TARGET_SHARD_NAME`, `TARGET_SHARD_URL`, `DB_RESHARDING_PHASE`).

## Resharding flow

Driven by `CONFIGS["DB_RESHARDING"]`: `OPERATION` (`ADD` | `REMOVE`), `TARGET_SHARD_NAME`, `TARGET_SHARD_URL`, and `DB_RESHARDING_PHASE`. During `DUAL_WRITE`, reads stay on the old ring; writes fan out to both rings when the key is moving. During `CUTOVER`, the ring is updated and reads/writes flow normally.

### Add

Walks `shard_3` into the cluster.

```
IDLE -> ALLOW_MIGRATIONS -> DUAL_WRITE -> CUTOVER -> IDLE (shard_3 promoted)
```

1. Start `shard_3` — `docker compose --profile migration up -d`
2. Set `OPERATION=ADD`, `TARGET_SHARD_NAME=shard_3`, `MIGRATION_PHASE=ALLOW_MIGRATIONS` in `.env` or `configs/config.py` 
3. `python scripts/migrate.py upgrade head` — brings `shard_3`'s schema to parity
4. Set `MIGRATION_PHASE=DUAL_WRITE`
5. `python scripts/resharding_backfill.py` — copies historical rows to `shard_3`
6. Set `MIGRATION_PHASE=CUTOVER`, restart backend
7. `python scripts/resharding_cutover.py` — deletes stale rows on old shards
8. Move `shard_3` into `SHARD_URLS`, set `MIGRATION_PHASE=IDLE`, restart

### Remove

Walks `shard_3` back out. No `ALLOW_MIGRATIONS` step (existing shards already have the schema).

```
IDLE -> DUAL_WRITE -> CUTOVER -> IDLE (shard_3 gone)
```

1. Set `OPERATION=REMOVE`, `TARGET_SHARD_NAME=shard_3`, `MIGRATION_PHASE=DUAL_WRITE`, restart backend
2. `python scripts/resharding_decommission.py` — drains `shard_3` across the remaining shards
3. Set `MIGRATION_PHASE=CUTOVER`, (now the router pretends `shard_3` doesn't exist)
4. `python scripts/resharding_wipe.py` — drops every table on `shard_3` via `alembic downgrade base`
5. `docker compose stop shard_3` — tear the container down
6. Remove `shard_3` from `SHARD_URLS`, set `MIGRATION_PHASE=IDLE`, restart

## Useful commands

```bash
# Inside the backend container
alembic revision --autogenerate -m "your migration"
python sharding/ring.py        # ring distribution + resharding demo
python scripts/migrate.py upgrade head
python scripts/migrate.py upgrade head --timeout 600
```

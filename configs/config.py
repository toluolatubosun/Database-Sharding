import os
from datetime import timedelta

APP_VERSION = '1.0.0'
DEPLOYMENT_ENV = os.getenv('DEPLOYMENT_ENV', 'development')

# Global constants
GLOBAL_CONSTANTS = {
    # System Constants
    "APP_NAME": "Database Sharding API",
}

# Configuration for different environments
CONFIG_BUILDER = {
    "development": {
        **GLOBAL_CONSTANTS,

        "REDIS_URI": "redis://redis:6379",

        "SHARD_URLS": {
            "shard_0": "postgresql://postgres:password@shard_0:5432/shard_0",
            "shard_1": "postgresql://postgres:password@shard_1:5432/shard_1",
            "shard_2": "postgresql://postgres:password@shard_2:5432/shard_2",
            
            # # Uncomment below to enable shard_3 for development testing. 
            # # This should be done after the resharding process is complete. Check the README for instructions.
            # "shard_3": "postgresql://postgres:password@shard_3:5432/shard_3",
        },
        "GLOBAL_DB_URL": "postgresql://postgres:password@global_db:5432/global",

        # Resharding operation
        # ADD    - Bring a new shard online; rows redistribute *to* the target.
        # REMOVE - Drain an existing shard; rows redistribute *off* the target across the remaining shards.
        #
        # Resharding phases (apply to both operations)
        # IDLE             - No resharding activity, normal operations.
        # ALLOW_MIGRATIONS - Tooling-only flag so scripts/migrate.py can bring the new shard's schema to parity. The router still behaves as IDLE.
        # DUAL_WRITE       - Application writes to both old and new shards, but reads from old shard.
        # CUTOVER          - Application reads and writes from the new shard, old shard is being cleared of redundant data.
        "DB_RESHARDING": {
            "OPERATION": "ADD",  # Options: ADD or REMOVE
            "TARGET_SHARD_NAME": "shard_3",
            "TARGET_SHARD_URL": "postgresql://postgres:password@shard_3:5432/shard_3",
            "DB_RESHARDING_PHASE": "IDLE",  # Options: IDLE, ALLOW_MIGRATIONS, DUAL_WRITE, CUTOVER
        },
    },

    "production": {
        **GLOBAL_CONSTANTS,

        "REDIS_URI": os.getenv("REDIS_URI") or "redis://redis:6379",

        "SHARD_URLS": {
            "shard_0": os.getenv("SHARD_0_URL") or "postgresql://postgres:password@shard_0:5432/shard_0",
            "shard_1": os.getenv("SHARD_1_URL") or "postgresql://postgres:password@shard_1:5432/shard_1",
            "shard_2": os.getenv("SHARD_2_URL") or "postgresql://postgres:password@shard_2:5432/shard_2",
        },
        "GLOBAL_DB_URL": os.getenv("GLOBAL_DB_URL") or "postgresql://postgres:password@global_db:5432/global",

        "DB_RESHARDING": {
            "OPERATION": os.getenv("MIGRATION_OPERATION", "ADD"),
            "TARGET_SHARD_NAME": os.getenv("MIGRATION_TARGET_SHARD"),
            "TARGET_SHARD_URL": os.getenv("MIGRATION_TARGET_URL") or "postgresql://postgres:password@shard_3:5432/shard_3",
            "DB_RESHARDING_PHASE": os.getenv("MIGRATION_PHASE", "IDLE"),
        },
    }
}

# Check if DEPLOYMENT_ENV is valid
if DEPLOYMENT_ENV not in CONFIG_BUILDER:
    raise ValueError(f"Invalid DEPLOYMENT_ENV: {DEPLOYMENT_ENV}")

# Get the configuration for the current environment
CONFIGS = CONFIG_BUILDER[DEPLOYMENT_ENV]

# Uncomment below to check configs set
# print("CONFIGS:", CONFIGS)

# Variables are already accessible when importing this module

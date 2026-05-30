from sqlmodel import Session

from configs.config import CONFIGS
from sharding.router import ShardRouter


router = ShardRouter(
    shard_urls=CONFIGS["SHARD_URLS"],
    global_url=CONFIGS["GLOBAL_DB_URL"],
)


def get_session_shard(name: str) -> Session:
    return router.session(name)

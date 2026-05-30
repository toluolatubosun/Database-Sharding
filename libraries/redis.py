import redis

from configs.config import CONFIGS


redis_client = redis.Redis.from_url(CONFIGS["REDIS_URI"], decode_responses=True)

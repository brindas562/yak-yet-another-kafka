import redis
from common.config import REDIS_HOST, REDIS_PORT

def get_redis():
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)
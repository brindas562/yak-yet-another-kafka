from common.redis_client import get_redis
r = get_redis()
try:
    ok = r.ping()
    print("PING OK:", ok)
except Exception as e:
    print("Redis connection failed:", type(e).__name__, e)
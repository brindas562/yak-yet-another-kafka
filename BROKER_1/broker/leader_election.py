import time
from common.redis_client import get_redis
from common.config import LEASE_KEY, LEASE_TTL

r = get_redis()

def try_become_leader(broker_ip):
    """
    Tries to acquire leader lease using SETNX (atomic).
    """
    if r.set(LEASE_KEY, broker_ip, nx=True, ex=LEASE_TTL):
        print(f"[INFO] {broker_ip} became the leader")
        return True
    return False

def renew_lease(broker_ip):
    """
    Renews leader lease if still leader.
    """
    current = r.get(LEASE_KEY)
    if current == broker_ip:
        r.set(LEASE_KEY, broker_ip, ex=LEASE_TTL)
        return True
    return False

def get_current_leader():
    """
    Returns IP of current leader from Redis.
    """
    return r.get(LEASE_KEY)
import requests
from common.redis_client import get_redis
from common.config import HWM_KEY

r = get_redis()

def replicate_to_follower(follower_url, data):
    """
    Synchronous replication: waits for ACK before commit.
    """
    try:
        res = requests.post(follower_url, json=data, timeout=3)
        if res.status_code == 200 and res.json().get("ack"):
            print("[REPLICATION] Follower ACK received")
            return True
    except Exception as e:
        print("[REPLICATION ERROR]", e)
    return False

def update_high_water_mark(offset):
    r.set(HWM_KEY, offset)

def get_high_water_mark():
    val = r.get(HWM_KEY)
    return int(val) if val else 0
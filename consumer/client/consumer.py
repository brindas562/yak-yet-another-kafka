import os
import time
import requests
from common.config import BROKER_1, BROKER_2

BROKERS = [BROKER_1, BROKER_2]
PORT = 8000
OFFSET_FILE = "consumer_offset.txt"
BATCH_LIMIT = 50  # how many messages per pull

def load_offset() -> int:
    if not os.path.exists(OFFSET_FILE):
        return 1  # your storage starts offsets at 1
    with open(OFFSET_FILE, "r") as f:
        txt = f.read().strip()
        return int(txt) if txt else 1

def save_offset(next_off: int) -> None:
    with open(OFFSET_FILE, "w") as f:
        f.write(str(next_off))

def get_leader() -> str | None:
    """
    Ask brokers' /metadata/leader (NOT Redis).
    """
    for b in BROKERS:
        try:
            r = requests.get(f"http://{b}:{PORT}/metadata/leader", timeout=2)
            if r.status_code == 200:
                leader = r.json().get("leader")
                if leader:
                    return leader
        except Exception:
            continue
    return None

def consume_once():
    offset = load_offset()
    leader = get_leader()
    if not leader:
        print("❌ No leader available. Retrying...")
        time.sleep(2)
        return

    try:
        # ask only for messages at/after my current offset
        resp = requests.get(
            f"http://{leader}:{PORT}/consume",
            params={"offset": offset, "limit": BATCH_LIMIT},
            timeout=4
        )
        if resp.status_code == 403:
            # hit follower by mistake; try again
            print("⚠ Not the leader. Rediscovering...")
            time.sleep(1)
            return

        resp.raise_for_status()
        data = resp.json()
        all_msgs = data.get("messages", [])

        # CRITICAL FIX: Broker returns ALL messages, so we filter locally
        # Only process messages at or after our current offset
        new_msgs = [m for m in all_msgs if m.get("offset", 0) >= offset]

        if not new_msgs:
            # nothing new; don't advance offset
            time.sleep(1)
            return

        print(f"📊 Received {len(all_msgs)} total, {len(new_msgs)} new (starting from offset {offset})")
        
        # Process only NEW messages and save offset after EACH one
        for m in new_msgs:
            msg_offset = m.get("offset", offset)
            print(f"📥 [Offset {msg_offset}] {m.get('data', m)}")
            # Save the NEXT offset to read (current message offset + 1)
            save_offset(msg_offset + 1)

    except Exception as e:
        print(f"⚠ Consumer error: {e}. Will retry...")
        time.sleep(2)

if __name__ == "__main__":
    print("🔥 Consumer running (leader discovery + local offset + HWM safe)")
    while True:
        consume_once()
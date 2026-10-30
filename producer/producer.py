import requests, sys, time
from common.config import BROKER_1, BROKER_2

brokers = [BROKER_1, BROKER_2]

def get_leader():
    for b in brokers:
        try:
            r = requests.get(f"http://{b}:8000/metadata/leader", timeout=2)
            if r.status_code == 200:
                return r.json()["leader"]
        except:
            continue
    return None

def send_message(message):
    leader = get_leader()
    if not leader:
        print("[ERROR] No leader found")
        return

    try:
        res = requests.post(f"http://{leader}:8000/produce", json={"message": message}, timeout=3)
        if res.status_code == 200:
            print("[PRODUCER] Message sent:", res.json())
        else:
            print("[WARN] Leader rejected, retrying...")
            send_message(message)
    except Exception:
        print("[ERROR] Connection failed, retrying...")
        time.sleep(2)
        send_message(message)

if __name__ == "__main__":
    while True:
        msg = sys.argv[1] if len(sys.argv) > 1 else "Test Message"
        send_message(msg)
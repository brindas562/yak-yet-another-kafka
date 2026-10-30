from fastapi import FastAPI, Request, HTTPException
import requests, threading, time, sys
from broker.leader_election import try_become_leader, renew_lease, get_current_leader
from broker.storage import append_message, read_all, read_upto_offset
from broker.replication import replicate_to_follower, update_high_water_mark, get_high_water_mark
from common.config import BROKER_1, BROKER_2, LEASE_TTL



app = FastAPI()
ROLE = None
BROKER_IP = None


FOLLOWER_URL = f"http://{BROKER_2}:8000/internal/replicate"

@app.post("/produce")
async def produce(req: Request):
    data = await req.json()
    leader = get_current_leader()
    if leader != BROKER_IP:
        raise HTTPException(status_code=403, detail="Not the leader")

    # Save message locally
    offset = append_message(data)

    # Replicate synchronously
    if not replicate_to_follower(FOLLOWER_URL, data):
        print("[WARN] Replication failed, not committing message")
        raise HTTPException(status_code=500, detail="Replication failed")

    # Update HWM (committed)
    update_high_water_mark(offset)
    print(f"[LEADER] Message committed (offset={offset}): {data}")
    return {"status": "ok", "leader": BROKER_IP, "offset": offset}


@app.post("/internal/replicate")
async def replicate(req: Request):
    data = await req.json()
    offset = append_message(data)
    print(f"[FOLLOWER] Replicated message (offset={offset}): {data}")
    return {"ack": True}


@app.get("/consume")
async def consume():
    hwm = get_high_water_mark()
    messages = read_upto_offset(hwm)
    return {"messages": messages, "hwm": hwm}


@app.get("/metadata/leader")
async def metadata_leader():
    leader = get_current_leader()
    return {"leader": leader}


def lease_heartbeat():
    """Leader keeps renewing lease."""
    while True:
        if ROLE == "leader":
            renew_lease(BROKER_IP)
        time.sleep(LEASE_TTL / 2)


def follower_monitor():
    """Follower monitors Redis for expired leader lease."""
    while True:
        leader = get_current_leader()
        if not leader:
            success = try_become_leader(BROKER_IP)
            if success:
                global ROLE
                ROLE = "leader"
                print(f"[PROMOTION] {BROKER_IP} is now Leader!")
                threading.Thread(target=lease_heartbeat, daemon=True).start()
                break
        time.sleep(1)


def start(role, ip):
    global ROLE, BROKER_IP
    ROLE, BROKER_IP = role, ip
    print(f"Starting {ROLE.upper()} broker at {BROKER_IP}")

    if role == "leader":
        try_become_leader(ip)
        threading.Thread(target=lease_heartbeat, daemon=True).start()
    elif role == "follower":
        threading.Thread(target=follower_monitor, daemon=True).start()


if __name__ == "__main__":
    import uvicorn
    role, ip = sys.argv[1], sys.argv[2]
    start(role, ip)
    uvicorn.run(app, host="0.0.0.0", port=8000)
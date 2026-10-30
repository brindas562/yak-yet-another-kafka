import json
import os



LOG_FILE = "yak_log.jsonl"

def append_message(data):
    """
    Append message to local log with an incrementing offset.
    """
    offset = get_next_offset()
    entry = {"offset": offset, "data": data}
    with open(LOG_FILE, "a") as f:
        f.write(json.dumps(entry) + "\n")
    return offset

def read_all():
    if not os.path.exists(LOG_FILE):
        return []
    with open(LOG_FILE, "r") as f:
        return [json.loads(line) for line in f.readlines()]

def read_upto_offset(hwm):
    messages = read_all()
    return [m for m in messages if m["offset"] <= hwm]

def get_next_offset():
    messages = read_all()
    return messages[-1]["offset"] + 1 if messages else 1

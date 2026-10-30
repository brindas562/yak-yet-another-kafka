# Yet Another Kafka (YAK)

A minimal, from-scratch, Kafka-inspired distributed message broker built in Python. YAK implements the core ideas behind Kafka-style log-based messaging — a single elected leader, synchronous replication, high-water-mark commit semantics, and automatic failover — using nothing but **FastAPI**, **Redis**, and plain **HTTP/TCP**.

> Built as a learning/systems project to understand how distributed logs, leader election, and replication protocols work under the hood — not a production message queue.

---

## ✨ Key Features

- **Single-leader architecture** — only the broker holding the Redis-backed lease accepts writes (`/produce`); all others reject with `403`.
- **Leader election via Redis lease (`SET NX EX`)** — brokers race to atomically acquire a time-boxed lease key; the winner becomes leader.
- **Automatic failover** — if the leader stops renewing its lease (crash, network partition), the lease expires and a follower promotes itself.
- **Synchronous replication** — the leader only commits (and acks the producer) after the follower has replicated and ACKed the message.
- **High-Water-Mark (HWM) commit semantics** — consumers can only read messages up to the HWM, so uncommitted/unreplicated messages are never visible to readers.
- **Leader-aware clients** — the producer and consumer discover the current leader dynamically via `/metadata/leader` and re-resolve it on failure, instead of hardcoding a broker.
- **Append-only local log** — each broker persists messages to a `yak_log.jsonl` file with a monotonically increasing offset, mimicking a Kafka partition log segment.
- **Offset tracking on the consumer side** — the consumer checkpoints its last-read offset to disk (`consumer_offset.txt`) so it can resume after a restart.

---

## 🏗️ Architecture

```
                 ┌────────────────────┐
                 │       Redis         │   ← leader lease (yak:leader)
                 │  (metadata store)   │   ← high-water-mark (yak:hwm)
                 └─────────▲──────────┘
                           │ lease / HWM
             ┌─────────────┴─────────────┐
             │                           │
     ┌───────▼────────┐         ┌────────▼───────┐
     │   BROKER_1      │  sync   │   BROKER_2      │
     │  (leader today)  │◄──────►│  (follower)     │
     │  FastAPI :8000  │  repl.  │  FastAPI :8000  │
     └───────▲────────┘         └────────▲────────┘
             │  /produce                 │
             │  /consume                 │
   ┌─────────┴─────────┐       ┌─────────┴─────────┐
   │     Producer        │       │     Consumer        │
   │  producer.py         │       │  consumer.py         │
   └─────────────────────┘       └─────────────────────┘
```

**Write path:** Producer → asks any broker for the current leader (`/metadata/leader`) → sends message to leader's `/produce` → leader appends to its local log → leader synchronously replicates to the follower's `/internal/replicate` and waits for an ACK → leader advances the HWM in Redis → success is returned to the producer.

**Read path:** Consumer → asks a broker for the current leader → pulls messages from `/consume` on the leader (only messages at/below the HWM are returned) → advances its local offset checkpoint.

**Failover path:** Each broker holds/renews a Redis lease (`yak:leader`, TTL-based) while it's leader. Followers poll Redis; if the lease key is missing (leader crashed or stopped renewing), the first follower to win `SET NX` becomes the new leader and starts serving `/produce`.

---

## 📁 Project Structure

```
Yet_Another_kafka-main/
├── BROKER_1/                 # Broker node instance #1
│   ├── broker/
│   │   ├── broker_app.py     # FastAPI app: /produce, /consume, /metadata/leader (HWM-aware — main entrypoint)
│   │   ├── yak_broker.py     # Earlier/simpler broker variant (no HWM gating)
│   │   ├── leader_election.py# Redis-lease based leader election (SETNX + renew)
│   │   ├── replication.py    # Synchronous replication + high-water-mark helpers
│   │   └── storage.py        # Append-only JSONL log with incrementing offsets
│   ├── common/
│   │   ├── config.py         # Redis + broker IPs, lease TTL, Redis keys
│   │   └── redis_client.py   # Redis connection factory
│   └── requirenments.txt     # Python dependencies
│
├── BROKER_2/                 # Broker node instance #2 (mirrors BROKER_1)
│   ├── broker/                # Same modules as BROKER_1
│   ├── common/
│   │   ├── config.py
│   │   ├── redis_client.py
│   │   └── test_redis.py     # Quick Redis connectivity check (PING)
│   └── requirements.txt
│
├── producer/
│   ├── producer.py            # CLI producer: discovers leader, POSTs /produce, retries on failure
│   └── common/                # config.py + redis_client.py (shared pattern)
│
├── consumer/
│   ├── client/
│   │   └── consumer.py        # Polls leader's /consume, filters by local offset, persists checkpoint
│   └── common/                # config.py + redis_client.py (shared pattern)
│
├── redis/
│   └── docker-compose.yml     # Standalone Redis service (AOF persistence enabled)
│
└── README.md
```

Each of `BROKER_1`, `BROKER_2`, `producer`, and `consumer` is a **self-contained deployable unit** — in the reference setup these run as separate processes/machines (e.g. over a ZeroTier overlay network), each with its own copy of `common/config.py` pointing at the shared Redis instance and both broker IPs.

---

## ⚙️ Tech Stack

| Layer | Technology |
|---|---|
| Broker HTTP API | FastAPI + Uvicorn |
| Inter-node communication | `requests` (broker replication, client discovery) |
| Metadata / coordination store | Redis (leader lease, high-water-mark) |
| Persistence | Local append-only JSONL log files |
| Client libraries | `requests`, `redis-py` |
| Container orchestration | Docker Compose (for Redis) |

---

## 🔑 Coordination Model (Redis Keys)

| Key | Purpose | Set by |
|---|---|---|
| `yak:leader` | Holds the IP of the current leader broker. Acquired via `SET yak:leader <ip> NX EX <LEASE_TTL>` so only one broker can hold it at a time. | Leader election (`leader_election.py`) |
| `yak:hwm` | The highest **committed** (i.e. replicated + ACKed) offset. Consumers may only read up to this offset. | Leader, after a successful replication ACK (`replication.py`) |

Lease TTL defaults to **5 seconds**; the leader renews it roughly every `LEASE_TTL / 2` seconds. If a leader stops renewing (crash/partition), the key expires and any follower polling Redis can win the next `SETNX` race and promote itself.

---

## 🚀 Getting Started

### 1. Prerequisites
- Python 3.10+
- Docker (for Redis) — or a reachable Redis instance
- Network connectivity between all nodes (the reference setup uses a ZeroTier overlay so brokers/producer/consumer can reach each other across machines)

### 2. Start Redis

```bash
cd redis
docker compose up -d
```

This starts Redis on port `6379` with append-only persistence enabled.

### 3. Configure the nodes

Each component (`BROKER_1`, `BROKER_2`, `producer`, `consumer`) has its own `common/config.py`. Update the following in **every copy** so they all agree:

```python
REDIS_HOST = "<redis-node-ip>"
REDIS_PORT = 6379

BROKER_1 = "<broker-1-ip>"   # Leader-eligible node
BROKER_2 = "<broker-2-ip>"   # Follower-eligible node

LEASE_KEY = "yak:leader"
LEASE_TTL = 5                # seconds
HWM_KEY = "yak:hwm"
```

### 4. Install dependencies (per node)

```bash
# On BROKER_1
cd BROKER_1
pip install -r requirenments.txt

# On BROKER_2
cd BROKER_2
pip install -r requirements.txt
```

Producer/consumer machines just need `requests` and `redis` installed.

### 5. Run the brokers

From inside each broker's directory:

```bash
# BROKER_1 — attempts to start as leader
python -m broker.broker_app leader <broker-1-ip>

# BROKER_2 — starts as a follower, watches for leader failure
python -m broker.broker_app follower <broker-2-ip>
```

Each broker serves FastAPI on `0.0.0.0:8000`.

### 6. Run the producer

```bash
cd producer
python producer.py "hello from producer"
```

The producer looks up the current leader via `/metadata/leader` on either broker, then POSTs to that leader's `/produce`. It retries automatically on connection failure or if it hits a non-leader (`403`).

### 7. Run the consumer

```bash
cd consumer/client
python consumer.py
```

The consumer polls the leader's `/consume?offset=<n>&limit=50` in a loop, only accepts messages at or beyond its local offset, and checkpoints progress to `consumer_offset.txt` after each processed message — so it can resume where it left off after a restart.

---

## 📡 API Reference (per broker)

| Method | Endpoint | Description |
|---|---|---|
| `POST` | `/produce` | Accepts a message if the node is the current leader; otherwise returns `403`. Appends to local log, replicates synchronously to the follower, advances the HWM, and returns the assigned offset. |
| `POST` | `/internal/replicate` | Internal-only. Follower endpoint that appends a replicated message to its own log and returns `{"ack": true}`. |
| `GET` | `/consume` | Returns all messages up to the current high-water-mark, plus the HWM value. |
| `GET` | `/metadata/leader` | Returns `{"leader": "<ip or null>"}` — used by producer/consumer for leader discovery. |

---

## 🧪 Simulating Failover

1. Start `BROKER_1` as leader and `BROKER_2` as follower, plus a producer/consumer.
2. Confirm messages flow end-to-end (producer → leader → follower replication → consumer read).
3. Kill `BROKER_1`.
4. Once its lease (`yak:leader` in Redis) expires (~5s), `BROKER_2`'s `follower_monitor` loop wins the `SETNX` race and promotes itself, logging `[PROMOTION] <ip> is now Leader!`.
5. The producer and consumer, on their next request, re-resolve the leader via `/metadata/leader` and continue seamlessly against `BROKER_2`.

---

## ⚠️ Known Limitations / Notes

- **No log compaction, partitioning, or topics** — there is a single implicit log per broker; this is a simplified single-partition model, not a multi-topic broker.
- **Two broker implementations coexist**: `broker_app.py` (HWM-gated, synchronous-ACK commit path) is the more correct/complete implementation; `yak_broker.py` is an earlier version without high-water-mark gating (a consumer could see a message before it's replicated). Prefer `broker_app.py` as the entrypoint.
- **Replication target is hardcoded per node** — each broker's `FOLLOWER_URL` points at a fixed peer rather than being derived dynamically from cluster membership, so this setup targets exactly two brokers.
- **No re-election back to the original leader** — once a follower is promoted, there's no automatic rebalancing when the original leader comes back online; it would need to rejoin as a follower.
- **Config duplication** — `common/config.py` is copy-pasted across `BROKER_1`, `BROKER_2`, `producer`, and `consumer` rather than shared from a single package, so IPs must be kept in sync manually across all four copies.
- **Plaintext HTTP, no auth** — there's no TLS or authentication between brokers/clients; intended for trusted/local network use only.

---

## 🎯 What This Project Demonstrates

- Distributed leader election using an external coordination store (Redis) with lease/TTL semantics, analogous to how ZooKeeper/etcd-based systems elect a leader.
- Synchronous ("all-ACKs") replication and the resulting durability/consistency guarantees, including the role of a high-water-mark in preventing dirty reads.
- Client-side service discovery — producers/consumers don't hardcode "the" broker; they discover and adapt to the current leader.
- Building a Kafka-like append-only log with offset-based sequential reads from first principles.

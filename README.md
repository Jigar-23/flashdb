# FlashDB ⚡

[![Go Version](https://img.shields.io/badge/go-1.23+-00ADD8.svg?style=flat&logo=go)](https://go.dev/)
[![Protocol](https://img.shields.io/badge/protocol-RESP%20v2-DC382D.svg?style=flat&logo=redis)](https://redis.io/docs/reference/protocol-spec/)
[![Throughput](https://img.shields.io/badge/throughput-163k%20ops%2Fsec-success.svg?style=flat)]()
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A high-throughput, low-latency in-memory key-value store and cache built from scratch in Go. 

FlashDB implements a core command subset using the standard **Redis Serialization Protocol (RESP v2)** wire format, allowing standard Redis clients and CLI tools (`redis-cli`, `redis-benchmark`) to connect directly over TCP without custom protocol drivers.

---

## 🏛️ System Architecture

```
                      +-----------------------------+
                      |   Standard Redis Clients    |
                      | (redis-cli, apps, SDKs)     |
                      +--------------+--------------+
                                     |  TCP (Port 6379)
                                     v
                      +-----------------------------+
                      |     TCP Socket Listener     |
                      |   (Goroutine Pool + RESP)   |
                      +--------------+--------------+
                                     |
               +---------------------+---------------------+
               |                                           |
               v                                           v
+-------------------------------+             +---------------------------+
|    RESP v2 Streaming Parser   |             |   Command Router & Dispatcher |
|     (Buffered bufio Engine)   |             |   (Type/Argument Validator)|
+--------------+----------------+             +-------------+-------------+
               |                                           |
               +---------------------+---------------------+
                                     |
                                     v
                 +---------------------------------------+
                 |       64-Shard Memory Engine          |
                 |  (FNV-1a Hashed Concurrency Shards)   |
                 +-------------------+-------------------+
                                     |
         +---------------------------+---------------------------+
         |                           |                           |
         v                           v                           v
+-----------------+         +------------------+        +-----------------+
|     O(1) LRU    |         |   Millisecond    |        |  Append-Only    |
| Eviction Engine |         |  TTL Expiration  |        |  File (AOF)     |
| (DLL + HashMap) |         | (Active/Passive) |        |  Durability     |
+-----------------+         +------------------+        +-----------------+
```

---

## 🚀 Key Engineering Highlights

### 1. 64-Shard Partitioned Memory Model
* **The Problem:** A single global `sync.RWMutex` over a central hash table causes severe CPU core contention when thousands of concurrent goroutines perform writes.
* **The Solution:** FlashDB splits memory into **64 independent shards** using FNV-1a hash distribution (`hash(key) % 64`). Writes to different keys acquire isolated shard locks without waiting on each other, unlocking near-linear multi-core CPU scaling.

### 2. Streaming RESP v2 Wire Protocol
* Built on top of `bufio.Reader` and streaming byte parsers, the protocol engine decodes Simple Strings (`+`), Errors (`-`), Integers (`:`), Bulk Strings (`$`), and Arrays (`*`) directly from the TCP socket buffer into typed commands.

### 3. $O(1)$ LRU Eviction Engine
* Implements a custom Doubly Linked List paired with a pointer index map. Cache access, eviction, and removals operate in strict **$O(1)$ time complexity**.
* Configurable memory caps (`--maxmemory`) automatically trigger LRU eviction on `SET` operations when limits are reached.

### 4. Active & Passive TTL Expiration
* **Passive Expiration:** Expired keys are lazily pruned upon read operations (`GET`, `EXISTS`).
* **Active Background Sweeper:** A periodic background routine probabilistically samples random keys from shards and clears expired entries (Redis-style adaptive sweep), ensuring memory is reclaimed even if keys are never accessed again.

### 5. Crash Durability: Append-Only File (AOF) & Rewrite Compaction
* Logs all mutating state commands into an Append-Only File with configurable `fsync` policies:
  * `always`: Synchronous disk commit on every operation.
  * `everysec`: Background disk commit every 1000ms.
  * `no`: OS buffer flush.
* **AOF Compaction:** Supports live background rewrite (`REWRITEAOF`), taking an in-memory snapshot and collapsing millions of incremental modifications into minimal state-setting commands.

---

## 📊 Performance Benchmarks

Benchmarked locally on Apple Silicon (M-series, Darwin arm64) using `flashdb-cli` over TCP:

| Metric | Result |
| :--- | :--- |
| **Throughput** | **~158,000 – 163,000 operations / sec** |
| **Duration (50k ops)** | **~0.31 – 0.32 seconds** |
| **Concurrent Clients** | 50 clients |
| **Latency (p50)** | **~200 – 210 µs** (0.20 ms) |
| **Latency (p95)** | **~770 – 800 µs** (0.78 ms) |
| **Latency (p99)** | **~1.3 – 1.6 ms** |

---

## 🛠️ Supported Commands

* **Connection & Server:** `PING`, `ECHO`, `INFO`, `DBSIZE`, `FLUSHDB`, `COMMAND`, `REWRITEAOF`
* **Key Management:** `DEL`, `EXISTS`, `TYPE`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`
* **Strings:** `SET [EX seconds] [PX millis]`, `GET`, `MSET`, `MGET`, `INCR`, `DECR`, `APPEND`, `STRLEN`
* **Lists:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`
* **Hashes:** `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN`
* **Sets:** `SADD`, `SMEMBERS`, `SREM`, `SISMEMBER`, `SCARD`

---

## 💻 Quickstart

### 1. Run with Docker Compose
```bash
docker compose up -d
```

### 2. Run Directly from Source
```bash
# Build binaries
make build

# Start FlashDB Server on port 6379
./bin/flashdb-server --port 6379

# In another terminal, connect using official redis-cli
redis-cli -p 6379
127.0.0.1:6379> SET user:1 "Alice"
OK
127.0.0.1:6379> GET user:1
"Alice"
127.0.0.1:6379> INCR visitor_counter
(integer) 1
```

### 3. Run High-Speed Benchmark
```bash
./bin/flashdb-cli --bench -p 6379 -c 50 -n 50000
```

### 4. Run Tests & Race Detector
```bash
make test-race
```

---

## ⚖️ Design Decisions & Trade-Offs

1. **Why 64 Shards instead of `sync.Map`?**  
   Go's built-in `sync.Map` is optimized for append-only or read-heavy workloads where keys are rarely updated. In a high-throughput cache with frequent `SET`, `DEL`, and `EXPIRE` mutations, a partitioned array of 64 mutex-protected maps consistently outperforms `sync.Map` by over 40% due to deterministic cache locality and isolated lock domains.

2. **Why RESP v2 instead of gRPC/Protobuf?**  
   Adhering to RESP v2 ensures wire compatibility with standard Redis tooling for supported commands, demonstrating low-level TCP socket parsing, byte manipulation, and network framing without third-party RPC frameworks.

3. **Why AOF over RDB?**  
   AOF gives finer granularity for crash durability without requiring expensive process fork (`fork()`) memory copying, which can cause severe latency spikes on memory-constrained servers.

---

## 📄 License
MIT License. Created by [Jigar](https://github.com/Jigar-23).

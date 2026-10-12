# FlashDB

A sharded in-memory key-value store in Go with RESP v2 support.

FlashDB is a lightweight in-memory cache and key-value database built from scratch in Go. It implements the Redis Serialization Protocol (RESP v2), meaning standard Redis tools like `redis-cli` or language client libraries can connect directly over TCP without custom drivers.

I built this project to get hands-on experience with database internals: low-level wire protocol parsing, memory sharding to reduce lock contention under concurrency, cache eviction algorithms, and crash durability with append-only logs.

---

## Architecture

```
                      +-----------------------------+
                      |   Standard Redis Clients    |
                      | (redis-cli, redis-benchmark)|
                      +--------------+--------------+
                                     |  TCP (Port 6379)
                                     v
                      +-----------------------------+
                      |     TCP Socket Listener     |
                      |    (Goroutine per conn)     |
                      +--------------+--------------+
                                     |
                                     v
                      +-----------------------------+
                      |   RESP v2 Streaming Parser  |
                      |   (Buffered bufio Reader)   |
                      +--------------+--------------+
                                     |
                                     v
                      +-----------------------------+
                      |       Command Router        |
                      +--------------+--------------+
                                     |
                                     v
                      +-----------------------------+
                      |       64-Shard Engine       |
                      |    (FNV-1a Hash Shards)     |
                      +--------------+--------------+
                                     |
         +---------------------------+---------------------------+
         |                           |                           |
         v                           v                           v
+-----------------+         +------------------+        +-----------------+
|     O(1) LRU    |         |   TTL Sweeper    |        |  Append-Only    |
| Eviction Engine |         | (Active/Passive) |        |  File (AOF)     |
| (DLL + HashMap) |         |                  |        |  Durability     |
+-----------------+         +------------------+        +-----------------+
```

---

## How It Works

### 1. 64-Shard Partitioned Memory
Using a single global `sync.RWMutex` around an in-memory map creates heavy lock contention when dozens of concurrent client connections perform writes. 

FlashDB partitions its keyspace across 64 independent shards using an FNV-1a hash (`hash(key) % 64`). Each shard maintains its own `sync.RWMutex`. Writes to different keys acquire independent shard locks, allowing concurrent goroutines to write in parallel without blocking each other.

### 2. Streaming RESP v2 Parser
Instead of using external frameworks or HTTP, the server parses raw TCP byte streams directly using `bufio.Reader`. It decodes the core RESP v2 data types:
* Simple Strings (`+`)
* Errors (`-`)
* Integers (`:`)
* Bulk Strings (`$`)
* Arrays (`*`)

### 3. O(1) LRU Eviction
When `--maxmemory` is configured, adding new keys beyond the threshold triggers eviction. Eviction is handled by a doubly linked list paired with a hash map lookup:
* Cache hits move elements to the head in $O(1)$ time.
* When memory is exhausted, the tail element is evicted in $O(1)$ time.

### 4. Active and Passive TTL Expiration
* **Passive expiration:** Keys are checked when accessed (`GET`, `EXISTS`). If expired, they are deleted on the fly and reported as non-existent.
* **Active background sweeper:** A background goroutine wakes up every 100ms, selects a random shard, samples up to 20 keys with TTLs, and removes any expired ones. This prevents keys that are never read again from leaking memory.

### 5. Append-Only File (AOF) & Rewrite Compaction
* Every mutating command is written to an append-only log with configurable `fsync` policies (`always`, `everysec`, or `no`).
* On restart, the engine replays the AOF file sequentially to rebuild the in-memory state.
* **AOF Compaction:** Running `REWRITEAOF` takes an in-memory snapshot of all active keys and rewrites the log with minimal `SET` statements, collapsing redundant updates into a compact file.

---

## Benchmarks

Benchmarked locally on an Apple M-series Mac (Darwin arm64) over TCP loopback using the built-in benchmark runner:

```bash
./bin/flashdb-cli --bench -p 6379 -c 50 -n 50000
```

| Metric | Result |
| :--- | :--- |
| **Throughput** | ~158,000 – 163,000 ops/sec |
| **Duration (50,000 ops)** | ~0.31 – 0.32 s |
| **Concurrency** | 50 clients |
| **Latency p50** | ~0.20 ms (203 µs) |
| **Latency p95** | ~0.79 ms (797 µs) |
| **Latency p99** | ~1.37 ms |

*(Note: These figures reflect local loopback testing. In production, network transit and disk fsync settings will dictate overall latency.)*

---

## Supported Commands

* **Server & Utility:** `PING`, `ECHO`, `INFO`, `DBSIZE`, `FLUSHDB`, `COMMAND`, `REWRITEAOF`
* **Keys:** `DEL`, `EXISTS`, `TYPE`, `EXPIRE`, `PEXPIRE`, `TTL`, `PTTL`, `PERSIST`
* **Strings:** `SET [EX seconds] [PX millis]`, `GET`, `MSET`, `MGET`, `INCR`, `DECR`, `APPEND`, `STRLEN`
* **Lists:** `LPUSH`, `RPUSH`, `LPOP`, `RPOP`, `LRANGE`, `LLEN`
* **Hashes:** `HSET`, `HGET`, `HDEL`, `HGETALL`, `HEXISTS`, `HLEN`
* **Sets:** `SADD`, `SMEMBERS`, `SREM`, `SISMEMBER`, `SCARD`

---

## Getting Started

### Prerequisites
* Go 1.23 or newer
* (Optional) `redis-cli` for testing connections
* (Optional) Docker

### Build and Run

1. **Clone the repository:**
   ```bash
   git clone https://github.com/Jigar-23/flashdb.git
   cd flashdb
   ```

2. **Build the server and CLI:**
   ```bash
   make build
   ```

3. **Start the server:**
   ```bash
   ./bin/flashdb-server --port 6379
   ```

4. **Connect using standard `redis-cli`:**
   ```bash
   redis-cli -p 6379
   127.0.0.1:6379> SET name "FlashDB"
   OK
   127.0.0.1:6379> GET name
   "FlashDB"
   127.0.0.1:6379> INCR counter
   (integer) 1
   ```

5. **Run tests and race detector:**
   ```bash
   make test-race
   ```

6. **Run with Docker Compose:**
   ```bash
   docker compose up -d
   ```

---

## Design Decisions & Trade-Offs

* **64 Shards vs. `sync.Map`:** Go's `sync.Map` works well for append-only or cache-read workloads with few updates. For an in-memory cache with frequent `SET`, `DEL`, and `EXPIRE` writes, partitioned maps with per-shard `RWMutex` significantly reduce lock contention.
* **RESP v2 Wire Format vs. JSON/Protobuf:** Implementing RESP keeps the server compatible with the existing Redis ecosystem, allowing testing with official tools like `redis-cli` and `redis-benchmark`.
* **AOF vs. RDB Snapshots:** AOF provides straightforward durability without needing `fork()`. In Go, forking a process can be tricky with runtime threads and goroutines, making an append-only log with snapshot rewrite a cleaner fit.

---

## Scope & Limitations

To be clear about what this project is and isn't:
* This is a focused implementation of core Redis commands, not a full 1:1 replacement. Features like Redis Pub/Sub, Lua scripting, transactions (`MULTI`/`EXEC`), and cluster replication are not implemented.
* Sharding is strictly in-process across CPU cores, not distributed across multiple nodes.
* Memory estimation for LRU tracks raw payload sizes rather than low-level Go allocator overhead.

---

## License

MIT License. Built by [Jigar](https://github.com/Jigar-23).

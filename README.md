# redis

A small Redis-style in-memory key-value server written from scratch in Rust. It speaks RESP2 and supports a focused subset of Redis commands.

This is a personal portfolio project focused on the internals behind a Redis-style server: RESP parsing, non-blocking TCP I/O, command execution, key expiry, append-only persistence, memory accounting, and cache eviction policies.

It is intentionally smaller than Redis itself. The goal is to show a working systems project with a clear execution path, a tested protocol layer, and implementation choices that are easy to inspect.

## Features

- RESP2 parser and encoder for simple strings, errors, integers, bulk strings, null bulk strings, arrays, and null arrays.
- Non-blocking TCP server built with `mio`.
- Multiple client support with read and write buffering.
- Pipelined command handling.
- Binary-safe key and value storage.
- Core Redis-style commands:
  - `PING`
  - `ECHO`
  - `SET`
  - `SET key value EX seconds`
  - `GET`
  - `DEL`
  - `EXISTS`
  - `EXPIRE`
  - `TTL`
  - `INFO`
- Lazy expiration when keys are read.
- Active expiration sampling on the server loop.
- Append-only file persistence with startup replay.
- Configurable AOF fsync behavior:
  - `always`
  - `everysec`
  - `no`
- Memory accounting for stored values and expiry metadata.
- Eviction policy implementation hooks with support for:
  - `noeviction`
  - `allkeys-random`
  - `volatile-random`
  - `volatile-ttl`
  - `allkeys-sieve`
- SIEVE-style second-chance eviction for touched keys.
- `INFO` output for server version, git hash, build type, memory usage, key count, expiry count, and eviction policy.
- `mimalloc` as the global allocator.
- Unit tests covering protocol parsing, command behavior, expiry, eviction, pipelining, and AOF replay.

## Quick Start

Install a Rust toolchain that supports the 2024 edition, then run:

```sh
cargo test
cargo run
```

By default the server listens on `127.0.0.1:6379`. Append-only persistence is available but disabled by default.

In another terminal, connect with `redis-cli`:

```sh
redis-cli -p 6379 PING
redis-cli -p 6379 SET greeting hello
redis-cli -p 6379 GET greeting
redis-cli -p 6379 SET session abc EX 10
redis-cli -p 6379 TTL session
redis-cli -p 6379 EXISTS greeting session missing
redis-cli -p 6379 DEL greeting
redis-cli -p 6379 INFO
```

You can also use any RESP-compatible TCP client. The server speaks RESP over a normal TCP socket.

## Running

Start the server with default settings:

```sh
cargo run
```

Listen on a different address:

```sh
cargo run -- --host 127.0.0.1 --port 6380
```

Enable append-only persistence:

```sh
cargo run -- --aof-enabled
```

Use a custom AOF file:

```sh
cargo run -- --aof-enabled --aof-path ./data/db.aof
```

Use `everysec` fsync behavior:

```sh
cargo run -- --aof-enabled --aof-fsync-policy everysec
```

Available CLI options:

```text
Usage: redis [OPTIONS]

Options:
  -p, --port <PORT>                          Port to listen for incoming connections [default: 6379]
      --host <HOST>                          Host to listen for incoming connections [default: 127.0.0.1]
      --aof-enabled                          Enable append-only file persistence
      --aof-path <AOF_PATH>                  Path to the append-only file [default: db.aof]
      --aof-fsync-policy <AOF_FSYNC_POLICY>  When to flush AOF writes to disk [default: always] [possible values: always, everysec, no]
  -h, --help                                 Print help
  -V, --version                              Print version
```

AOF is disabled by default in the current CLI configuration. Pass `--aof-enabled` to enable append-only persistence.

## Command Support

| Command                    | Example               | Notes                                                                                    |
| -------------------------- | --------------------- | ---------------------------------------------------------------------------------------- |
| `PING`                     | `PING`                | Returns `PONG`.                                                                          |
| `PING message`             | `PING hello`          | Echoes the provided message.                                                             |
| `ECHO message`             | `ECHO hello`          | Returns the provided message.                                                            |
| `SET key value`            | `SET name sazid`      | Stores a binary-safe value. A plain `SET` clears any existing TTL.                       |
| `SET key value EX seconds` | `SET token abc EX 30` | Stores a value with a TTL. TTL must be greater than zero.                                |
| `GET key`                  | `GET name`            | Returns a bulk string or null bulk string.                                               |
| `DEL key [key ...]`        | `DEL a b c`           | Returns the number of deleted keys.                                                      |
| `EXISTS key [key ...]`     | `EXISTS a b c`        | Returns the number of existing keys.                                                     |
| `EXPIRE key seconds`       | `EXPIRE token 30`     | Returns `1` when the key is updated, `0` when missing. Non-positive TTL deletes the key. |
| `TTL key`                  | `TTL token`           | Returns remaining seconds, `-1` for no expiry, and `-2` for missing keys.                |
| `INFO`                     | `INFO`                | Returns server and memory metadata.                                                      |

Mutating commands that successfully change state are appended to the AOF:

- `SET`
- `SET ... EX`
- `DEL` when at least one key is deleted
- `EXPIRE` when the target key exists

Read-only commands are not persisted.

## Architecture

The project is intentionally compact and organized around the major parts of a Redis-style server:

```text
src/
  main.rs                  CLI entry point
  config.rs                CLI flags and runtime config
  resp.rs                  RESP2 parser and encoder
  db.rs                    In-memory key-value database, expiry, memory accounting
  eviction.rs              Eviction policy selection and memory-limit enforcement
  server/
    mod.rs                 TCP event loop, clients, pipelining, AOF replay
    aof.rs                 Append-only file writer and fsync policies
    commands/
      mod.rs               Command dispatch
      set.rs               SET and SET EX
      get.rs               GET
      del.rs               DEL
      exists.rs            EXISTS
      expire.rs            EXPIRE
      ttl.rs               TTL
      ping.rs              PING
      echo.rs              ECHO
      info.rs              INFO
```

Request flow:

1. `mio` polls the server socket and client sockets.
2. Client bytes are appended to a per-client read buffer.
3. `resp::decode_one` parses one complete RESP value at a time.
4. The command dispatcher validates arguments and applies the command to `RedisDb`.
5. Successful mutating commands are written to the append-only file.
6. Encoded RESP responses are queued in the client's write buffer.
7. The event loop flushes pending responses when the socket is writable.

This design keeps networking, parsing, command dispatch, persistence, and storage mostly separate, which makes the code easier to test without needing a live TCP socket for every behavior.

## Persistence

The server uses append-only file persistence:

- On startup, the configured AOF file is read if it exists.
- Each command in the AOF is decoded as RESP and replayed into the in-memory database.
- During runtime, successful mutating commands are appended in RESP form.
- Fsync behavior is controlled by `--aof-fsync-policy`.

The supported fsync policies are:

| Policy     | Behavior                                                                                    |
| ---------- | ------------------------------------------------------------------------------------------- |
| `always`   | Sync after every appended command. Safest, slowest.                                         |
| `everysec` | Sync roughly once per second. Balanced durability and throughput.                           |
| `no`       | Let the operating system decide when data reaches disk. Fastest, least explicit durability. |

This is AOF-only persistence. There is no RDB snapshot format.

## Expiration

Key expiry uses two strategies:

- Lazy expiry: commands such as `GET`, `EXISTS`, and `TTL` check whether a key has expired before returning a result.
- Active expiry: the server loop periodically samples expiring keys and deletes stale entries.

The active expiry loop samples a bounded number of keys so expiry cleanup does not monopolize the event loop.

## Eviction

The database tracks an estimated memory usage for values and expiry metadata. When a memory limit is configured at the database layer, eviction can be enforced with several policies:

| Policy            | Behavior                                                                 |
| ----------------- | ------------------------------------------------------------------------ |
| `noeviction`      | Reject writes that would exceed the configured memory limit.             |
| `allkeys-random`  | Evict random keys from the full keyspace.                                |
| `volatile-random` | Evict random keys that have a TTL.                                       |
| `volatile-ttl`    | Evict keys with the shortest TTL, using sampling for larger expiry sets. |
| `allkeys-sieve`   | Use a SIEVE-style second-chance scan over all keys.                      |

`allkeys-sieve` is the default database policy. Reads mark keys as touched, and the eviction hand gives touched keys one second chance before selecting a victim.

The current CLI does not yet expose `maxmemory` or eviction policy flags. Those paths are implemented and tested at the database layer and are natural next steps for runtime configuration.

## Testing

Run the test suite:

```sh
cargo test
```

The suite currently covers:

- RESP encoding and decoding.
- Invalid and incomplete protocol input.
- Command argument validation.
- Case-insensitive command dispatch.
- Key/value mutation and lookup.
- TTL behavior and lazy expiry.
- Active expiry sampling.
- AOF replay and malformed AOF handling.
- Pipelined command processing.
- Memory accounting.
- Eviction policies and out-of-memory responses.

At the time this README was written, the test suite contains 171 tests.

## Benchmarks

I benchmarked this server against the official `redis-server` using the same client, command mix, and request profile.

Benchmark setup:

- Date: June 1, 2026.
- Machine: Apple M4, 10 CPU cores, 24 GB memory.
- OS: macOS Darwin 25.5.0.
- Client: `redis-benchmark`.
- Official Redis: `redis-server` v8.6.3.
- Workload: 100,000 requests per command, 50 concurrent clients, no pipelining.
- Persistence: disabled for both servers.
- Transport: localhost TCP.
- Build: this project built with `cargo build --release`.

Results:

| Command | This server req/s | redis-server req/s | This / Redis | This p50 ms | Redis p50 ms |
| --- | ---: | ---: | ---: | ---: | ---: |
| `PING` | 230,947 | 238,095 | 0.97x | 0.111 | 0.111 |
| `ECHO` | 245,700 | 238,663 | 1.03x | 0.111 | 0.111 |
| `SET` | 244,499 | 241,546 | 1.01x | 0.111 | 0.111 |
| `SET EX` | 244,499 | 243,309 | 1.00x | 0.111 | 0.111 |
| `GET` | 244,499 | 239,234 | 1.02x | 0.111 | 0.111 |
| `DEL` | 246,305 | 239,808 | 1.03x | 0.111 | 0.111 |
| `EXISTS` | 246,305 | 240,964 | 1.02x | 0.111 | 0.111 |
| `EXPIRE` | 245,098 | 242,718 | 1.01x | 0.111 | 0.111 |
| `TTL` | 242,718 | 240,385 | 1.01x | 0.111 | 0.111 |
| `INFO` | 240,385 | 68,353 | 3.52x | 0.111 | 0.679 |

Average throughput excluding `INFO` was 243,397 requests/second for this server and 240,525 requests/second for Redis, or about 101% of Redis throughput across the comparable command set.

`INFO` is not a fair throughput win because this server returns a small custom metadata payload while Redis returns a much larger response. The comparable commands are also close to the local `redis-benchmark`/localhost ceiling on this machine, so small differences around 240,000 requests/second should not be over-interpreted.

### Benchmark Matrix

The no-pipeline benchmark above is useful as a simple latency-oriented smoke test, but it is limited by localhost round trips and the benchmark client. To expose more server-side work, I also ran a small matrix using pipelining, threaded benchmark clients, and larger payloads.

The matrix can be rerun locally with:

```sh
scripts/benchmark_matrix.sh
```

The script builds the release binary, starts this server on port `6380`, starts `redis-server` on port `6381`, runs the matrix, prints raw `redis-benchmark` CSV output plus summary tables, and stops both servers on exit.

Useful overrides:

```sh
THIS_PORT=6380 REDIS_PORT=6381 REQUESTS=100000 CLIENTS=50 PIPELINE=16 THREADS=4 PAYLOAD_SIZE=1024 scripts/benchmark_matrix.sh
```

Matrix setup:

- Date: June 1, 2026.
- Requests: 1,000,000 per command.
- Clients: 50.
- Pipelining: 16 requests.
- Threaded mode: `redis-benchmark --threads 4`.
- 1KB payload mode: `ECHO`, `SET`, and `GET` with 1,024-byte values.

Summary:

| Mode | This server avg req/s | redis-server avg req/s | This / Redis |
| --- | ---: | ---: | ---: |
| Pipeline 16 | 2,996,018 | 2,451,847 | 1.22x |
| Pipeline 16, 4 benchmark threads | 1,997,339 | 1,850,078 | 1.08x |
| Pipeline 16, 4 benchmark threads, 1KB payload | 1,553,633 | 1,775,855 | 0.87x |

Pipeline 16 results:

| Command | This server req/s | redis-server req/s | This / Redis |
| --- | ---: | ---: | ---: |
| `PING` | 2,985,075 | 3,184,713 | 0.94x |
| `ECHO` | 3,095,975 | 2,941,177 | 1.05x |
| `SET` | 2,732,241 | 1,980,198 | 1.38x |
| `SET EX` | 2,392,345 | 1,342,282 | 1.78x |
| `GET` | 3,194,888 | 2,583,979 | 1.24x |
| `DEL` | 3,322,259 | 2,941,177 | 1.13x |
| `EXISTS` | 3,278,689 | 2,652,520 | 1.24x |
| `EXPIRE` | 2,747,253 | 1,766,785 | 1.55x |
| `TTL` | 3,215,434 | 2,673,797 | 1.20x |

Pipeline 16 with 4 benchmark threads:

| Command | This server req/s | redis-server req/s | This / Redis |
| --- | ---: | ---: | ---: |
| `PING` | 2,000,000 | 2,000,000 | 1.00x |
| `ECHO` | 1,996,008 | 2,000,000 | 1.00x |
| `SET` | 2,000,000 | 2,000,000 | 1.00x |
| `SET EX` | 1,996,008 | 1,333,333 | 1.50x |
| `GET` | 1,996,008 | 1,996,008 | 1.00x |
| `DEL` | 1,996,008 | 1,996,008 | 1.00x |
| `EXISTS` | 1,996,008 | 1,996,008 | 1.00x |
| `EXPIRE` | 1,996,008 | 1,333,333 | 1.50x |
| `TTL` | 2,000,000 | 1,996,008 | 1.00x |

Pipeline 16 with 4 benchmark threads and 1KB payloads:

| Command | This server req/s | redis-server req/s | This / Redis |
| --- | ---: | ---: | ---: |
| `ECHO` | 1,331,558 | 1,996,008 | 0.67x |
| `SET` | 1,333,333 | 1,331,558 | 1.00x |
| `GET` | 1,996,008 | 2,000,000 | 1.00x |

Observations:

- Pipelining is the most useful change to the benchmark. It moves the test from roughly 240,000 requests/second to multi-million request throughput by amortizing round trips and syscalls.
- Adding `--threads 4` did not help on this machine. The threaded benchmark run flattened around 2,000,000 requests/second and was lower than the single benchmark-thread pipelined run for this server, likely because the server itself is single-threaded and extra benchmark threads add scheduling overhead.
- Increasing payload size makes copy and buffer behavior more visible. The 1KB `SET` and `GET` results are close to Redis, while 1KB `ECHO` is behind Redis; that points toward request parsing or response payload handling rather than storage.
- Pipelined benchmarks are throughput tests, not single-request round-trip latency tests. Both views are useful, but they answer different questions.
- These command benchmarks repeatedly operate on a small number of keys. They are useful for hot-path throughput comparisons, but they do not replace high-cardinality workload tests.

### Interesting Notes

An earlier run of this same benchmark measured the server with several `println!` calls still present on the connection and request paths. Those calls printed accepted/disconnected clients, raw socket reads, and parsed RESP values. Even when stdout was redirected away from the terminal, Rust still had to format those values, lock stdout, and perform the writes.

Removing those hot-path prints caused the largest improvement in this workload. Later small cleanups around AOF persistence, response encoding, and database lookups kept the result in the same localhost-limited range:

| Command | Previous req/s | Latest req/s | Change |
| --- | ---: | ---: | ---: |
| `PING` | 196,464 | 230,947 | +18% |
| `ECHO` | 185,529 | 245,700 | +32% |
| `SET` | 134,409 | 244,499 | +82% |
| `SET EX` | 122,399 | 244,499 | +100% |
| `GET` | 176,056 | 244,499 | +39% |
| `DEL` | 172,414 | 246,305 | +43% |
| `EXISTS` | 173,310 | 246,305 | +42% |
| `EXPIRE` | 162,602 | 245,098 | +51% |
| `TTL` | 176,367 | 242,718 | +38% |

Across the comparable commands, average throughput improved from 166,617 to 243,397 requests/second, a 46% increase. The main lesson is that logging in a hot path is still work, even if the output is discarded later.

## What This Demonstrates

This project is meant to be read as much as it is meant to be run. It demonstrates:

- Implementing a network protocol parser without relying on Redis internals.
- Building a non-blocking event loop around `mio`.
- Handling partial reads, buffered writes, and pipelined requests.
- Modeling Redis-style command semantics in small, focused modules.
- Replaying an append-only log into an in-memory state machine.
- Tracking approximate memory usage and enforcing eviction policies.
- Writing tests around protocol, storage, persistence, and server behavior.

## Limitations

This is not a production replacement for Redis. It is a focused educational and portfolio implementation.

Notable limitations:

- Supports only a small command subset.
- Single-process, single-threaded server loop.
- No replication.
- No clustering.
- No transactions.
- No Lua scripting.
- No pub/sub.
- No streams, sets, hashes, or sorted sets.
- No authentication or ACL system.
- No TLS.
- No RDB snapshots.
- Benchmarks are local scripts, not a CI performance suite.

## Roadmap

Potential next steps:

- Expose `maxmemory` and eviction policy through CLI flags.
- Add integration tests that drive the server over TCP with `redis-cli` or a RESP client.
- Add parser, command-dispatch, AOF fsync, and eviction microbenchmarks.
- Implement additional commands such as `MGET`, `MSET`, `INCR`, `DECR`, and `FLUSHDB`.
- Add a clean shutdown path.
- Add GitHub Actions for formatting, clippy, and tests.
- Add Docker packaging for quick demos.

## Project Status

The server is usable for local experimentation and for demonstrating the core mechanics of a Redis-style database server. It is best treated as a learning project and portfolio artifact rather than infrastructure.

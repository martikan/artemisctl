[![CI/CD](https://github.com/martikan/artemisctl/actions/workflows/ci-cd.yml/badge.svg)](https://github.com/martikan/artemisctl/actions/workflows/ci-cd.yml)
[![Release](https://img.shields.io/github/v/release/martikan/artemisctl)](https://github.com/martikan/artemisctl/releases/latest)
[![codecov](https://codecov.io/gh/martikan/artemisctl/branch/main/graph/badge.svg?token=RL2Z3Y7CLV)](https://codecov.io/gh/martikan/artemisctl)

# artemisctl

A command-line tool for managing and recovering Apache ActiveMQ Artemis
brokers. It checks broker status and health, browses queues non-destructively,
can cordon the broker to stop new messages before an export, performs an
emergency drain of every message into a local store, and later redelivers those
messages once the broker is healthy again. It can also produce messages onto a
queue for testing and load generation.

Communicates with the broker over AMQP 1.0, using the `activemq.management`
address for management operations.

> **Contributing?** See [`docs/CONTRIBUTING.md`](docs/CONTRIBUTING.md) for how to
> report issues, run the tests, and open a pull request.

## Install

Three ways to get the CLI: build from source, download a released binary, or
run the container image.

### Build from source

Requires Go (version pinned in `go.mod`). Clone the repo and use `make`:

```bash
git clone https://github.com/martikan/artemisctl.git
cd artemisctl
make build      # produces bin/artemisctl
make help       # list all targets (build, test, coverage, release, clean)
```

### Download a release binary

Tagged releases (`v*`) publish prebuilt binaries on the GitHub Releases page,
one per OS/arch:

| Asset | Platform |
| --- | --- |
| `artemisctl-linux-x64` | Linux amd64 (built `GOAMD64=v3`) |
| `artemisctl-linux-arm` | Linux arm64 |
| `artemisctl-darwin-x64` | macOS Intel (amd64) |
| `artemisctl-darwin-arm` | macOS Apple Silicon (arm64) |

```bash
curl -sSLO https://github.com/martikan/artemisctl/releases/latest/download/artemisctl-linux-x64
chmod +x artemisctl-linux-x64
./artemisctl-linux-x64 status
```

Each release is produced by the [SLSA3](https://slsa.dev) secure builder and
ships a `.intoto.jsonl` provenance receipt alongside the binaries, so a download
can be cryptographically verified back to the exact source and build:

```bash
slsa-verifier verify-artifact artemisctl-linux-x64 \
  --provenance-path artemisctl-linux-x64.intoto.jsonl \
  --source-uri github.com/martikan/artemisctl
```

### Container image (GHCR)

Tagged releases also push a container image to the GitHub Container Registry,
tagged both `:latest` and the version:

```bash
docker pull ghcr.io/martikan/artemisctl:latest

# The entrypoint is the CLI itself — pass subcommands/flags directly.
docker run --rm ghcr.io/martikan/artemisctl:latest status --url broker.internal:61616
```

The image is a Google
[distroless](https://github.com/GoogleContainerTools/distroless) `static:nonroot`
base holding only the statically linked binary — no shell, no package manager,
runs as a non-root user (UID 65532).

## Connection

Every command connects to a single broker. Global flags:

| Flag | Default | Description |
| --- | --- | --- |
| `--url` | `127.0.0.1:61616` | AMQP 1.0 broker address `host:port` |
| `-u`, `--username` | `artemis` | Broker username |
| `-p`, `--password` | `artemis` | Broker password |
| `--timeout` | `30s` | Max time to establish the broker connection before failing fast |

`--timeout` bounds only the initial connection, so an unresponsive broker
errors out instead of hanging forever. It does **not** cap the running time of a
long `export` drain or `redeliver` replay, which may legitimately exceed it.

Prefer the `ARTEMIS_PASSWORD` environment variable over `-p` for secrets; when
set, it overrides `-p`.

```bash
export ARTEMIS_PASSWORD='…'
artemisctl status --url broker.internal:61616 -u admin
```

## Commands

### `status`

List queues and their message counts (descending). Internal and temporary
queues (`activemq.*`, `$`-prefixed, 36-char temp-queue UUIDs) are filtered out.

```bash
artemisctl status
```

### `health`

Report disk-store usage, memory usage, and producer-blocking state, mapped to a
single verdict. The process exit code reflects the verdict, so the check is
scriptable.

| Verdict | Condition | Exit |
| --- | --- | --- |
| `OK` | all usage < 70% | 0 |
| `DEGRADED` | any usage 70–90% | 0 |
| `CRITICAL` | any usage > 90%, or the broker is blocking producers | non-zero |

```bash
artemisctl health
```

**Producer-blocking** is derived from the broker's disk-full protection: Artemis
blocks all producers once disk-store usage reaches the configured
`max-disk-usage` (read live via `broker.getMaxDiskUsage`, default 90%). This
tracks the broker's own block threshold rather than a hardcoded cutoff, so it
also fires under a custom lower `max-disk-usage`. (The dedicated
`broker.isDiskFull` operation is not available on Artemis 2.31.2.)

### `browse`

Non-destructively peek at the messages on a queue — nothing is consumed.

| Flag | Default | Description |
| --- | --- | --- |
| `--queue` | *(required)* | Queue to browse |
| `--limit` | `20` | Max messages to list |
| `--offset` | `0` | Skip the first N messages |
| `--message` | | Show the full body + properties for a single message ID |

```bash
artemisctl browse --queue orders --limit 50
artemisctl browse --queue orders --message 21
```

### `produce`

Send messages to a queue — either generated synthetic test data or messages
read from a JSON file. Use it to seed a queue for testing, reproduce a message
copied out of the Artemis web console, or drive load.

| Flag | Default | Description |
| --- | --- | --- |
| `--queue` | *(required)* | Target queue (authoritative — a file message's `address` is ignored) |
| `--count` | `1` | Number of generated messages (ignored with `--file`) |
| `--size` | `256` | Generated message body size in bytes (ignored with `--file`) |
| `--rate` | `0` | Max messages per second in total, across all workers (`0` = unlimited) |
| `--workers` | `1` | Parallel sender sessions (`1` = ordered, sequential) |
| `--property` | | Application property `k=v` set on every message (repeatable) |
| `--file` | | JSON file of messages to send (see below) |

Messages are sent **durable** by default. `Ctrl-C` stops cleanly between
messages, reporting how many were sent.

Each send waits for the broker to settle the message, so single-worker
throughput is capped at one broker round-trip per message. `--workers N` opens
N senders, each on its own AMQP session over the one connection, so those
settlement waits overlap — throughput scales near-linearly with workers
(benchmarked ~110 msgs/s at 1 worker vs ~1400 msgs/s at 16 against a local
broker). With more than one worker, delivery **order is not preserved**;
`--rate` still bounds the total rate. Separate connections per worker measured
no better than sessions, so `produce` always uses a single connection.

```bash
# 1000 generated 512-byte messages at 100/sec, each tagged env=test
artemisctl produce --queue orders --count 1000 --size 512 --rate 100 --property env=test

# load generation: 8 parallel senders, as fast as the broker settles
artemisctl produce --queue orders --count 100000 --workers 8

# replay messages authored/exported in the Artemis console JSON layout
artemisctl produce --queue orders --file messages.json
```

**`--file` layout** is the Artemis web-console / `listMessagesAsJSON` shape: a
JSON **array** of message objects. The body comes from `text`; `durable`
defaults to `true` and `priority` to `4` when omitted; a non-zero `expiration`
(unix millis) sets the message's absolute expiry. Each typed property bucket is
flattened into the message's application properties with its correct AMQP type,
and any `--property` flags are merged on top (overriding a file property of the
same name). `address` and `type` are ignored.

```json
[
  {
    "address": "orders",
    "durable": true,
    "priority": 4,
    "expiration": 0,
    "type": 3,
    "text": "hello world",
    "StringProperties": { "region": "eu" },
    "IntProperties": { "attempt": 1 }
  }
]
```

### `cordon`

Block producers across the whole broker before an export, so the message set
does not grow while you drain it. Applies an address-full `FAIL` policy to the
match-all wildcard (`#`), so new sends are rejected with
`amqp:resource-limit-exceeded` ("Address … is full"). Consumers and `export` are
unaffected — the connection stays open and draining still works.

| Flag | Default | Description |
| --- | --- | --- |
| `--state-file` | `artemisctl-cordon.json` | Where to save the pre-cordon settings for `uncordon` |
| `--yes` | `false` | Skip the "this blocks ALL producers" confirmation prompt |

```bash
artemisctl cordon                 # prompts, then blocks producers broker-wide
artemisctl cordon --yes           # no prompt (scripting)
```

- **Reversible:** the wildcard's pre-cordon address-settings are saved to the
  state file so `uncordon` can restore them exactly.
- **Broker version:** requires a broker that accepts `addAddressSettings` over
  AMQP management (Artemis **2.33+**). Older brokers (e.g. 2.31.x) do not expose
  the operation over AMQP; `cordon` fails fast with a clear message and changes
  nothing.
- **Caveat:** the first message to an otherwise-empty address can slip in as the
  cordon takes hold; every subsequent send is rejected.

### `uncordon`

Lift a cordon, restoring the settings saved by `cordon`.

| Flag | Default | Description |
| --- | --- | --- |
| `--state-file` | `artemisctl-cordon.json` | Pre-cordon settings written by `cordon` |
| `--force-remove` | `false` | Remove the wildcard settings entry instead of restoring saved state |

```bash
artemisctl uncordon               # restore from the state file, then delete it
artemisctl uncordon --force-remove  # no state file? clear the wildcard entry, revert to broker defaults
```

### `export`

**Destructive.** Drains every message off every user queue into a local store
file. Messages are removed from the broker as they are written. Use this to
evacuate a dying broker; the store can be replayed later with `redeliver`.

| Flag | Default | Description |
| --- | --- | --- |
| `--out` | *(required)* | Output store file |
| `--drain-timeout` | `5s` | Idle time before a queue is considered empty |
| `--batch` | `100` | Persist/ack batch size |

```bash
artemisctl export --out broker-2026-07-09.artx
```

**No-loss invariant:** for each batch, records are written and `fsync`ed to the
store *before* the messages are acknowledged on the broker. A crash between the
`fsync` and the ack leaves a message on the broker that is drained again on the
next run — an at-least-once duplicate, absorbed by redelivery dedup (below).

### `salvage`

**Offline.** Recovers messages from a *stopped* broker's data directory
straight off disk — no broker connection at all — and writes every
recoverable message to a local `.artx` store, replayed later with
`redeliver` exactly like an `export`ed store. Use this when the broker is
dead and won't start, so `export` (which needs a live AMQP connection) isn't
an option. See [`docs/offline-recovery.md`](docs/offline-recovery.md) for
the full step-by-step runbook, including the paging and Core-protocol
caveats.

`salvage` is the native equivalent of `artemis data exp` — its output is a
`.artx` store replayed with `redeliver`, not XML consumed by `artemis data
imp`.

| Flag | Default | Description |
| --- | --- | --- |
| `--data` | *(required unless `--bindings` and `--journal` are both given)* | Broker data directory; sub-dirs derived: `bindings/`, `journal/`, `large-messages/`, `paging/` |
| `--bindings` | derived from `--data` | Bindings journal dir override |
| `--journal` | derived from `--data` | Message journal dir override |
| `--large-messages` | derived from `--data` | Large-messages dir override |
| `--paging` | derived from `--data` | Paging dir override |
| `--out` | *(required)* | Output store file |
| `--force` | `false` | Proceed even if the `server.lock` live-broker probe suggests a broker is still running |
| `--allow-skips` | `false` | Exit 0 even though some messages were skipped or corruption diagnostics were reported (unsupported/corrupt data) |

```bash
artemisctl salvage --data /mnt/rescue/data-snapshot --out rescue.artx
```

- **No broker connection.** `salvage` ignores the global `--url`/
  `--username`/`--password`/`--timeout` connection flags entirely — it never
  dials the broker, only reads the data directory files.
- **Live-broker guard:** refuses to run if `server.lock` (probed at
  `<journal>/server.lock`, `<data>/server.lock`, or `<data>/../server.lock`)
  is flock-held, i.e. a broker process is still using that exact directory —
  use `export` instead, or `--force` if you're certain the lock is stale. A
  copied/snapshotted data directory passes the guard without `--force`
  because nothing holds the flock on the copy.
- **Core-protocol messages:** decoded and exported alongside AMQP messages
  (standard, large, and paged). Because this tool is an AMQP-1.0 client, it
  cannot speak the Core wire protocol on redelivery, so `redeliver` converts
  each Core record to an equivalent AMQP message before sending; the broker
  re-converts it to Core for any Core/JMS consumer. Conversion covers the
  common body types (text/bytes/map/object/stream) and standard headers; a Core
  message that cannot be converted is skipped on redelivery (left in the store),
  never silently dropped. Legacy pre-persister core adds (userType 31, not
  produced by modern brokers) are still reported as skips.
- **At-least-once paging:** a handful of already-consumed messages from the
  most recently paged, partially-consumed page may be resurrected — matching
  the tool's existing at-least-once philosophy. See the runbook for the full
  caveat.
- **Empty result:** if nothing survives, the summary reports "salvaged 0
  messages" and **no output file is written** — never replay a store you
  didn't get an actual path for.
- **Skips and corruption fail the exit code.** Unrecoverable records are
  itemized in a `skipped:` section; damaged journal/bindings/page-file
  records (bad check-size, truncated record, broken page framing, an
  undecodable bindings-record body) are itemized in a separate
  `diagnostics:` section, naming the file and offset. Both fail the exit
  code by default, unless `--allow-skips` — a recovery tool must not
  silently lose messages. Not every `diagnostics:` entry is corruption:
  informational notes (missing large-messages/paging dir, orphaned
  large-message files, unknown-queue fallback, benign fileID-mismatch
  reuse-leftover notes) never gate the exit code. The `.artx` store is still
  written even when skips or corruption diagnostics are present; only the
  exit code is gated. See [`docs/offline-recovery.md`](docs/offline-recovery.md)
  for the full breakdown of which diagnostics gate and which don't.

### `redeliver`

Replay a store file back to the broker.

| Flag | Default | Description |
| --- | --- | --- |
| `--in` | *(required)* | Input store file |
| `--queue` | | Redirect *all* messages to this queue (default: each message's original queue) |
| `--force` | `false` | Redeliver even if broker health is `CRITICAL` |

```bash
artemisctl redeliver --in broker-2026-07-09.artx
```

- **Health-gated:** refuses to run if the broker's health verdict is
  `CRITICAL`, unless `--force` is passed — this prevents re-flooding a broker
  that is still failing.
- **Resumable:** progress is tracked in a sidecar checkpoint file
  (`<store>.ckpt`) holding the byte offset of the last redelivered record. A
  re-run resumes from there.
- **Deduplicated:** each record carries a stable id, replayed as `_AMQ_DUPL_ID`,
  so re-running a completed or interrupted redelivery cannot create duplicates —
  the broker drops the repeats.
- **Graceful `Ctrl-C`:** `SIGINT` cancels the replay after the in-flight record;
  the last checkpoint is already durable, so the run stops cleanly and resumes
  from the same point on the next invocation. `export` handles `SIGINT` the same
  way — already-drained records are `fsync`ed before the interrupt returns.

## Recovery workflow

```bash
# 1. (Optional, Artemis 2.33+) Freeze the broker so no new messages arrive
#    while you drain it.
artemisctl cordon --yes --url dying-broker:61616

# 2. Broker is failing — evacuate everything to a local store.
artemisctl export --out rescue.artx --url dying-broker:61616

# 3. Bring up a healthy broker (or repair the old one), then replay.
artemisctl health   --url healthy-broker:61616
artemisctl redeliver --in rescue.artx --url healthy-broker:61616

# 4. If you cordoned the old broker and it lives on, lift the block.
artemisctl uncordon --url dying-broker:61616
```

If `redeliver` is interrupted, just run the same command again — it resumes from
the checkpoint and dedup prevents double-delivery.

**Broker won't start at all?** The workflow above needs a live AMQP
connection for step 2 (`export`). If the broker is dead — process won't
start, no connection possible — recover offline from its data directory
instead:

```bash
# 1. Snapshot the dead broker's data dir — salvage is read-only, but the
#    on-disk journal is the only copy until you've salvaged it.
cp -a /var/lib/artemis/data /mnt/rescue/data-snapshot

# 2. Salvage straight off disk — no broker connection needed.
artemisctl salvage --data /mnt/rescue/data-snapshot --out rescue.artx

# 3. Same replay path as above, into a fresh/wiped broker. Never boot the
#    old data dir again once this is done — see the runbook for why.
artemisctl health   --url new-broker:61616
artemisctl redeliver --in rescue.artx --url new-broker:61616
```

See [`docs/offline-recovery.md`](docs/offline-recovery.md) for the full
runbook, including the live-broker guard, the paging/dedup caveats, and a
failure-mode appendix.

## Store format

The store is an **append-only write-ahead log** (WAL), chosen over a
columnar/analytics format precisely because it must survive a crash
mid-evacuation.

```
File header:  magic "ARTX" | version (1 byte)
Record:       totalLen u32 | recUUID (16 bytes) | drainedAt i64 (unix nanos)
              queueLen u16 | queueName bytes
              amqpLen u32  | amqp.Message.MarshalBinary() bytes
              crc32 u32
```

- Records hold the raw AMQP wire encoding
  (`amqp.Message.MarshalBinary()`/`UnmarshalBinary()`), so redelivery is a
  perfect-fidelity replay of body, properties, headers, durability, priority,
  and TTL — not a reconstruction.
- `recUUID` is the deterministic per-record id (`sha256`, first 16 bytes) reused
  as `_AMQ_DUPL_ID` on redelivery. It hashes a **normalized** projection of the
  message — a clone with the volatile `Header.DeliveryCount`/`FirstAcquirer` and
  delivery-annotations cleared — *not* the raw `rec.AMQP` bytes. Normalizing is
  what makes the same broker message re-drained after a crash (its delivery-count
  bumped) collapse to a single delivery; a raw hash of the wire bytes would give
  the two copies different ids and defeat dedup. (Tradeoff: two content-identical
  messages hash equal, so the broker drops one as a duplicate.)
- `crc32` guards each record against truncation/corruption. On a mismatch,
  readers stop at that offset and report it — a truncated WAL is still readable
  up to the last good record.

## Testing

```bash
make test         # gofmt + go vet + full suite, boots real Artemis containers (Testcontainers)
make coverage     # same suite with -race + coverage, writes coverage.html
```

Unit tests cover the `store` package (write/read round-trip, crc detection,
checkpoint seek) and message building for `produce` (generated body sizing,
Artemis-JSON parsing with typed properties). Integration tests boot a real
Artemis broker to exercise drain → redeliver round-trips, resume, dedup, the
health verdict, non-destructive browse, and `produce` → browse round-trips
(sequential and parallel-worker). Benchmarks (`go test -bench . ./internal/broker/`)
measure produce throughput at 1/2/4/8/16 workers, comparing session-level vs
connection-level parallelism.

## Future work

- **Parquet analytics snapshot.** A separate, **non-destructive**
  `export --format parquet` snapshot path for analytics/inspection (query in
  DuckDB/pandas): flattened metadata columns plus a body blob column.
  Deliberately kept out of the crash-safe drain path — Parquet's write-at-close
  footer makes it unreadable if a drain crashes mid-file, which is why the
  emergency store is a custom append-only WAL.
- TLS/SSL connections.
- Credentials from Kubernetes secrets / Vault.
- DLQ / expiry-queue depth in `health`.

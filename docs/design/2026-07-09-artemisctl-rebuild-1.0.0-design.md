# artemisctl 1.0.0 — Design

**Date:** 2026-07-09
**Branch:** `feature/rebuild-1.0.0`
**Status:** Approved

## Purpose

A command-line tool to manage Apache ActiveMQ Artemis brokers, focused on
operational recovery. It checks broker status/health, browses queues and
messages non-destructively, performs an emergency drain (evacuate every
message off a broker into a local store), and later redelivers those messages
back once the broker is healthy again.

## Scope

In scope for 1.0.0:

- `status` — list queues and message counts
- `health` — resource usage + a redelivery-readiness verdict
- `browse` — paged, non-destructive peek with message drill-down
- `export` — destructive drain of every queue into a local binary store
- `redeliver` — replay a store back to a broker

Explicitly out of scope for 1.0.0:

- `produce` (test-data load generator from the reference tool) — not needed for
  an ops/recovery tool.
- `consume` (destructive read to stdout/file) — superseded by `export`/`browse`.
- Multi-broker config file / named brokers — single broker per command.
- Parquet store format — see "Future work".

## Connection model

Single broker per command, matching the reference tool's ergonomics:

- `--url` (default `127.0.0.1:61616`), `-u/--username`, `-p/--password`
- `ARTEMIS_PASSWORD` environment variable preferred over `-p` for secrets
- Connects over AMQP 1.0 via `github.com/Azure/go-amqp`
- Management operations go through the `activemq.management` address using the
  request/reply pattern (dynamic reply-to receiver) proven in the reference.

## Package layout

- `cmd/artemisctl/main.go` — entrypoint, wires cobra root
- `internal/broker` — connection, management RPC, queue enumeration, drain,
  browse, health probes
- `internal/store` — binary store writer/reader and checkpoint sidecar
- `internal/cli` — cobra commands, flag and environment wiring

Rationale: the risky, novel work (drain → store → redeliver with crash-safety,
resume, and dedup) lives behind a clean `store` boundary that can be tested in
isolation from any broker.

## Commands

```
artemisctl status                 # queues + message counts
artemisctl health                 # disk%, mem%, blocking + OK/DEGRADED/CRITICAL verdict
artemisctl browse --queue Q       # paged summary; --limit/--offset; --message <id> drill-down
artemisctl export  --out FILE     # DRAIN every queue (destructive) into a binary store
artemisctl redeliver --in FILE    # replay a store to the broker; health-gated, resumable, dedup
```

## Binary store format (append-only WAL)

The store is an append-only write-ahead log — chosen over a columnar/analytics
format precisely because it must survive a crash mid-evacuation.

```
File header:  magic "ARTX" | version (1 byte)
Record:       totalLen u32 | recUUID (16 bytes) | crc32 u32
              queueLen u16 | queueName bytes
              drainedAt i64 (unix nanos)
              amqpLen u32  | amqp.Message.MarshalBinary() bytes
```

- `amqp.Message.MarshalBinary()` / `UnmarshalBinary()` are public in go-amqp
  v1.5.1, so records hold the raw AMQP wire encoding — perfect-fidelity body,
  properties, headers, durability, priority, and TTL. Redelivery is a faithful
  replay, not a reconstruction.
- `recUUID` is a deterministic per-record id, reused as `_AMQ_DUPL_ID` on
  redelivery so re-running redeliver cannot create duplicates (the broker drops
  repeats).
- `crc32` guards each record against truncation/corruption. On a crc mismatch,
  readers stop at that offset and report it — a truncated WAL is still readable
  up to the last good record.
- Redelivery progress is tracked in a sidecar checkpoint file `FILE.ckpt`
  holding the byte offset of the last successfully redelivered record; resume
  seeks to that offset.

## Drain flow (`export`)

1. Enumerate user queues via the `listQueues` management operation, filtering
   out internal/temporary queues (`activemq.*`, `$`-prefixed, and 36-char
   temporary-queue names — reuse the reference filter).
2. For each queue, open a consuming receiver and read until idle. "Idle" =
   no message received within `--drain-timeout` (default 5s), treated as
   queue-empty.
3. **Durability invariant:** for each batch, write records → `fsync` the store
   → **then** acknowledge (settle/accept) the messages on the broker. Ordering
   guarantees no message is lost if the process crashes mid-drain. The cost is
   at-least-once: a crash between fsync and ack leaves a message on the broker
   that is drained again on the next run, producing a duplicate record — which
   is absorbed by redelivery dedup.

## Redeliver flow (`redeliver`)

1. Probe broker health. Refuse to redeliver if the verdict is `CRITICAL`,
   unless `--force` is passed. Prevents re-flooding a broker that is still
   failing.
2. Open the store and seek to the checkpoint offset (0 if none).
3. For each record: `UnmarshalBinary` the message, set `_AMQ_DUPL_ID` =
   `recUUID`, send to the queue the message was drained from (`--queue`
   overrides all messages to a single queue). On broker ack, advance and
   persist the checkpoint.
4. On SIGINT, flush a clean checkpoint before exit so the next run resumes
   exactly.

## Health verdict (`health`)

Queries management operations for disk-store usage %, address/global memory
usage %, and producer-blocking/paging state. Maps to a single verdict:

- `OK` — all usage < 70%
- `DEGRADED` — any usage 70–90%
- `CRITICAL` — any usage > 90%, or the broker is blocking producers

The process exit code reflects the verdict so the check is scriptable, and
`redeliver` gates on it.

## Browse (`browse`)

Non-destructive paged peek:

- Default: a table of messages (id, size, timestamp, body preview) with
  `--limit` / `--offset` paging.
- `--message <id>`: full body + properties for one message.

**Implementation spike/risk (flagged honestly):** non-destructive peek over
AMQP is less clean than management RPC. Plan: build the summary table from the
Artemis management `browse` operation (returns message metadata without moving
messages); serve full-body drill-down via a browse-mode receiver. Exact
go-amqp browse-mode support needs a short spike during implementation before
this command is finalized.

## Error handling

- Connection/auth failure: fail fast with a clear message and non-zero exit.
- Partial drain: already-acked messages are safely in the store; unacked
  messages remain on the broker for the next run.
- Corrupt store record (crc mismatch): stop reading, report the byte offset,
  redeliver only the good prefix.
- Interrupted redelivery (SIGINT): flush the checkpoint cleanly so resume is
  exact.

## Testing

Integration tests use Testcontainers Artemis (already a dependency pattern in
the reference tool):

- drain → redeliver round-trip preserves message bodies and properties
- resume after a killed redelivery re-sends only remaining messages
- dedup: re-running a completed redelivery produces no duplicates on the broker
- health verdict maps usage thresholds to OK/DEGRADED/CRITICAL correctly
- corrupt/truncated store is read up to the last good record

Unit tests cover the `store` package (write/read round-trip, crc detection,
checkpoint seek) with no broker required.

## Future work

- **Parquet analytics snapshot.** Add `export --format parquet` as a separate,
  **non-destructive** snapshot path for analytics/inspection (query in
  DuckDB/pandas): flattened metadata columns plus a body blob column, footer
  finalized because it is not evacuating a dying broker. Deliberately kept out
  of the crash-safe drain path — Parquet's write-at-close footer makes it
  unreadable if a drain crashes mid-file, which is why the emergency store is a
  custom append-only WAL.
- TLS/SSL connections.
- Credentials from Kubernetes secrets / Vault.
- DLQ / expiry-queue depth in `health`.

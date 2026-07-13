# Offline recovery runbook

This runbook is for the scenario `export` can't handle: the broker is **dead**
and won't start, so there's no AMQP connection to drain. Its messages are only
reachable as files on disk (data directory, possibly on an NFS mount that
survived the crash). `salvage` reads that data directory directly — no JVM, no
running broker — and produces the same `.artx` store that `export` produces,
replayed with the same `redeliver` command.

If the broker **is** still up (even in a degraded state), don't use this
document — use the live-broker path in the main [README](../README.md#recovery-workflow)
(`cordon` + `export`). Salvage is a strictly worse recovery than a live drain:
it can resurrect a handful of already-consumed paged messages (see the caveat
in step 3). Reach for it only when `export` truly isn't an option.

## Decision table

| Broker state | Path |
| --- | --- |
| Alive (even degraded/CRITICAL) — accepts an AMQP connection | `cordon` (optional, freezes producers) then `export` — see the [README recovery workflow](../README.md#recovery-workflow) |
| Dead — process won't start, no AMQP connection possible | This document: `salvage` → `redeliver` |

If you're not sure which one you're in, try to connect first:

```bash
artemisctl status --url dead-broker:61616
```

If that times out or errors and the broker process genuinely won't come back
up, you're in the dead-broker case below.

## Procedure

### 1. Confirm the broker is dead

Check for a live broker process and a held `server.lock` on the data
directory before doing anything else — `salvage` refuses to run against a
data directory whose broker process is still holding that lock (see step 3),
but it's worth confirming manually too so you don't spend time on the wrong
path. If the broker process is running or can be started, stop — go use
`cordon` + `export` instead (see the decision table above).

### 2. Snapshot the data dir

`salvage` opens every file `O_RDONLY` and never writes into the data
directory it reads, but the on-disk journal is the *only* copy of these
messages until you've salvaged them — always work from a copy, never the
broker's live directory:

```bash
cp -a /var/lib/artemis/data /mnt/rescue/data-snapshot-2026-07-12
# or, on NFS: take a filesystem/volume snapshot instead of cp -a
```

A copied/snapshotted data dir also sidesteps the live-broker lock probe
cleanly: the `server.lock` file is copied along with everything else, but
nothing holds an flock on the *copy*, so step 3's guard passes without
needing `--force`.

### 3. Salvage

Run `salvage` against the snapshot, not the original:

```bash
artemisctl salvage --data /mnt/rescue/data-snapshot-2026-07-12 \
  --out rescue-2026-07-12.artx
```

`--data` is the broker data directory; `salvage` derives `bindings/`,
`journal/`, `large-messages/`, and `paging/` sub-directories from it. Pass
those individually instead (`--bindings`, `--journal`, `--large-messages`,
`--paging`) if your layout doesn't match, or if you only have `--bindings`
and `--journal` on hand.

`salvage` is **fully offline** — it never dials the broker and ignores the
global `--url`/`--username`/`--password`/`--timeout` connection flags
entirely.

**Live-broker guard:** before touching anything, `salvage` probes for a held
`server.lock` (checking `<journal>/server.lock`, then `<data>/server.lock`,
then `<data>/../server.lock`, using whichever exists first) and refuses to
run if it's flock-held — that means a broker process is still using this
exact directory, and you should use `export` instead. Pass `--force` only if
you're certain the lock is stale (e.g. unreliable NFS lock semantics) and no
broker process actually holds it.

Read the summary printed at the end:

```
salvaged 512 messages to rescue.artx
  salvage.large      1  (largest 300.1 KiB)
  salvage.paged      500
  salvage.plain      5
  salvage.props      5
  salvage.scheduled  1
```

When `salvage` encounters records it cannot process, a `skipped:` section
lists each skip category (e.g., missing large-message body files, in-doubt
transactions, undecodable bodies, or legacy userType-31 core adds) with a count
of affected records.

When `salvage` encounters damaged data -- a corrupted journal, bindings, or
page-file record (bad check-size, truncated record, broken page framing, an
undecodable bindings-record body) -- a separate `diagnostics:` section lists
each incident, naming the file and byte offset (or, for a damaged
bindings-record body, the binding's record id) and the reason. Not every
`diagnostics:` line is corruption, though: some are informational notes that
never affect the exit code (a missing `large-messages`/`paging` dir the
journal doesn't actually reference, records exported under a synthetic
`unknown-queue-<id>` name, orphaned large-message files, or a benign
fileID-mismatch note from a normally-reused journal file) -- see the
per-category description in the failure-mode appendix below for which is
which.

By default `salvage` exits non-zero when anything was skipped **or** when a
corruption-class diagnostic was reported, specifically so neither can be
missed in a script. If a `skipped:` section or a corruption-class
`diagnostics:` entry is present, resolve or accept it before proceeding (see
the failure-mode appendix below). Pass `--allow-skips` to accept both skips
and corruption diagnostics and exit 0 anyway once you've reviewed them --
the flag name predates the corruption-gating behavior but covers both.

**Caveats to understand before you proceed:**

- **Core-protocol messages** are decoded and exported alongside AMQP messages
  (standard, large, and paged). On `redeliver` they are converted to equivalent
  AMQP messages and sent over AMQP (this tool has no Core wire client); the
  broker re-converts them to Core for Core/JMS consumers. A Core message that
  cannot be converted is skipped on redelivery (left in the store), never
  silently dropped. Legacy pre-persister core adds (userType 31) are still
  reported as skips.
- **At-least-once paging.** Pages are honored as complete-page markers where
  decodable, but within a partially consumed page, `salvage` exports the
  whole page when the cursor position isn't decodable. Net effect: a handful
  of messages a consumer had *already processed* before the crash may be
  resurrected and redelivered. `redeliver`'s `_AMQ_DUPL_ID` dedup only
  catches duplicates *within* what was salvaged (the content hash is salted
  by the destination queue) — it has no way to know a message was consumed
  and acknowledged before the crash, in a delivery this store never saw. If
  your consumers aren't idempotent, be aware a few messages may arrive twice
  after this recovery.
- **Empty result.** If nothing survives, `salvage` prints "salvaged 0
  messages" and does **not** write an output file — don't expect a
  `rescue-*.artx` to exist if the summary reports zero.

### 4. Verify the store

Sanity-check the salvaged counts against what you expect (queue depths from
monitoring, last-known `status` output, etc.) before touching the new broker.
There's no separate "inspect" command for a `.artx` file — the salvage
summary from step 3 is the record of what's in it until you `redeliver` it.

### 5. Stand up a fresh broker (or wipe the old data dir)

Replay always targets a broker other than the one you salvaged from — either
a genuinely fresh instance, or the same instance with its data directory
wiped and reinitialized.

> **Never boot the old journal again after `redeliver`.** Once you've
> replayed the salvaged store, the old data directory and the new broker are
> two divergent copies of the same messages. Booting the old journal after
> this point reintroduces messages `redeliver` already delivered, with no
> dedup between the two — pick **one** source of truth (the new broker) and
> retire the old data directory for good (step 9).

### 6. Health-gate the new broker

```bash
artemisctl health --url new-broker:61616
```

`redeliver` (next step) itself refuses to run against a `CRITICAL` broker
unless you pass `--force`, but checking first avoids finding that out after
you've already committed to the replay.

### 7. Replay

```bash
artemisctl redeliver --in rescue-2026-07-12.artx --url new-broker:61616
```

Add `--queue <name>` to redirect every message to a single destination queue
instead of each message's original queue (e.g. if the target broker uses
different queue names).

`redeliver` is the same command used for a live-broker `export`/`redeliver`
round-trip, so the same resume/dedup/`Ctrl-C` guarantees apply:

- **Resumable:** progress is checkpointed (`<store>.ckpt`); a re-run resumes
  from the last durable point instead of restarting.
- **Deduplicated:** each record's `_AMQ_DUPL_ID` means re-running a
  completed *or* interrupted redelivery of the same store cannot create
  duplicates on the broker — this is also the answer if `redeliver` gets
  interrupted partway through (see the failure-mode appendix).
- **Graceful `Ctrl-C`:** `SIGINT` cancels after the in-flight record; the
  last checkpoint is already durable, so re-running the same command
  resumes cleanly.
- **Scheduled messages** carry their original scheduled-delivery time
  through salvage (as the `x-opt-delivery-time` annotation); the broker
  honors it after redelivery, so a message scheduled for the future stays
  scheduled instead of delivering immediately. A scheduled time already in
  the past delivers right away, which is correct.

### 8. Verify

```bash
artemisctl status --url new-broker:61616
artemisctl browse --queue orders --url new-broker:61616 --limit 20
```

Compare counts against the salvage summary from step 3 (accounting for any
`--queue` redirection).

### 9. Retire the old data dir

Once the replay is verified, rename or archive the old data directory —
don't delete it outright in case you need to audit it later, but make sure
it can never again be started as a broker (per the warning in step 5). This
closes out the recovery: the new broker is now the sole source of truth.

## Failure-mode appendix

| Failure | What to do |
| --- | --- |
| `redeliver` is interrupted (crash, `Ctrl-C`, network drop) | Just run the same `redeliver` command again. It resumes from the checkpoint file, and `_AMQ_DUPL_ID` dedup means any record that was already delivered before the interruption is silently dropped by the broker rather than duplicated. |
| `salvage` reports skips (partial salvage) | Read the `skipped:` section of the summary to see which categories were affected (e.g., Core-protocol messages that this AMQP-only reader cannot decode, missing large-message body files, in-doubt transactions, or undecodable message bodies). Depending on your setup, these may or may not matter: Core-protocol clients are unsupported, in-doubt transactions need manual recovery outside this tool — but a missing large-message body file means an unconsumed message's body is gone, so that skip is a real loss worth investigating (leftover orphan `.msg` files with no journal record, by contrast, are benign lazy-deletion residue and never appear as skips). If records were exported under a synthetic `unknown-queue-<id>` name (visible in the per-queue count lines and as a `diagnostics:` note, not the skip section), re-route them with `redeliver --queue <target>`. Once you've reviewed the skips, either accept them (`--allow-skips`, or just ignore the non-zero exit code in a script that already checked the summary) or resolve the underlying cause and re-run `salvage`. |
| `salvage` reports a corrupt-record diagnostic (damaged journal/bindings/page data) | Read the `diagnostics:` section: a corruption-class entry names the affected file and byte offset (e.g. `message journal: .../activemq-data-1.amq (offset 273): check-size mismatch`) or, for a damaged bindings-record body, the binding's record id (`bindings journal: .../bindings (offset 0): decode queue binding id 3: ...`). This is real, visible data loss — the damaged record itself could not be recovered (its message may be missing entirely, or, for a damaged binding, its messages fall back to a synthetic `unknown-queue-<id>` name) — and by default it exits non-zero exactly like a skip. Not everything in `diagnostics:` is corruption, though: informational notes (missing large-messages/paging dir the journal doesn't reference, orphaned large-message files, benign fileID-mismatch reuse-leftover notes) never gate the exit code — only structural damage does. Once you've reviewed the corruption diagnostics, either accept them (`--allow-skips`, which covers corruption diagnostics too) or investigate the underlying disk/copy damage and re-run `salvage` against a better copy if one exists. |
| The old broker revives mid-procedure (comes back up on its own, or someone restarts it) | **Stop.** You now have two potentially-diverging sources of truth — the broker that just came back, and whatever you've salvaged/replayed so far. Do not continue the procedure blindly. Pick one source of truth: either abandon the salvage output and use the now-live broker with the normal `cordon` + `export` path instead, or shut the revived broker down again and continue treating the salvaged snapshot as authoritative. Don't let both run concurrently against the same queues. |

# faultline

Fault-injection testing for distributed logs, in the style of Jepsen: run a real
cluster, break it on purpose, record every client operation, then check the
history against the guarantees the system documents.

First target: **Apache Iggy 0.10.0**, the first release with VSR (Viewstamped
Replication) clustering built into the standard server.

## How it works

```
cmd/faultline        composition root: flags, wiring, artifacts
internal/history     ordered record of every invoke / ok / fail / info
internal/queue       system-agnostic append-log workload and checker
internal/nemesis     faults: kill, kill-majority, pause, partition, bridge, mix
internal/iggy        Iggy adapters: HTTP client, Rust-SDK idempotent producer pool
drivers/iggy-producer  Rust driver for Iggy's idempotent (exactly-once) producer
targets/iggy         3-node docker compose cluster
```

Each append carries a unique value. Outcomes are classified the way the
system documents them:

- **ok**: acknowledged, with the offset the system reported
- **fail**: the system guarantees no effect (e.g. `TransientNotAccepted`, connection refused)
- **info**: unknown outcome (timeouts, `504 produce_timeout`); may or may not have landed

After the run, faults are healed, the cluster settles, and every node's full
log is read. The checker reports:

| Anomaly | Meaning |
|---|---|
| `lost-write` | acknowledged, but missing from every node's log |
| `ack-offset-mismatch` | the acknowledged offset holds a different value |
| `duplicate` | one append stored at more than one offset |
| `phantom` | a value stored whose append definitely failed, or was never sent |
| `dirty-read` | a reader saw a record the final log contradicts |
| `divergence` | two nodes hold different values at the same offset |

Network faults use a privileged sidecar container that joins the target's
network namespace and edits iptables, so the target image needs no tools and
the host needs no root. Partitions cut replica-to-replica traffic only, so
clients can still reach an isolated node, which is the dangerous case.

## Running

```sh
cd targets/iggy && docker compose up -d && cd ../..
docker build -t faultline/netns tools/netns
# the idempotent-producer driver builds against the Iggy SDK at the tested tag
git clone --depth 1 --branch server-0.10.0 https://github.com/apache/iggy vendor-src/iggy
(cd drivers/iggy-producer && cargo build --release)
go build -o faultline ./cmd/faultline

./faultline -nemesis none                       # control run
./faultline -nemesis partition -duration 120s
./faultline -nemesis mix -healthy 3s -faulty 6s -duration 300s
./faultline -nemesis kill-majority -durability persisted
./faultline -producer idempotent -durability persisted -nemesis mix
```

Artifacts land in `runs/<id>/`: `history.jsonl` (every operation) and
`result.json` (the verdict and anomalies).

## Known limits of this harness

- `docker kill` kills the process but not the kernel page cache, so it does
  not simulate power loss. It cannot tell Iggy's `persisted` (fsync before
  ack) from `replicated`. Testing that claim needs a filesystem that drops
  unsynced writes (e.g. LazyFS).
- Clocks are shared by all containers, so clock-skew faults are not covered.
- One partition per topic: per-partition ordering only.

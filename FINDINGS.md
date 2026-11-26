# Findings

Tested on 2026-11-24 on one laptop (24 cores, Docker 29.6). Every run's full
history and verdict is in `runs/`.

## Summary

| System | Version | Safety (data) | Availability |
|---|---|---|---|
| Apache Iggy | 0.10.0 | No violations found | Recovers ~6s after quorum loss, as designed |
| AutoMQ | 1.7.5 | No violations found | **Writes stall for the whole outage when a leader loses object storage** (F1) |

"No violations found" means these runs found none. It does not prove there are none.

---

## F1. AutoMQ: a broker cut off from object storage keeps leadership, and writes stall without limit

**Severity:** availability / gray failure. No acknowledged data was lost.

**Setup:** AutoMQ 1.7.5 open source, the upstream 3-node compose with MinIO
(`targets/automq`), a single-partition topic, `acks=all`, idempotent producer.

**Fault:** drop packets from MinIO to the partition leader only. The leader
still reaches the controllers, its peers and clients. The other two brokers
and MinIO stay healthy.

**Observed:**
- The leader keeps leadership for the whole outage. Checked every 20s for 180s:
  `Leader: 2` every time.
- Writes to that partition do not complete. A write sent at the moment of the
  cut was acknowledged **3m59s** later, only after storage was restored.
- On the broker, produce callbacks hit the 30s timeout and clients retry without limit.
- The broker clearly sees the problem. Every 10s it logs
  `ERROR Found N pending upload tasks exceed 5s (DefaultWriter)`, plus
  `PutObject ... fail, retry`. It takes no action.
- When storage returns, every pending write is committed exactly once. The checker
  found no lost, duplicated or reordered records.

**Why (from the 1.7.5 source):**
- `DefaultWriter.startMonitor()` only logs the stuck-upload condition.
- `AbstractObjectStorage` retries retryable PutObject errors with no limit
  (exponential backoff, capped near 60s). So the WAL append never fails, the WAL is
  never fenced, and the storage-failure handlers (force-close, then halt) never run.
- Controller-side failover (`NodeRuntimeMetadata.shouldFailover()`) needs the
  broker to be KRaft-fenced, i.e. to have stopped heartbeating. A broker that can
  reach the controller keeps heartbeating, so failover never starts.
- No configuration changes this in the open-source edition. Standby-bucket failover
  is documented as a commercial feature.

**Why it matters:** AutoMQ's value is that brokers are stateless and data lives on
shared storage. That is exactly what should make failover cheap. A healthy broker
could take over this partition by replaying the WAL from S3, which every other
node can still reach. A per-broker storage path failure (a bad NAT gateway,
a security-group or route change, a stuck S3 connection pool) turns into
unlimited write unavailability for every partition that broker leads.
Published RTO figures ("under 30 seconds") describe node loss, not this case.

**Related:**
- AutoMQ#3724 (closed) is the opposite case: a *partial* S3 recovery makes
  the broker exit. The maintainers state the intended behavior for
  retryable storage errors is to "keep retrying until the operation succeeds".
  That confirms the stall is by design today, but it leaves no path to fail over.
- No existing issue reports the leadership-retention stall (searched 2026-11-24).

**Reproduce:** `scripts/repro-automq-s3-stall.sh` (set `CUT_SECONDS` to change the outage length).

**Possible fixes to propose upstream:** treat "WAL uploads stuck longer than X" as
a storage failure and hand it to the existing failure handler, or report WAL health
in the broker heartbeat so the controller can fence the broker and run the
existing WAL failover.

---

## Runs with no anomalies

| Target | Fault | Ops attempted | Notes |
|---|---|---|---|
| Iggy | none (control) | 4,591 | |
| Iggy | partition (isolate 1 of 3 from peers) | 11,554 | isolated node correctly refuses with 503 |
| Iggy | kill -9 one node | 16,708 | |
| Iggy | pause one node | 9,317 | |
| Iggy | kill -9 majority, `persisted` | 6,352 | ~6s to reopen writes after restart |
| Iggy | bridge (non-transitive partition) | 13,407 | |
| Iggy | mix, 5 min, fast flapping | 42,266 | |
| Iggy | idempotent producer, mix, forced resends from new sessions | 20,711 | 22 acks needed a resend; 0 duplicates |
| Iggy | idempotent producer, majority kills, 100ms resend timeout | 500 | |
| AutoMQ | none (control) | 310 | ~250ms acks (S3 WAL batching) |
| AutoMQ | kill -9 one broker | 6,278 | |
| AutoMQ | cut one broker from MinIO | 6,859 | stalls; see F1 |
| AutoMQ | partition one broker from peers and controllers | 11,670 | |

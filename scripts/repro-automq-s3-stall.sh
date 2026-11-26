#!/usr/bin/env bash
# Reproduces: an AutoMQ 1.7.5 broker that loses network access to object
# storage keeps partition leadership, and writes to its partitions stall for
# the whole outage even though the other brokers and the object store are healthy.
#
# Requires: targets/automq cluster running, faultline/netns image, ./probe built.
set -euo pipefail

ROOT=$(cd "$(dirname "$0")/.." && pwd)

CUT_SECONDS=${CUT_SECONDS:-180}
TOPIC=${TOPIC:-s3-stall-repro-$(date +%s)}
MINIO_IP=10.6.0.5
KAFKA_BIN=/opt/automq/kafka/bin

kafka() { docker exec automq-server1 "$KAFKA_BIN/$1" --bootstrap-server server1:9092 "${@:2}"; }
leader() { kafka kafka-topics.sh --describe --topic "$TOPIC" | grep -o 'Leader: [0-9]*' | awk '{print $2}'; }
netns() { docker run --rm --net "container:$1" --cap-add NET_ADMIN faultline/netns "${@:2}"; }

kafka kafka-topics.sh --create --topic "$TOPIC" --partitions 1 --replication-factor 1 >/dev/null
sleep 3
LEADER_ID=$(leader)
LEADER="automq-server$((LEADER_ID + 1))"
echo "topic $TOPIC, partition 0 led by node $LEADER_ID ($LEADER)"

"$ROOT/probe" -topic "$TOPIC" -duration $((CUT_SECONDS + 60))s -interval 500ms > "$ROOT/runs/probe-$TOPIC.log" 2>&1 &
PROBE=$!
sleep 15

echo "$(date +%T) cutting $LEADER off from object storage ($MINIO_IP) for ${CUT_SECONDS}s"
netns "$LEADER" iptables -A INPUT -s "$MINIO_IP" -j DROP
trap 'netns "$LEADER" iptables -D INPUT -s "$MINIO_IP" -j DROP 2>/dev/null || true' EXIT

for ((t = 30; t <= CUT_SECONDS; t += 30)); do
	sleep 30
	echo "$(date +%T) +${t}s leader is node $(leader)"
done

netns "$LEADER" iptables -D INPUT -s "$MINIO_IP" -j DROP
trap - EXIT
echo "$(date +%T) storage restored"
wait "$PROBE" || true

echo
echo "writes that took 10s or longer to be acknowledged:"
grep -E 'latency=([0-9]+m[0-9]|[1-9][0-9]+(\.[0-9]+)?s)' "$ROOT/runs/probe-$TOPIC.log" || echo "  none"
echo "broker-side errors during the cut:"
docker exec "$LEADER" sh -c "cat /opt/automq/kafka/logs/s3-object.log* /opt/automq/kafka/logs/server.log* 2>/dev/null | grep -c 'pending upload tasks exceed 5s'" || true

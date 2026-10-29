# sr-raw-backup

Backs up Confluent Schema Registry by exporting its **storage topic** (`kafkastore.topic`,
usually `_schemas`) as **raw bytes** to one JSON Lines file.

Unlike a backup over the registry's REST API, this captures everything the registry wrote: hard-deleted
schemas (until compaction removes them), registration timestamps, delete markers, config and mode
records, contexts — byte for byte.

## Build

JDK 21. Tests use Testcontainers (real Kafka + Schema Registry) and are skipped without Docker.

```bash
./gradlew build                      # -> build/libs/sr-raw-backup.jar (self-contained)
```

## Export

```bash
java -jar build/libs/sr-raw-backup.jar export \
  --bootstrap-server broker1:9092 \
  --topic _schemas \
  --file schemas-$(date +%Y%m%dT%H%M%S).jsonl \
  [--command-config client.properties] [--timeout-seconds 60]
```

`--command-config` takes ordinary Kafka client properties, e.g. for SASL over TLS:

```properties
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="backup" password="...";
ssl.truststore.location=/etc/kafka/truststore.jks
ssl.truststore.password=...
```

**ACLs needed** on the topic: `Read`, `Describe`, `DescribeConfigs`. No consumer group is used
(the tool assigns the partition and never commits), so no group ACL.

Exit codes: `0` ok, `1` export failed (no file is left behind), `2` bad usage.
Logs go to stderr.

## What you get

```
{"format":"sr-raw-backup/1","topic":"_schemas","clusterId":"5L6g…","partition":0,"beginOffset":0,"endOffset":24,"createdAt":"…","topicConfig":{"cleanup.policy":"compact"}}
{"offset":0,"ts":1793247770000,"key":"eyJrZXl0eXBlIjoiTk9PUCIsIm1hZ2ljIjowfQ==","value":null,"headers":[]}
{"offset":8,"ts":1793247777000,"key":"eyJrZXl0eXBlIjoiU0NIRU1BIi…","value":"eyJzdWJqZWN0IjoidG1wLXZhbHVlIi…","headers":[]}
…
{"records":24,"tombstones":4,"keyTypes":{"CONFIG":2,"DELETE_SUBJECT":1,"MODE":1,"NOOP":2,"SCHEMA":18},"sha256":"891b8e…"}
```

| Line | Content |
|---|---|
| first | **Header**: topic, Kafka cluster id, the offset range captured, configs set explicitly on the topic (to recreate it identically) |
| middle | One **record** per Kafka record: offset, timestamp, key / value / header values as base64 of the exact bytes. `null` stays `null` — a tombstone, which the registry relies on when it replays the topic |
| last | **Trailer**: record count, tombstones, count per registry key type, SHA-256 of every byte before this line |

### Guarantees

- **Point-in-time.** The end offset is pinned *before* reading; records written while the export
  runs are left out. The file is the topic as it was at one instant, never half of a change.
- **Byte-exact.** Bytes are base64'd, never decoded and re-encoded.
- **Complete or absent.** Written to `<file>.tmp`, fsynced, then atomically renamed. A failed
  export leaves nothing under the real name.
- **Checkable later**, e.g. after copying to object storage:
  ```bash
  sed '$d' schemas.jsonl | shasum -a 256      # must equal the trailer's sha256
  tail -1 schemas.jsonl | jq -r .sha256
  ```
- **Sanity check.** If a non-empty topic contains no Schema Registry key types, it warns: you
  probably passed the wrong topic. A topic with more than one partition is refused outright —
  the registry's storage topic always has exactly one.

### Reading it

```bash
# every record, decoded (SR keys and values are JSON)
sed '1d;$d' schemas.jsonl | jq -c '{offset,
  key:   (.key|@base64d|fromjson),
  value: (if .value then (.value|@base64d|fromjson) else null end)}'
```

## Notes for the restore side (not built yet)

- Produce the records, **in order, tombstones included**, into a new empty topic created with
  the header's `topicConfig` (and 1 partition) — **before** the target registry starts, or with it
  stopped. It rebuilds its state, its ID counter and timestamps included, by replaying the topic.
- Restore into the **same or a newer** registry version: the topic format is internal to the registry.
- Compaction removes a hard-deleted schema's records (after `delete.retention.ms`, 24h by default).
  A backup taken after that no longer has them — run backups more often than that if old message
  IDs must stay resolvable.

## Try it against the sr-backup-poc compose stack

The broker there advertises `kafka:9092`, so run the jar on the compose network:

```bash
docker run --rm --network sr-backup-poc_default \
  -v "$PWD/build/libs:/app:ro" -v "$PWD:/out" eclipse-temurin:21-jre \
  java -jar /app/sr-raw-backup.jar export --bootstrap-server kafka:9092 --topic _schemas --file /out/schemas.jsonl
```

package com.example.kafka.srrawbackup;

import java.util.List;
import java.util.Map;

/**
 * The backup file is JSON Lines:
 *
 * <pre>
 * line 1      Header   topic, cluster, the offset range captured, topic configs
 * line 2..n   Record   one per Kafka record, key/value/header bytes as base64, null kept as null
 * last line   Trailer  record count, counts per key type, SHA-256 of every byte before this line
 * </pre>
 *
 * Bytes are never decoded or re-encoded: base64 of exactly what is in the topic.
 */
public final class BackupFormat {

    public static final String FORMAT = "sr-raw-backup/1";

    private BackupFormat() {
    }

    /**
     * @param beginOffset first offset still in the topic when the backup started
     * @param endOffset   the topic's end offset when the backup started; records at or after it are NOT included
     * @param topicConfig configs set explicitly on the topic (e.g. cleanup.policy=compact), to recreate it
     */
    public record Header(String format, String topic, String clusterId, int partition,
                         long beginOffset, long endOffset, String createdAt,
                         Map<String, String> topicConfig) {
    }

    /** A null value is a tombstone and must stay null: Schema Registry relies on it when replaying. */
    public record Record(long offset, long ts, String key, String value, List<KafkaHeader> headers) {
    }

    public record KafkaHeader(String key, String value) {
    }

    /** {@code sed '$d' backup.jsonl | shasum -a 256} reproduces {@code sha256}. */
    public record Trailer(long records, long tombstones, Map<String, Long> keyTypes, String sha256) {
    }
}

package com.example.kafka.srrawbackup;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Reads a topic as a consistent snapshot:
 *   1. {@link #snapshot} pins the end offset FIRST
 *   2. {@link #read} delivers every record from the beginning up to (not including) that offset
 * Anything written while the backup runs is left out, so the file is the topic exactly as it
 * was at one instant - never half of a change.
 */
public final class TopicExporter implements AutoCloseable {

    private final Admin admin;
    private final KafkaConsumer<byte[], byte[]> consumer;
    private final Duration timeout;

    public TopicExporter(Properties common, Duration timeout) {
        this.timeout = timeout;
        this.admin = Admin.create(common);

        Properties c = new Properties();
        c.putAll(common);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // no group id: we assign + seek, never commit
        c.putIfAbsent(ConsumerConfig.CLIENT_ID_CONFIG, "sr-raw-backup");
        this.consumer = new KafkaConsumer<>(c);
    }

    public BackupFormat.Header snapshot(String topic) throws Exception {
        TopicDescription desc;
        try {
            desc = get(admin.describeTopics(List.of(topic)).allTopicNames()).get(topic);
        } catch (ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                throw new IllegalArgumentException("Topic '" + topic + "' does not exist");
            }
            throw e;
        }
        if (desc.partitions().size() != 1) {
            throw new IllegalArgumentException("Topic '" + topic + "' has " + desc.partitions().size()
                    + " partitions. Schema Registry's storage topic always has exactly 1 - wrong topic?");
        }
        TopicPartition tp = new TopicPartition(topic, 0);

        // pin the range BEFORE reading anything
        long end = get(admin.listOffsets(Map.of(tp, OffsetSpec.latest())).partitionResult(tp)).offset();
        long begin = get(admin.listOffsets(Map.of(tp, OffsetSpec.earliest())).partitionResult(tp)).offset();

        String clusterId = get(admin.describeCluster().clusterId());

        ConfigResource res = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        Map<String, String> topicConfig = new TreeMap<>();
        for (ConfigEntry e : get(admin.describeConfigs(List.of(res)).all()).get(res).entries()) {
            if (e.source() == ConfigEntry.ConfigSource.DYNAMIC_TOPIC_CONFIG) {
                topicConfig.put(e.name(), e.value());
            }
        }

        return new BackupFormat.Header(BackupFormat.FORMAT, topic, clusterId, 0,
                begin, end, Instant.now().toString(), topicConfig);
    }

    /** Delivers records in offset order, from the snapshot's begin offset up to (excluding) its end offset. */
    public void read(BackupFormat.Header snapshot, Consumer<ConsumerRecord<byte[], byte[]>> sink) {
        TopicPartition tp = new TopicPartition(snapshot.topic(), snapshot.partition());
        long end = snapshot.endOffset();
        consumer.assign(List.of(tp));
        consumer.seek(tp, snapshot.beginOffset());

        long position = consumer.position(tp, timeout);
        long lastProgress = System.nanoTime();
        while (position < end) {
            ConsumerRecords<byte[], byte[]> batch = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<byte[], byte[]> r : batch.records(tp)) {
                if (r.offset() >= end) {
                    break; // written after the snapshot started
                }
                sink.accept(r);
            }
            long now = consumer.position(tp, timeout);
            if (now > position) {
                position = now;
                lastProgress = System.nanoTime();
            } else if (System.nanoTime() - lastProgress > timeout.toNanos()) {
                throw new IllegalStateException("No progress reading " + tp + " for " + timeout.toSeconds()
                        + "s: stuck at offset " + position + ", need to reach " + end);
            }
        }
    }

    private <T> T get(org.apache.kafka.common.KafkaFuture<T> f) throws Exception {
        return f.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void close() {
        consumer.close(Duration.ofSeconds(5));
        admin.close(Duration.ofSeconds(5));
    }
}

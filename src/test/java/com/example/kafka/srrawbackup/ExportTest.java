package com.example.kafka.srrawbackup;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/** Needs Docker; skipped without it. */
@Testcontainers(disabledWithoutDocker = true)
class ExportTest {

    static final Network NET = Network.newNetwork();

    @Container
    static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.1.1")
            .withNetwork(NET)
            .withListener("kafka:19092");

    @Container
    static final GenericContainer<?> SR = new GenericContainer<>("confluentinc/cp-schema-registry:8.1.0")
            .withNetwork(NET)
            .dependsOn(KAFKA)
            .withExposedPorts(8081)
            .withEnv("SCHEMA_REGISTRY_HOST_NAME", "sr")
            .withEnv("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:19092")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC", "_schemas")
            .withEnv("SCHEMA_REGISTRY_KAFKASTORE_TOPIC_REPLICATION_FACTOR", "1")
            .waitingFor(Wait.forHttp("/subjects").forStatusCode(200));

    static final ObjectMapper MAPPER = new ObjectMapper();
    static final HttpClient HTTP = HttpClient.newHttpClient();

    @TempDir
    Path dir;

    @Test
    void exportsTheRegistryTopicByteForByte() throws Exception {
        seedRegistry();
        Path file = dir.resolve("schemas.jsonl");

        assertThat(run("export", "--bootstrap-server", KAFKA.getBootstrapServers(),
                "--topic", "_schemas", "--file", file.toString()).exitCode).isZero();

        List<String> lines = Files.readAllLines(file);
        var header = MAPPER.readValue(lines.getFirst(), BackupFormat.Header.class);
        var trailer = MAPPER.readValue(lines.getLast(), BackupFormat.Trailer.class);
        List<BackupFormat.Record> records = new ArrayList<>();
        for (String l : lines.subList(1, lines.size() - 1)) {
            records.add(MAPPER.readValue(l, BackupFormat.Record.class));
        }

        assertThat(header.format()).isEqualTo(BackupFormat.FORMAT);
        assertThat(header.topicConfig()).containsEntry("cleanup.policy", "compact");

        // an independent read of the same offset range must give exactly the same bytes
        assertThat(records).isEqualTo(readTopic("_schemas", header.endOffset()));

        assertThat(trailer.records()).isEqualTo(records.size());
        assertThat(trailer.tombstones()).as("hard delete leaves tombstones").isPositive();
        assertThat(trailer.keyTypes()).containsKeys("SCHEMA", "CONFIG", "DELETE_SUBJECT");
        assertThat(trailer.sha256()).isEqualTo(sha256OfAllButLastLine(file));
    }

    @Test
    void recordsWrittenDuringTheExportAreLeftOut() throws Exception {
        String topic = "snapshot-test";
        createTopic(topic, 1);
        try (var producer = producer()) {
            for (int i = 0; i < 5; i++) {
                var rec = new ProducerRecord<>(topic, ("k" + i).getBytes(), i == 3 ? null : ("v" + i).getBytes());
                rec.headers().add(new RecordHeader("h", i == 1 ? null : new byte[]{0, 1, 2}));
                producer.send(rec);
            }
            producer.flush();

            Properties common = new Properties();
            common.put("bootstrap.servers", KAFKA.getBootstrapServers());
            Path file = dir.resolve("snap.jsonl");
            try (var exporter = new TopicExporter(common, Duration.ofSeconds(30));
                 var out = new BackupWriter(file)) {
                var snap = exporter.snapshot(topic);
                assertThat(snap.endOffset()).isEqualTo(5);

                List<Long> seen = new ArrayList<>();
                exporter.read(snap, r -> {
                    if (seen.isEmpty()) { // a writer racing the backup
                        for (int i = 0; i < 3; i++) {
                            producer.send(new ProducerRecord<>(topic, "late".getBytes(), "late".getBytes()));
                        }
                        producer.flush();
                    }
                    seen.add(r.offset());
                    out.record(r);
                });
                var trailer = out.finish();

                assertThat(seen).containsExactly(0L, 1L, 2L, 3L, 4L);
                assertThat(trailer.tombstones()).isEqualTo(1);
                assertThat(trailer.keyTypes()).containsOnlyKeys("NOT_JSON");
            }
            assertThat(readTopic(topic, 8)).hasSize(8); // the late records did land in the topic
        }
    }

    @Test
    void missingTopicFailsAndLeavesNoFile() {
        Path file = dir.resolve("missing.jsonl");
        Result r = run("export", "--bootstrap-server", KAFKA.getBootstrapServers(),
                "--topic", "no-such-topic", "--file", file.toString(), "--timeout-seconds", "15");

        assertThat(r.exitCode).isEqualTo(1);
        assertThat(r.err).contains("does not exist");
        assertThat(dir).isEmptyDirectory();
    }

    @Test
    void multiPartitionTopicIsRejected() throws Exception {
        createTopic("two-partitions", 2);
        Result r = run("export", "--bootstrap-server", KAFKA.getBootstrapServers(),
                "--topic", "two-partitions", "--file", dir.resolve("x.jsonl").toString());

        assertThat(r.exitCode).isEqualTo(1);
        assertThat(r.err).contains("has 2 partitions");
        assertThat(dir).isEmptyDirectory();
    }

    @Test
    void missingRequiredOptionIsAUsageError() {
        assertThat(run("export", "--topic", "_schemas").exitCode).isEqualTo(2);
    }

    // ---------------------------------------------------------------- helpers

    record Result(int exitCode, String err) {
    }

    private static Result run(String... args) {
        StringWriter err = new StringWriter();
        var cmd = SrRawBackup.commandLine();
        cmd.setErr(new PrintWriter(err));
        int rc = cmd.execute(args);
        return new Result(rc, err.toString());
    }

    /** Evolution, a soft delete, a hard delete (tombstones) and a config change. */
    private static void seedRegistry() throws Exception {
        String v1 = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"}]}";
        String v2 = "{\"type\":\"record\",\"name\":\"Order\",\"fields\":[{\"name\":\"id\",\"type\":\"string\"},"
                + "{\"name\":\"amount\",\"type\":\"double\",\"default\":0}]}";
        String tmp = "{\"type\":\"record\",\"name\":\"Tmp\",\"fields\":[{\"name\":\"x\",\"type\":\"int\"}]}";

        sr("POST", "/subjects/orders-value/versions", MAPPER.writeValueAsString(Map.of("schema", v1)));
        sr("POST", "/subjects/orders-value/versions", MAPPER.writeValueAsString(Map.of("schema", v2)));
        sr("DELETE", "/subjects/orders-value/versions/1", null);
        sr("POST", "/subjects/tmp-value/versions", MAPPER.writeValueAsString(Map.of("schema", tmp)));
        sr("DELETE", "/subjects/tmp-value", null);
        sr("DELETE", "/subjects/tmp-value?permanent=true", null);
        sr("PUT", "/config/orders-value", "{\"compatibility\":\"FULL\"}");
    }

    private static void sr(String method, String path, String body) throws Exception {
        var req = HttpRequest.newBuilder(URI.create("http://" + SR.getHost() + ":" + SR.getMappedPort(8081) + path))
                .header("Content-Type", "application/vnd.schemaregistry.v1+json")
                .method(method, body == null ? HttpRequest.BodyPublishers.noBody()
                        : HttpRequest.BodyPublishers.ofString(body))
                .build();
        var res = HTTP.send(req, HttpResponse.BodyHandlers.ofString());
        assertThat(res.statusCode()).as(method + " " + path + " -> " + res.body()).isLessThan(300);
    }

    private static void createTopic(String name, int partitions) throws Exception {
        try (Admin admin = Admin.create(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()))) {
            admin.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get();
        }
    }

    private static KafkaProducer<byte[], byte[]> producer() {
        return new KafkaProducer<>(Map.of("bootstrap.servers", KAFKA.getBootstrapServers()),
                new ByteArraySerializer(), new ByteArraySerializer());
    }

    /** Plain consumer read of offsets [0, end), in the backup's record shape. */
    private static List<BackupFormat.Record> readTopic(String topic, long end) {
        var b64 = Base64.getEncoder();
        List<BackupFormat.Record> out = new ArrayList<>();
        try (var c = new KafkaConsumer<>(Map.<String, Object>of("bootstrap.servers", KAFKA.getBootstrapServers()),
                new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            var tp = new TopicPartition(topic, 0);
            c.assign(List.of(tp));
            c.seekToBeginning(List.of(tp));
            long deadline = System.currentTimeMillis() + 30_000;
            while (c.position(tp) < end && System.currentTimeMillis() < deadline) {
                for (ConsumerRecord<byte[], byte[]> r : c.poll(Duration.ofSeconds(1))) {
                    if (r.offset() >= end) break;
                    List<BackupFormat.KafkaHeader> hs = new ArrayList<>();
                    r.headers().forEach(h -> hs.add(new BackupFormat.KafkaHeader(h.key(),
                            h.value() == null ? null : b64.encodeToString(h.value()))));
                    out.add(new BackupFormat.Record(r.offset(), r.timestamp(),
                            r.key() == null ? null : b64.encodeToString(r.key()),
                            r.value() == null ? null : b64.encodeToString(r.value()), hs));
                }
            }
        }
        return out;
    }

    private static String sha256OfAllButLastLine(Path file) throws Exception {
        byte[] all = Files.readAllBytes(file);
        int lastLineStart = new String(all, StandardCharsets.ISO_8859_1).lastIndexOf('\n', all.length - 2) + 1;
        MessageDigest md = MessageDigest.getInstance("SHA-256");
        md.update(all, 0, lastLineStart);
        return HexFormat.of().formatHex(md.digest());
    }
}

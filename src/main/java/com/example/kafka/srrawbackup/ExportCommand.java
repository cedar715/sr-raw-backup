package com.example.kafka.srrawbackup;

import org.apache.kafka.clients.CommonClientConfigs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Callable;

@Command(name = "export",
        mixinStandardHelpOptions = true,
        description = "Export every record of the topic, as raw bytes, to a JSON Lines file.")
final class ExportCommand implements Callable<Integer> {

    private static final Logger log = LoggerFactory.getLogger(ExportCommand.class);

    /** Key types Schema Registry writes; none of them in a non-empty topic means it isn't a registry topic. */
    private static final Set<String> SR_KEY_TYPES =
            Set.of("SCHEMA", "CONFIG", "MODE", "NOOP", "DELETE_SUBJECT", "CLEAR_SUBJECT", "CONTEXT");

    @Option(names = "--bootstrap-server", required = true, paramLabel = "<host:port>",
            description = "Kafka bootstrap servers.")
    String bootstrapServer;

    @Option(names = "--topic", required = true,
            description = "Schema Registry's storage topic (its kafkastore.topic), usually _schemas.")
    String topic;

    @Option(names = "--file", required = true, description = "Backup file to write (JSON Lines).")
    Path file;

    @Option(names = "--command-config", paramLabel = "<client.properties>",
            description = "Kafka client properties: security.protocol, sasl.*, ssl.* ...")
    Path commandConfig;

    @Option(names = "--timeout-seconds", defaultValue = "60",
            description = "Give up when Kafka makes no progress for this long (default: ${DEFAULT-VALUE}).")
    int timeoutSeconds;

    @Override
    public Integer call() throws Exception {
        Properties common = new Properties();
        if (commandConfig != null) {
            try (InputStream in = Files.newInputStream(commandConfig)) {
                common.load(in);
            }
        }
        common.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);

        try (TopicExporter exporter = new TopicExporter(common, Duration.ofSeconds(timeoutSeconds));
             BackupWriter out = new BackupWriter(file)) {

            BackupFormat.Header snap = exporter.snapshot(topic);
            log.info("Exporting {} on cluster {}: offsets {}..{} (end pinned; later writes are not included)",
                    topic, snap.clusterId(), snap.beginOffset(), snap.endOffset());

            out.header(snap);
            exporter.read(snap, out::record);
            BackupFormat.Trailer t = out.finish();

            log.info("Wrote {} records ({} tombstones) to {} ({} bytes)",
                    t.records(), t.tombstones(), file.toAbsolutePath(), Files.size(file));
            log.info("Key types: {}", t.keyTypes());
            log.info("sha256 {}", t.sha256());
            if (t.records() > 0 && t.keyTypes().keySet().stream().noneMatch(SR_KEY_TYPES::contains)) {
                log.warn("No Schema Registry key types found - is '{}' really the registry's storage topic?", topic);
            }
        }
        return 0;
    }
}

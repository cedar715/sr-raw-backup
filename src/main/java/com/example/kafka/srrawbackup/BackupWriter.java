package com.example.kafka.srrawbackup;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.security.DigestOutputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Writes the backup file (see {@link BackupFormat}) to {@code <file>.tmp}, fsyncs it and renames
 * it into place: a crash or a failed export never leaves a half-written file under the real name.
 */
public final class BackupWriter implements AutoCloseable {

    private static final Base64.Encoder B64 = Base64.getEncoder();

    private final ObjectMapper mapper = new ObjectMapper();
    private final Path file;
    private final Path tmp;
    private final FileOutputStream fos;
    private final MessageDigest sha256;
    private final OutputStream out;

    private long records;
    private long tombstones;
    private final Map<String, Long> keyTypes = new TreeMap<>();
    private boolean finished;

    public BackupWriter(Path file) throws IOException {
        this.file = file;
        this.tmp = file.resolveSibling(file.getFileName() + ".tmp");
        try {
            this.sha256 = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
        this.fos = new FileOutputStream(tmp.toFile());
        this.out = new BufferedOutputStream(new DigestOutputStream(fos, sha256), 1 << 16);
    }

    public void header(BackupFormat.Header header) throws IOException {
        line(header);
    }

    public void record(ConsumerRecord<byte[], byte[]> r) {
        List<BackupFormat.KafkaHeader> headers = new ArrayList<>();
        for (Header h : r.headers()) {
            headers.add(new BackupFormat.KafkaHeader(h.key(), b64(h.value())));
        }
        try {
            line(new BackupFormat.Record(r.offset(), r.timestamp(), b64(r.key()), b64(r.value()), headers));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        records++;
        if (r.value() == null) {
            tombstones++;
        }
        keyTypes.merge(keyType(r.key()), 1L, Long::sum);
    }

    /** Writes the trailer, fsyncs and moves the file into place. */
    public BackupFormat.Trailer finish() throws IOException {
        out.flush(); // everything so far is now in the digest
        BackupFormat.Trailer trailer = new BackupFormat.Trailer(
                records, tombstones, keyTypes, HexFormat.of().formatHex(sha256.digest()));
        line(trailer);
        out.flush();
        fos.getFD().sync();
        out.close();
        Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        finished = true;
        return trailer;
    }

    @Override
    public void close() throws IOException {
        if (!finished) {
            out.close();
            Files.deleteIfExists(tmp);
        }
    }

    private void line(Object value) throws IOException {
        out.write(mapper.writeValueAsBytes(value));
        out.write('\n');
    }

    /**
     * Schema Registry keys are JSON like {"keytype":"SCHEMA","subject":...}. Counting them is only a
     * sanity check that the topic really is a registry's - the bytes are stored untouched either way.
     */
    private String keyType(byte[] key) {
        if (key == null) {
            return "NULL_KEY";
        }
        try {
            JsonNode k = mapper.readTree(key);
            return k != null && k.hasNonNull("keytype") ? k.get("keytype").asText() : "UNKNOWN";
        } catch (IOException e) {
            return "NOT_JSON";
        }
    }

    private static String b64(byte[] bytes) {
        return bytes == null ? null : B64.encodeToString(bytes);
    }
}

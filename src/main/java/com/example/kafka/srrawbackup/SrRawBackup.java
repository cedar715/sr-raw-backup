package com.example.kafka.srrawbackup;

import picocli.CommandLine;
import picocli.CommandLine.Command;

/**
 * Raw backup of Schema Registry's storage topic (usually {@code _schemas}).
 *
 * <pre>
 * java -jar sr-raw-backup.jar export --bootstrap-server kafka:9092 --topic _schemas --file schemas.jsonl
 * </pre>
 *
 * Exit codes: 0 ok, 1 export failed, 2 bad usage.
 */
@Command(name = "sr-raw-backup",
        mixinStandardHelpOptions = true,
        version = "sr-raw-backup 0.1.0",
        description = "Raw backup of Schema Registry's storage topic.",
        subcommands = ExportCommand.class)
public class SrRawBackup {

    public static void main(String[] args) {
        System.exit(commandLine().execute(args));
    }

    static CommandLine commandLine() {
        return new CommandLine(new SrRawBackup())
                // one readable line instead of a stack trace; the cause chain is what matters
                .setExecutionExceptionHandler((ex, cmd, parseResult) -> {
                    cmd.getErr().println("error: " + describe(ex));
                    return 1;
                });
    }

    private static String describe(Throwable ex) {
        StringBuilder sb = new StringBuilder();
        for (Throwable t = ex; t != null; t = t.getCause()) {
            String msg = t.getMessage();
            if (msg != null && !sb.toString().contains(msg)) {
                if (!sb.isEmpty()) sb.append(" <- ");
                sb.append(msg);
            }
        }
        return sb.isEmpty() ? ex.getClass().getName() : sb.toString();
    }
}

package ch.so.agi.cli;

import ch.so.agi.cloudformats.GeoPackageGeometryReader;
import ch.so.agi.cloudformats.GeoPackageTableDescriptorProvider;
import ch.so.agi.cloudformats.TableDescriptorProvider;
import ch.so.agi.flatgeobuf.FlatGeobufExporter;
import ch.so.agi.parquet.ParquetExporter;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public final class Gpkg2CloudFormatCli {
    private Gpkg2CloudFormatCli() {
    }

    public static void main(String[] args) {
        int exitCode = run(args, System.out, System.err);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    static int run(String[] args, PrintStream out, PrintStream err) {
        try {
            return new Runner(out).run(args);
        } catch (IllegalArgumentException e) {
            err.println(e.getMessage());
            err.println();
            err.println(usage());
            return 2;
        } catch (Exception e) {
            err.println("Error: " + e.getMessage());
            return 1;
        }
    }

    private static String usage() {
        return """
                Usage: java -jar gpkg2cloudformat.jar --input <gpkg> --output <dir> [--tables \"<table1>\";\"<table2>\"] --format <flatgeobuf|parquet>

                Options:
                  --input    Geopackage-Datei
                  --output   Verzeichnis fuer exportierte Dateien (muss existieren)
                  --tables   Optionale, mit Semikolon getrennte Liste von Tabellennamen in doppelten Anfuehrungszeichen
                  --format   flatgeobuf oder parquet
                """.trim();
    }

    private static final class Runner {
        private final PrintStream out;

        private Runner(PrintStream out) {
            this.out = out;
        }

        private int run(String[] args) throws Exception {
            Map<String, String> options = parseOptions(args);
            Path input = requirePath(options, "--input");
            Path outputDir = requirePath(options, "--output");
            String formatValue = requireValue(options, "--format");

            if (!Files.exists(input) || !Files.isRegularFile(input)) {
                throw new IllegalArgumentException("Input file does not exist: " + input);
            }
            if (!Files.exists(outputDir) || !Files.isDirectory(outputDir)) {
                throw new IllegalArgumentException("Output directory does not exist: " + outputDir);
            }

            List<String> tables = parseTables(options.get("--tables"));
            Format format = Format.from(formatValue);

            try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + input.toAbsolutePath())) {
                TableDescriptorProvider provider = new GeoPackageTableDescriptorProvider(tables);
                switch (format) {
                    case FLATGEOBUF -> {
                        FlatGeobufExporter exporter = new FlatGeobufExporter(new GeoPackageGeometryReader());
                        exporter.exportTables(connection, provider, outputDir);
                    }
                    case PARQUET -> {
                        ParquetExporter exporter = new ParquetExporter(new GeoPackageGeometryReader());
                        exporter.exportTables(connection, provider, outputDir);
                    }
                }
            } catch (SQLException e) {
                throw new IllegalArgumentException(e.getMessage(), e);
            }

            out.printf("Export completed (%s).%n", format.name().toLowerCase(Locale.ROOT));
            return 0;
        }

        private Map<String, String> parseOptions(String[] args) {
            Map<String, String> options = new HashMap<>();
            for (int index = 0; index < args.length; index++) {
                String arg = args[index];
                if (!arg.startsWith("--")) {
                    throw new IllegalArgumentException("Unexpected argument: " + arg);
                }
                if (index + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for option: " + arg);
                }
                String value = args[++index];
                if (options.put(arg, value) != null) {
                    throw new IllegalArgumentException("Option provided multiple times: " + arg);
                }
            }
            return options;
        }

        private Path requirePath(Map<String, String> options, String name) {
            String value = requireValue(options, name);
            return Path.of(value);
        }

        private String requireValue(Map<String, String> options, String name) {
            String value = options.get(name);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("Missing required option: " + name);
            }
            return value;
        }

        private List<String> parseTables(String tablesValue) {
            if (tablesValue == null || tablesValue.isBlank()) {
                return List.of();
            }
            String[] parts = tablesValue.split(";");
            List<String> tables = new ArrayList<>();
            for (String raw : parts) {
                String trimmed = raw.trim();
                if (trimmed.length() < 2 || trimmed.charAt(0) != '"' || trimmed.charAt(trimmed.length() - 1) != '"') {
                    throw new IllegalArgumentException("Table names must be wrapped in double quotes.");
                }
                String table = trimmed.substring(1, trimmed.length() - 1);
                if (table.isBlank()) {
                    throw new IllegalArgumentException("Table names must not be empty.");
                }
                tables.add(table);
            }
            return tables;
        }
    }

    private enum Format {
        FLATGEOBUF,
        PARQUET;

        private static Format from(String value) {
            return switch (value.toLowerCase(Locale.ROOT)) {
                case "flatgeobuf" -> FLATGEOBUF;
                case "parquet" -> PARQUET;
                default -> throw new IllegalArgumentException("Unsupported format: " + value);
            };
        }
    }
}

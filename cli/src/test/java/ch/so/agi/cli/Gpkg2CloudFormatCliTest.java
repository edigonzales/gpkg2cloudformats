package ch.so.agi.cli;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.wololo.flatgeobuf.HeaderMeta;

class Gpkg2CloudFormatCliTest {
    @TempDir
    Path tempDir;

    @Test
    void exportsFlatGeobufForSelectedTable() throws Exception {
        Path geopackage = resourcePath();
        Path outputDir = Files.createDirectory(tempDir.resolve("fgb-out"));

        int exitCode = runCli(
                "--input", geopackage.toString(),
                "--output", outputDir.toString(),
                "--tables", "\"abbaustelle\"",
                "--format", "flatgeobuf");

        assertThat(exitCode).isZero();
        Path output = outputDir.resolve("abbaustelle.fgb");
        assertThat(output).exists();
        assertThat(outputDir.resolve("surfacestructure.fgb")).doesNotExist();
        try (var input = Files.newInputStream(output)) {
            HeaderMeta header = HeaderMeta.read(input);
            assertThat(header.featuresCount).isPositive();
        }
    }

    @Test
    void exportsParquet() throws Exception {
        Path geopackage = resourcePath();
        Path outputDir = Files.createDirectory(tempDir.resolve("parquet-out"));

        int exitCode = runCli(
                "--input", geopackage.toString(),
                "--output", outputDir.toString(),
                "--format", "parquet",
                "--parquet-row-group-size", "65536");

        assertThat(exitCode).isZero();
        Path output = outputDir.resolve("abbaustelle.parquet");
        assertThat(output).exists();
        assertThat(Files.size(output)).isGreaterThan(4L);
        byte[] magic = new byte[4];
        try (var input = Files.newInputStream(output)) {
            assertThat(input.read(magic)).isEqualTo(4);
        }
        String header = new String(magic, StandardCharsets.US_ASCII);
        assertThat(header.toUpperCase(Locale.ROOT)).isEqualTo("PAR1");
    }

    @Test
    void failsWhenOutputDirectoryMissing() throws Exception {
        Path geopackage = resourcePath();
        Path outputDir = tempDir.resolve("missing");
        ByteArrayOutputStream err = new ByteArrayOutputStream();

        int exitCode = Gpkg2CloudFormatCli.run(
                new String[]{"--input", geopackage.toString(), "--output", outputDir.toString(), "--format", "flatgeobuf"},
                new PrintStream(ByteArrayOutputStream.nullOutputStream()),
                new PrintStream(err));

        assertThat(exitCode).isEqualTo(2);
        assertThat(err.toString()).contains("Output directory does not exist");
    }

    private int runCli(String... args) throws IOException {
        return Gpkg2CloudFormatCli.run(
                args,
                new PrintStream(ByteArrayOutputStream.nullOutputStream()),
                new PrintStream(ByteArrayOutputStream.nullOutputStream()));
    }

    private Path resourcePath() throws Exception {
        return Path.of(getClass().getResource("/data/ch.so.afu.abbaustellen.gpkg").toURI());
    }
}

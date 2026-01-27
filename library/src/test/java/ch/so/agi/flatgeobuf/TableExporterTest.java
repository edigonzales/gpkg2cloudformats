package ch.so.agi.flatgeobuf;

import ch.so.agi.cloudformats.TableDescriptor;
import ch.so.agi.cloudformats.TableDescriptorProvider;
import ch.so.agi.cloudformats.TableExporter;
import ch.so.agi.cloudformats.TableWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

class TableExporterTest {
    @TempDir
    Path tempDir;

    @Test
    void exportsUsingWriterExtension() throws SQLException, IOException {
        TableDescriptor descriptor = TableDescriptor.of("roads", "geom", 2056, 1);
        TableDescriptorProvider provider = connection -> List.of(descriptor);
        RecordingWriter writer = new RecordingWriter();

        new TableExporter().exportTables(null, provider, tempDir, writer);

        assertThat(writer.paths).containsExactly(tempDir.resolve("roads.test"));
    }

    private static final class RecordingWriter implements TableWriter<String> {
        private final List<Path> paths = new java.util.ArrayList<>();

        @Override
        public String fileExtension() {
            return "test";
        }

        @Override
        public String defaultOptions() {
            return "options";
        }

        @Override
        public void writeTable(Connection connection, TableDescriptor table, Path outputFile, String options) {
            paths.add(outputFile);
        }
    }
}

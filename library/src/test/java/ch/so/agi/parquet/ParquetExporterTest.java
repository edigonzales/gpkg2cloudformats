package ch.so.agi.parquet;

import ch.so.agi.cloudformats.TableDescriptor;
import ch.so.agi.cloudformats.TableDescriptorProvider;
import ch.so.agi.cloudformats.WkbGeometryReader;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetExporterTest {
    @TempDir
    Path tempDir;

    @Test
    void exportsTablesToParquet() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        WKBWriter wkbWriter = new WKBWriter();
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE features (id INTEGER, geom BLOB)");
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO features (id, geom) VALUES (?, ?)")) {
                for (int i = 0; i < 3; i++) {
                    insert.setInt(1, i);
                    insert.setBytes(2, wkbWriter.write(geometryFactory.createPoint(new Coordinate(i, i))));
                    insert.executeUpdate();
                }
            }

            TableDescriptor descriptor = TableDescriptor.of("features", "geom", 2056, 1);
            TableDescriptorProvider provider = connectionHandle -> List.of(descriptor);
            ParquetExporter exporter = new ParquetExporter(new WkbGeometryReader());

            exporter.exportTables(connection, provider, tempDir);
        }

        Path outputFile = tempDir.resolve("features.parquet");
        assertThat(outputFile).exists();
        assertThat(outputFile).isNotEmptyFile();
    }
}

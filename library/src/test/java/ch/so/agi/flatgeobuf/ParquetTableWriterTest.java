package ch.so.agi.flatgeobuf;

import ch.so.agi.cloudformats.TableDescriptor;
import ch.so.agi.cloudformats.WkbGeometryReader;
import ch.so.agi.parquet.ParquetTableWriter;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.List;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.io.WKBWriter;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetTableWriterTest {
    @TempDir
    Path tempDir;

    @Test
    void writesGeometryLogicalTypeAndRowGroups() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        WKBWriter wkbWriter = new WKBWriter();
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE features (id INTEGER, name TEXT, geom BLOB)");
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO features (id, name, geom) VALUES (?, ?, ?)")) {
                for (int i = 0; i < 10; i++) {
                    insert.setInt(1, i);
                    insert.setString(2, "name-" + i + "-".repeat(200));
                    insert.setBytes(3, wkbWriter.write(geometryFactory.createPoint(new Coordinate(i, i))));
                    insert.executeUpdate();
                }
            }
            TableDescriptor descriptor = TableDescriptor.of("features", "geom", 2056, 1);
            ParquetTableWriter writer = new ParquetTableWriter(new WkbGeometryReader());
            ParquetTableWriter.ParquetWriteOptions options = ParquetTableWriter.ParquetWriteOptions.builder()
                    .rowGroupSize(1)
                    .build();
            Path outputFile = tempDir.resolve("features.parquet");
            writer.writeTable(connection, descriptor, outputFile, options);

            try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(outputFile))) {
                MessageType schema = reader.getFooter().getFileMetaData().getSchema();
                PrimitiveType geomType = schema.getType("geom").asPrimitiveType();
                LogicalTypeAnnotation logicalType = geomType.getLogicalTypeAnnotation();
                assertThat(logicalType).isInstanceOf(LogicalTypeAnnotation.GeometryLogicalTypeAnnotation.class);
                assertThat(reader.getFooter().getBlocks()).isNotEmpty();
            }
        }
    }

    @Test
    void allowsSettingRowGroupSize() {
        ParquetTableWriter.ParquetWriteOptions options = ParquetTableWriter.ParquetWriteOptions.builder()
                .rowGroupSize(512)
                .build();

        assertThat(options.rowGroupSize()).isEqualTo(512);
    }

    @Test
    void writesParquetWithoutGeometryColumn() throws Exception {
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:");
             Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE attributes (id INTEGER, name TEXT)");
            statement.executeUpdate("INSERT INTO attributes (id, name) VALUES (1, 'alpha')");
            statement.executeUpdate("INSERT INTO attributes (id, name) VALUES (2, 'beta')");

            TableDescriptor descriptor = new TableDescriptor("attributes", null, 0, (byte) 0);
            ParquetTableWriter writer = new ParquetTableWriter(new WkbGeometryReader());
            Path outputFile = tempDir.resolve("attributes.parquet");
            writer.writeTable(connection, descriptor, outputFile, writer.defaultOptions());

            try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(outputFile))) {
                MessageType schema = reader.getFooter().getFileMetaData().getSchema();
                List<String> fieldNames = schema.getFields().stream().map(field -> field.getName()).toList();
                assertThat(fieldNames).containsExactly("id", "name");
            }
        }
    }
}

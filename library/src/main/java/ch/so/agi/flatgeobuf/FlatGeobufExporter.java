package ch.so.agi.flatgeobuf;

import ch.so.agi.cloudformats.GeometryReader;
import ch.so.agi.cloudformats.TableDescriptorProvider;
import ch.so.agi.cloudformats.TableExporter;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

public class FlatGeobufExporter {
    private final FlatGeobufTableWriter tableWriter;
    private final TableExporter tableExporter;

    public FlatGeobufExporter(GeometryReader geometryReader) {
        this.tableWriter = new FlatGeobufTableWriter(geometryReader);
        this.tableExporter = new TableExporter();
    }

    public void exportTables(Connection connection, TableDescriptorProvider tableDescriptorProvider, Path outputDirectory)
            throws SQLException, IOException {
        exportTables(connection, tableDescriptorProvider, outputDirectory, tableWriter.defaultOptions());
    }

    public void exportTables(Connection connection,
                             TableDescriptorProvider tableDescriptorProvider,
                             Path outputDirectory,
                             FlatGeobufTableWriter.FlatGeobufWriteOptions options)
            throws SQLException, IOException {
        tableExporter.exportTables(connection, tableDescriptorProvider, outputDirectory, tableWriter, options);
    }
}

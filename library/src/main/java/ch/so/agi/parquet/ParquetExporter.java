package ch.so.agi.parquet;

import ch.so.agi.cloudformats.GeometryReader;
import ch.so.agi.cloudformats.TableDescriptorProvider;
import ch.so.agi.cloudformats.TableExporter;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

public class ParquetExporter {
    private final ParquetTableWriter tableWriter;
    private final TableExporter tableExporter;

    public ParquetExporter(GeometryReader geometryReader) {
        this.tableWriter = new ParquetTableWriter(geometryReader);
        this.tableExporter = new TableExporter();
    }

    public void exportTables(Connection connection, TableDescriptorProvider tableDescriptorProvider, Path outputDirectory)
            throws SQLException, IOException {
        exportTables(connection, tableDescriptorProvider, outputDirectory, tableWriter.defaultOptions());
    }

    public void exportTables(Connection connection,
                             TableDescriptorProvider tableDescriptorProvider,
                             Path outputDirectory,
                             ParquetTableWriter.ParquetWriteOptions options) throws SQLException, IOException {
        tableExporter.exportTables(connection, tableDescriptorProvider, outputDirectory, tableWriter, options);
    }
}

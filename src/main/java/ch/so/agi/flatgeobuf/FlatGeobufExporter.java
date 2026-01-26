package ch.so.agi.flatgeobuf;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class FlatGeobufExporter {
    private final FlatGeobufTableWriter tableWriter;

    public FlatGeobufExporter(GeometryReader geometryReader) {
        this.tableWriter = new FlatGeobufTableWriter(geometryReader);
    }

    public void exportTables(Connection connection, TableDescriptorProvider tableDescriptorProvider, Path outputDirectory)
            throws SQLException, IOException {
        List<TableDescriptor> tables = tableDescriptorProvider.listTables(connection);
        for (TableDescriptor table : tables) {
            Path target = outputDirectory.resolve(table.tableName() + ".fgb");
            try (OutputStream out = Files.newOutputStream(target)) {
                tableWriter.writeTable(connection, table, out);
            }
        }
    }
}

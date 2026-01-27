package ch.so.agi.cloudformats;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public class TableExporter {
    public <O> void exportTables(Connection connection,
                                 TableDescriptorProvider tableDescriptorProvider,
                                 Path outputDirectory,
                                 TableWriter<O> tableWriter) throws SQLException, IOException {
        exportTables(connection, tableDescriptorProvider, outputDirectory, tableWriter, tableWriter.defaultOptions());
    }

    public <O> void exportTables(Connection connection,
                                 TableDescriptorProvider tableDescriptorProvider,
                                 Path outputDirectory,
                                 TableWriter<O> tableWriter,
                                 O options) throws SQLException, IOException {
        List<TableDescriptor> tables = tableDescriptorProvider.listTables(connection);
        for (TableDescriptor table : tables) {
            Path target = outputDirectory.resolve(table.tableName() + "." + tableWriter.fileExtension());
            tableWriter.writeTable(connection, table, target, options);
        }
    }
}

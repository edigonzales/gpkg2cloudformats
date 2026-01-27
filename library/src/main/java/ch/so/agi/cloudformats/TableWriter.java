package ch.so.agi.cloudformats;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;

public interface TableWriter<O> {
    String fileExtension();

    O defaultOptions();

    void writeTable(Connection connection, TableDescriptor table, Path outputFile, O options)
            throws SQLException, IOException;
}

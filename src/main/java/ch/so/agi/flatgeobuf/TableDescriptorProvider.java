package ch.so.agi.flatgeobuf;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

public interface TableDescriptorProvider {
    List<TableDescriptor> listTables(Connection connection) throws SQLException;
}

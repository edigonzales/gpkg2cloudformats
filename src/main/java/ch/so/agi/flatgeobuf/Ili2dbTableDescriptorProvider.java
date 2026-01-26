package ch.so.agi.flatgeobuf;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.wololo.flatgeobuf.generated.GeometryType;

public class Ili2dbTableDescriptorProvider implements TableDescriptorProvider {
    private static final String QUERY = """
            SELECT t.tablename,
                   g.column_name,
                   g.geometry_type_name,
                   g.srs_id
              FROM T_ILI2DB_TABLE_PROP t
              JOIN gpkg_geometry_columns g
                ON g.table_name = t.tablename
             WHERE t.setting = 'CLASS'
            """;

    @Override
    public List<TableDescriptor> listTables(Connection connection) throws SQLException {
        List<TableDescriptor> tables = new ArrayList<>();
        try (PreparedStatement statement = connection.prepareStatement(QUERY);
             ResultSet resultSet = statement.executeQuery()) {
            while (resultSet.next()) {
                String tableName = resultSet.getString("tablename");
                String geometryColumn = resultSet.getString("column_name");
                String geometryType = resultSet.getString("geometry_type_name");
                int srid = resultSet.getInt("srs_id");
                tables.add(new TableDescriptor(tableName, geometryColumn, srid, toGeometryType(geometryType)));
            }
        }
        return tables;
    }

    private static byte toGeometryType(String geometryType) throws SQLException {
        if (geometryType == null) {
            return (byte) GeometryType.Unknown;
        }
        return switch (geometryType.toUpperCase(Locale.ROOT)) {
            case "POINT" -> (byte) GeometryType.Point;
            case "LINESTRING" -> (byte) GeometryType.LineString;
            case "POLYGON" -> (byte) GeometryType.Polygon;
            case "MULTIPOINT" -> (byte) GeometryType.MultiPoint;
            case "MULTILINESTRING" -> (byte) GeometryType.MultiLineString;
            case "MULTIPOLYGON" -> (byte) GeometryType.MultiPolygon;
            default -> throw new SQLException("Unsupported geometry type: " + geometryType);
        };
    }
}

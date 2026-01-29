package ch.so.agi.cloudformats;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.wololo.flatgeobuf.generated.GeometryType;

public class GeoPackageTableDescriptorProvider implements TableDescriptorProvider {
    private static final String TABLE_QUERY = """
            SELECT tablename
              FROM T_ILI2DB_TABLE_PROP
             WHERE setting = 'CLASS'
            """;

    private static final String GEOMETRY_QUERY = """
            SELECT table_name,
                   column_name,
                   geometry_type_name,
                   srs_id
              FROM gpkg_geometry_columns
            """;

    private final List<String> tables;

    public GeoPackageTableDescriptorProvider(List<String> tables) {
        this.tables = tables == null ? Collections.emptyList() : List.copyOf(tables);
    }

    @Override
    public List<TableDescriptor> listTables(Connection connection) throws SQLException {
        List<String> tableNames = loadTableNames(connection);
        validateTables(Set.copyOf(tableNames));

        Map<String, GeometryInfo> geometryInfo = loadGeometryInfo(connection, tableNames);
        Map<String, TableDescriptor> descriptors = new LinkedHashMap<>();
        for (String tableName : tableNames) {
            GeometryInfo info = geometryInfo.get(tableName);
            if (info == null) {
                descriptors.put(tableName,
                        new TableDescriptor(tableName, null, 0, (byte) GeometryType.Unknown));
            } else {
                descriptors.put(tableName,
                        new TableDescriptor(tableName, info.columnName(), info.srid(), toGeometryType(info.geometryType())));
            }
        }

        return new ArrayList<>(descriptors.values());
    }

    private List<String> loadTableNames(Connection connection) throws SQLException {
        List<String> tableNames = new ArrayList<>();
        String query = buildTableQuery();
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            bindTableNames(statement, tables);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    tableNames.add(resultSet.getString("tablename"));
                }
            }
        }
        return tableNames;
    }

    private Map<String, GeometryInfo> loadGeometryInfo(Connection connection, List<String> tableNames) throws SQLException {
        if (tableNames.isEmpty()) {
            return Map.of();
        }
        String query = buildGeometryQuery(tableNames);
        Map<String, GeometryInfo> geometryInfo = new LinkedHashMap<>();
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            bindTableNames(statement, tableNames);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("table_name");
                    String geometryColumn = resultSet.getString("column_name");
                    String geometryType = resultSet.getString("geometry_type_name");
                    int srid = resultSet.getInt("srs_id");
                    geometryInfo.put(tableName, new GeometryInfo(geometryColumn, geometryType, srid));
                }
            }
        }
        return geometryInfo;
    }

    private String buildTableQuery() {
        if (tables.isEmpty()) {
            return TABLE_QUERY;
        }
        String placeholders = tables.stream().map(value -> "?").collect(Collectors.joining(", "));
        return TABLE_QUERY + " AND tablename IN (" + placeholders + ")";
    }

    private String buildGeometryQuery(List<String> tableNames) {
        String placeholders = tableNames.stream().map(value -> "?").collect(Collectors.joining(", "));
        return GEOMETRY_QUERY + " WHERE table_name IN (" + placeholders + ")";
    }

    private void bindTableNames(PreparedStatement statement, List<String> names) throws SQLException {
        for (int index = 0; index < names.size(); index++) {
            statement.setString(index + 1, names.get(index));
        }
    }

    private void validateTables(Set<String> foundTables) throws SQLException {
        if (tables.isEmpty()) {
            return;
        }
        List<String> missing = tables.stream().filter(table -> !foundTables.contains(table)).toList();
        if (!missing.isEmpty()) {
            throw new SQLException("Unknown table(s) requested: " + String.join(", ", missing));
        }
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

    private record GeometryInfo(String columnName, String geometryType, int srid) {
    }
}

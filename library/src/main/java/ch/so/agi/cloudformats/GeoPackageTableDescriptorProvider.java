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
    private static final String BASE_QUERY = """
            SELECT c.table_name,
                   g.column_name,
                   g.geometry_type_name,
                   g.srs_id
              FROM gpkg_contents c
              JOIN gpkg_geometry_columns g
                ON c.table_name = g.table_name
             WHERE c.data_type = 'features'
            """;

    private final List<String> tables;

    public GeoPackageTableDescriptorProvider(List<String> tables) {
        this.tables = tables == null ? Collections.emptyList() : List.copyOf(tables);
    }

    @Override
    public List<TableDescriptor> listTables(Connection connection) throws SQLException {
        Map<String, TableDescriptor> descriptors = new LinkedHashMap<>();
        String query = buildQuery();
        try (PreparedStatement statement = connection.prepareStatement(query)) {
            bindTableNames(statement);
            try (ResultSet resultSet = statement.executeQuery()) {
                while (resultSet.next()) {
                    String tableName = resultSet.getString("table_name");
                    String geometryColumn = resultSet.getString("column_name");
                    String geometryType = resultSet.getString("geometry_type_name");
                    int srid = resultSet.getInt("srs_id");
                    descriptors.put(tableName,
                            new TableDescriptor(tableName, geometryColumn, srid, toGeometryType(geometryType)));
                }
            }
        }
        validateTables(descriptors.keySet());
        return new ArrayList<>(descriptors.values());
    }

    private String buildQuery() {
        if (tables.isEmpty()) {
            return BASE_QUERY;
        }
        String placeholders = tables.stream().map(value -> "?").collect(Collectors.joining(", "));
        return BASE_QUERY + " AND c.table_name IN (" + placeholders + ")";
    }

    private void bindTableNames(PreparedStatement statement) throws SQLException {
        for (int index = 0; index < tables.size(); index++) {
            statement.setString(index + 1, tables.get(index));
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
}

package ch.so.agi.flatgeobuf;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

public class WkbGeometryReader implements GeometryReader {
    private final WKBReader wkbReader = new WKBReader();

    @Override
    public Geometry readGeometry(ResultSet resultSet, String columnName) throws SQLException {
        byte[] bytes = resultSet.getBytes(columnName);
        if (bytes == null) {
            return null;
        }
        try {
            return wkbReader.read(bytes);
        } catch (ParseException e) {
            throw new SQLException("Unable to parse WKB geometry.", e);
        }
    }
}

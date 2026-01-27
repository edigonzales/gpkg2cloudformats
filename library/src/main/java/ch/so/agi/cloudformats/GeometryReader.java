package ch.so.agi.cloudformats;

import java.sql.ResultSet;
import java.sql.SQLException;
import org.locationtech.jts.geom.Geometry;

public interface GeometryReader {
    Geometry readGeometry(ResultSet resultSet, String columnName) throws SQLException;
}

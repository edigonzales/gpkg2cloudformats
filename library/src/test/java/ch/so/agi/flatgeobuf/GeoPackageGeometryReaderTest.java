package ch.so.agi.flatgeobuf;

import ch.so.agi.cloudformats.GeoPackageGeometryReader;

import static org.assertj.core.api.Assertions.assertThat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKBWriter;
import org.mockito.Mockito;

class GeoPackageGeometryReaderTest {
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final WKBWriter wkbWriter = new WKBWriter();

    @Test
    void readsPointGeometry() throws Exception {
        Point point = geometryFactory.createPoint(new org.locationtech.jts.geom.Coordinate(7, 47));
        Geometry geometry = readGeometry(point);
        assertThat(geometry).isInstanceOf(Point.class);
    }

    @Test
    void readsLineAndPolygonGeometries() throws Exception {
        LineString line = geometryFactory.createLineString(new org.locationtech.jts.geom.Coordinate[] {
                new org.locationtech.jts.geom.Coordinate(0, 0),
                new org.locationtech.jts.geom.Coordinate(1, 1)
        });
        Polygon polygon = geometryFactory.createPolygon(new org.locationtech.jts.geom.Coordinate[] {
                new org.locationtech.jts.geom.Coordinate(0, 0),
                new org.locationtech.jts.geom.Coordinate(0, 2),
                new org.locationtech.jts.geom.Coordinate(2, 2),
                new org.locationtech.jts.geom.Coordinate(0, 0)
        });

        assertThat(readGeometry(line)).isInstanceOf(LineString.class);
        assertThat(readGeometry(polygon)).isInstanceOf(Polygon.class);
    }

    @Test
    void readsMultiGeometries() throws Exception {
        MultiPoint multiPoint = geometryFactory.createMultiPointFromCoords(new org.locationtech.jts.geom.Coordinate[] {
                new org.locationtech.jts.geom.Coordinate(0, 0),
                new org.locationtech.jts.geom.Coordinate(1, 1)
        });
        MultiLineString multiLineString = geometryFactory.createMultiLineString(new LineString[] {
                geometryFactory.createLineString(new org.locationtech.jts.geom.Coordinate[] {
                        new org.locationtech.jts.geom.Coordinate(0, 0),
                        new org.locationtech.jts.geom.Coordinate(1, 1)
                })
        });
        MultiPolygon multiPolygon = geometryFactory.createMultiPolygon(new Polygon[] {
                geometryFactory.createPolygon(new org.locationtech.jts.geom.Coordinate[] {
                        new org.locationtech.jts.geom.Coordinate(0, 0),
                        new org.locationtech.jts.geom.Coordinate(0, 2),
                        new org.locationtech.jts.geom.Coordinate(2, 2),
                        new org.locationtech.jts.geom.Coordinate(0, 0)
                })
        });

        assertThat(readGeometry(multiPoint)).isInstanceOf(MultiPoint.class);
        assertThat(readGeometry(multiLineString)).isInstanceOf(MultiLineString.class);
        assertThat(readGeometry(multiPolygon)).isInstanceOf(MultiPolygon.class);
    }

    private Geometry readGeometry(Geometry geometry) throws SQLException {
        byte[] gpkg = toGeoPackageBlob(geometry);
        ResultSet resultSet = Mockito.mock(ResultSet.class);
        Mockito.when(resultSet.getBytes("geom")).thenReturn(gpkg);
        GeoPackageGeometryReader reader = new GeoPackageGeometryReader();
        return reader.readGeometry(resultSet, "geom");
    }

    private byte[] toGeoPackageBlob(Geometry geometry) {
        byte[] wkb = wkbWriter.write(geometry);
        ByteBuffer buffer = ByteBuffer.allocate(8 + wkb.length).order(ByteOrder.LITTLE_ENDIAN);
        buffer.put((byte) 0x47);
        buffer.put((byte) 0x50);
        buffer.put((byte) 0); // version
        buffer.put((byte) 1); // flags: little endian, no envelope
        buffer.putInt(2056);
        buffer.put(wkb);
        return buffer.array();
    }
}

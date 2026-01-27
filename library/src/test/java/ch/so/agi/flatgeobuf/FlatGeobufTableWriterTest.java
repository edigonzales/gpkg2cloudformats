package ch.so.agi.flatgeobuf;

import ch.so.agi.cloudformats.TableDescriptor;
import ch.so.agi.cloudformats.WkbGeometryReader;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKBWriter;
import org.wololo.flatgeobuf.HeaderMeta;
import org.wololo.flatgeobuf.generated.GeometryType;

class FlatGeobufTableWriterTest {
    private final GeometryFactory geometryFactory = new GeometryFactory();
    private final WKBWriter wkbWriter = new WKBWriter();
    private Connection connection;

    @BeforeEach
    void setUp() throws Exception {
        connection = DriverManager.getConnection("jdbc:sqlite::memory:");
    }

    @AfterEach
    void tearDown() throws Exception {
        connection.close();
    }

    @ParameterizedTest
    @MethodSource("geometryCases")
    void writesFlatGeobufForGeometryTypes(String tableName, Geometry geometry, int geometryType) throws Exception {
        createTable(tableName);
        insertFeature(tableName, 1, "one", geometry);
        insertFeature(tableName, 2, "two", geometry);

        FlatGeobufTableWriter writer = new FlatGeobufTableWriter(new WkbGeometryReader());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        writer.writeTable(connection, new TableDescriptor(tableName, "geom", 4326, (byte) geometryType), out);

        byte[] bytes = out.toByteArray();
        HeaderMeta header = FlatGeobufTestSupport.readHeader(bytes);
        assertThat(header.featuresCount).isEqualTo(2);
        assertThat(header.geometryType).isEqualTo((byte) geometryType);
        assertThat(header.indexNodeSize).isPositive();

        List<Geometry> geometries = FlatGeobufTestSupport.readGeometries(bytes);
        assertThat(geometries).hasSize(2);
        geometries.forEach(value -> assertThat(value.getGeometryType())
                .isEqualTo(geometry.getGeometryType()));
    }

    private void createTable(String tableName) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(
                "CREATE TABLE " + tableName + " (id INTEGER, name TEXT, geom BLOB)")) {
            statement.execute();
        }
    }

    private void insertFeature(String tableName, int id, String name, Geometry geometry) throws Exception {
        try (PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO " + tableName + " (id, name, geom) VALUES (?, ?, ?)")) {
            statement.setInt(1, id);
            statement.setString(2, name);
            statement.setBytes(3, wkbWriter.write(geometry));
            statement.executeUpdate();
        }
    }

    private static Stream<Arguments> geometryCases() {
        GeometryFactory geometryFactory = new GeometryFactory();
        Point point = geometryFactory.createPoint(new org.locationtech.jts.geom.Coordinate(0, 0));
        LineString lineString = geometryFactory.createLineString(new org.locationtech.jts.geom.Coordinate[] {
                new org.locationtech.jts.geom.Coordinate(0, 0),
                new org.locationtech.jts.geom.Coordinate(1, 1)
        });
        Polygon polygon = geometryFactory.createPolygon(new org.locationtech.jts.geom.Coordinate[] {
                new org.locationtech.jts.geom.Coordinate(0, 0),
                new org.locationtech.jts.geom.Coordinate(0, 2),
                new org.locationtech.jts.geom.Coordinate(2, 2),
                new org.locationtech.jts.geom.Coordinate(0, 0)
        });
        MultiPoint multiPoint = geometryFactory.createMultiPoint(new Point[] { point });
        MultiLineString multiLineString = geometryFactory.createMultiLineString(new LineString[] { lineString });
        MultiPolygon multiPolygon = geometryFactory.createMultiPolygon(new Polygon[] { polygon });
        return Stream.of(
                Arguments.of("points", point, GeometryType.Point),
                Arguments.of("lines", lineString, GeometryType.LineString),
                Arguments.of("polygons", polygon, GeometryType.Polygon),
                Arguments.of("multipoints", multiPoint, GeometryType.MultiPoint),
                Arguments.of("multilines", multiLineString, GeometryType.MultiLineString),
                Arguments.of("multipolygons", multiPolygon, GeometryType.MultiPolygon)
        );
    }
}

package ch.so.agi.flatgeobuf;

import ch.so.agi.cloudformats.TableDescriptor;
import ch.so.agi.cloudformats.WkbGeometryReader;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileInputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.io.WKBWriter;
import org.wololo.flatgeobuf.HeaderMeta;
import org.wololo.flatgeobuf.PackedRTree;
import org.wololo.flatgeobuf.PackedRTree.SearchHit;
import org.wololo.flatgeobuf.generated.Feature;
import org.wololo.flatgeobuf.generated.GeometryType;

class FlatGeobufStreamingTest {
    @Test
    void readsFeatureWithRangeRequests() throws Exception {
        GeometryFactory geometryFactory = new GeometryFactory();
        Polygon polygon = geometryFactory.createPolygon(new org.locationtech.jts.geom.Coordinate[] {
                new org.locationtech.jts.geom.Coordinate(0, 0),
                new org.locationtech.jts.geom.Coordinate(0, 2),
                new org.locationtech.jts.geom.Coordinate(2, 2),
                new org.locationtech.jts.geom.Coordinate(0, 0)
        });
        byte[] wkb = new WKBWriter().write(polygon);
        File tempFile = File.createTempFile("flatgeobuf", ".fgb");
        tempFile.deleteOnExit();

        try (Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:");
             PreparedStatement create = connection.prepareStatement(
                     "CREATE TABLE polygons (id INTEGER, name TEXT, geom BLOB)")) {
            create.execute();
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO polygons (id, name, geom) VALUES (?, ?, ?)")) {
                insert.setInt(1, 1);
                insert.setString(2, "one");
                insert.setBytes(3, wkb);
                insert.executeUpdate();
            }

            FlatGeobufTableWriter writer = new FlatGeobufTableWriter(new WkbGeometryReader());
            try (var out = new java.io.FileOutputStream(tempFile)) {
                writer.writeTable(connection, new TableDescriptor("polygons", "geom", 4326, (byte) GeometryType.Polygon), out);
            }
        }

        HeaderMeta header;
        try (FileInputStream in = new FileInputStream(tempFile)) {
            header = HeaderMeta.read(in);
        }

        int indexSize = (int) PackedRTree.calcSize((int) header.featuresCount, header.indexNodeSize);
        ByteBuffer indexBuffer = ByteBuffer.allocate(indexSize).order(ByteOrder.LITTLE_ENDIAN);
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "r");
             FileChannel channel = raf.getChannel()) {
            channel.read(indexBuffer, header.offset);
        }
        indexBuffer.flip();

        var hits = PackedRTree.search(indexBuffer, 0, (int) header.featuresCount, header.indexNodeSize, header.envelope);
        assertThat(hits).isNotNull();
        assertThat(hits).isNotEmpty();
        SearchHit hit = hits.get(0);

        long featureStart = header.offset + indexSize + hit.offset;
        int featureSize;
        byte[] featureBytes;
        try (RandomAccessFile raf = new RandomAccessFile(tempFile, "r")) {
            raf.seek(featureStart);
            byte[] sizeBytes = new byte[Integer.BYTES];
            raf.readFully(sizeBytes);
            featureSize = ByteBuffer.wrap(sizeBytes).order(ByteOrder.LITTLE_ENDIAN).getInt();
            featureBytes = new byte[featureSize];
            raf.readFully(featureBytes);
        }

        ByteBuffer featureBuffer = ByteBuffer.wrap(featureBytes).order(ByteOrder.LITTLE_ENDIAN);
        Feature feature = Feature.getRootAsFeature(featureBuffer);
        assertThat(feature.geometry()).isNotNull();
    }
}

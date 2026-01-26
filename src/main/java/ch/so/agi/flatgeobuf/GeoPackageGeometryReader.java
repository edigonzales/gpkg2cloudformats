package ch.so.agi.flatgeobuf;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.ResultSet;
import java.sql.SQLException;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;

public class GeoPackageGeometryReader implements GeometryReader {
    private static final byte MAGIC_1 = 0x47;
    private static final byte MAGIC_2 = 0x50;

    private final WKBReader wkbReader = new WKBReader();

    @Override
    public Geometry readGeometry(ResultSet resultSet, String columnName) throws SQLException {
        byte[] bytes = resultSet.getBytes(columnName);
        if (bytes == null) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        if (buffer.remaining() < 8) {
            throw new SQLException("Invalid GeoPackage geometry blob.");
        }
        byte magic1 = buffer.get();
        byte magic2 = buffer.get();
        if (magic1 != MAGIC_1 || magic2 != MAGIC_2) {
            throw new SQLException("Invalid GeoPackage geometry blob magic bytes.");
        }
        buffer.get(); // version
        byte flags = buffer.get();
        boolean littleEndian = (flags & 0x01) == 1;
        buffer.order(littleEndian ? ByteOrder.LITTLE_ENDIAN : ByteOrder.BIG_ENDIAN);
        buffer.getInt(); // srs_id
        int envelopeIndicator = (flags >> 1) & 0x07;
        int envelopeSize = envelopeSize(envelopeIndicator);
        if (buffer.remaining() < envelopeSize) {
            throw new SQLException("Invalid GeoPackage geometry envelope size.");
        }
        buffer.position(buffer.position() + envelopeSize);
        byte[] wkb = new byte[buffer.remaining()];
        buffer.get(wkb);
        try {
            return wkbReader.read(wkb);
        } catch (ParseException e) {
            throw new SQLException("Unable to parse WKB from GeoPackage geometry blob.", e);
        }
    }

    private static int envelopeSize(int envelopeIndicator) throws SQLException {
        return switch (envelopeIndicator) {
            case 0 -> 0;
            case 1 -> 4 * Double.BYTES;
            case 2, 3 -> 6 * Double.BYTES;
            case 4 -> 8 * Double.BYTES;
            default -> throw new SQLException("Unsupported GeoPackage envelope indicator: " + envelopeIndicator);
        };
    }
}

package ch.so.agi.flatgeobuf;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;
import org.wololo.flatgeobuf.GeometryConversions;
import org.wololo.flatgeobuf.HeaderMeta;
import org.wololo.flatgeobuf.PackedRTree;
import org.wololo.flatgeobuf.generated.ColumnType;
import org.wololo.flatgeobuf.generated.Feature;

final class FlatGeobufTestSupport {
    private FlatGeobufTestSupport() {
    }

    static HeaderMeta readHeader(byte[] bytes) throws Exception {
        ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
        return HeaderMeta.read(buffer);
    }

    static List<Geometry> readGeometries(byte[] bytes) throws Exception {
        HeaderMeta header = readHeader(bytes);
        int indexSize = header.indexNodeSize > 0
                ? (int) PackedRTree.calcSize((int) header.featuresCount, header.indexNodeSize)
                : 0;
        int offset = header.offset + indexSize;
        List<Geometry> geometries = new ArrayList<>();
        int position = offset;
        for (int i = 0; i < header.featuresCount; i++) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            int size = buffer.getInt(position);
            ByteBuffer featureBuffer = ByteBuffer.wrap(bytes, position + Integer.BYTES, size)
                    .order(ByteOrder.LITTLE_ENDIAN);
            Feature feature = Feature.getRootAsFeature(featureBuffer);
            Geometry geometry = GeometryConversions.deserialize(feature.geometry(), header.geometryType);
            geometries.add(geometry);
            position += Integer.BYTES + size;
        }
        return geometries;
    }

    static List<Map<String, Object>> readProperties(byte[] bytes) throws Exception {
        HeaderMeta header = readHeader(bytes);
        int indexSize = header.indexNodeSize > 0
                ? (int) PackedRTree.calcSize((int) header.featuresCount, header.indexNodeSize)
                : 0;
        int offset = header.offset + indexSize;
        List<Map<String, Object>> properties = new ArrayList<>();
        int position = offset;
        for (int i = 0; i < header.featuresCount; i++) {
            ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
            int size = buffer.getInt(position);
            ByteBuffer featureBuffer = ByteBuffer.wrap(bytes, position + Integer.BYTES, size)
                    .order(ByteOrder.LITTLE_ENDIAN);
            Feature feature = Feature.getRootAsFeature(featureBuffer);
            Map<String, Object> props = new LinkedHashMap<>();
            if (feature.propertiesLength() > 0) {
                ByteBuffer propsBuffer = feature.propertiesAsByteBuffer().order(ByteOrder.LITTLE_ENDIAN);
                while (propsBuffer.remaining() >= Short.BYTES) {
                    int columnIndex = Short.toUnsignedInt(propsBuffer.getShort());
                    if (columnIndex >= header.columns.size()) {
                        break;
                    }
                    var column = header.columns.get(columnIndex);
                    props.put(column.name, readValue(propsBuffer, column.type));
                }
            }
            properties.add(props);
            position += Integer.BYTES + size;
        }
        return properties;
    }

    private static Object readValue(ByteBuffer buffer, byte columnType) {
        return switch (columnType) {
            case ColumnType.Byte -> buffer.get();
            case ColumnType.UByte -> Byte.toUnsignedInt(buffer.get());
            case ColumnType.Bool -> buffer.get() == 1;
            case ColumnType.Short -> buffer.getShort();
            case ColumnType.UShort -> Short.toUnsignedInt(buffer.getShort());
            case ColumnType.Int -> buffer.getInt();
            case ColumnType.UInt -> Integer.toUnsignedLong(buffer.getInt());
            case ColumnType.Long, ColumnType.ULong -> buffer.getLong();
            case ColumnType.Float -> buffer.getFloat();
            case ColumnType.Double -> buffer.getDouble();
            case ColumnType.String, ColumnType.Json, ColumnType.DateTime -> readString(buffer);
            case ColumnType.Binary -> readBinary(buffer);
            default -> null;
        };
    }

    private static String readString(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return new String(bytes, java.nio.charset.StandardCharsets.UTF_8);
    }

    private static byte[] readBinary(ByteBuffer buffer) {
        int length = buffer.getInt();
        byte[] bytes = new byte[length];
        buffer.get(bytes);
        return bytes;
    }
}

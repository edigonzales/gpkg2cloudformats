package ch.so.agi.flatgeobuf;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Blob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import org.wololo.flatgeobuf.generated.ColumnType;

class FlatGeobufPropertiesWriter {
    private final List<FlatGeobufTableWriter.ColumnSpec> columns;

    FlatGeobufPropertiesWriter(List<FlatGeobufTableWriter.ColumnSpec> columns) {
        this.columns = columns;
    }

    byte[] write(ResultSet resultSet) throws SQLException {
        List<PropertyBuffer> buffers = new ArrayList<>();
        int totalSize = 0;
        for (int i = 0; i < columns.size(); i++) {
            FlatGeobufTableWriter.ColumnSpec column = columns.get(i);
            Object value = readValue(resultSet, column);
            if (value == null) {
                continue;
            }
            PropertyBuffer buffer = encodeValue(i, column, value);
            buffers.add(buffer);
            totalSize += buffer.size();
        }
        if (totalSize == 0) {
            return new byte[0];
        }
        ByteBuffer output = ByteBuffer.allocate(totalSize).order(ByteOrder.LITTLE_ENDIAN);
        for (PropertyBuffer buffer : buffers) {
            buffer.writeTo(output);
        }
        return output.array();
    }

    private static Object readValue(ResultSet resultSet, FlatGeobufTableWriter.ColumnSpec column) throws SQLException {
        int sqlType = column.sqlType();
        Object value = resultSet.getObject(column.name());
        if (resultSet.wasNull()) {
            return null;
        }
        if (sqlType == Types.BLOB || sqlType == Types.BINARY || sqlType == Types.VARBINARY
                || sqlType == Types.LONGVARBINARY) {
            if (value instanceof Blob blob) {
                return blob.getBytes(1, (int) blob.length());
            }
            return resultSet.getBytes(column.name());
        }
        return value;
    }

    private static PropertyBuffer encodeValue(int columnIndex, FlatGeobufTableWriter.ColumnSpec column, Object value)
            throws SQLException {
        ByteBuffer buffer;
        int columnType = column.columnType();
        return switch (columnType) {
            case ColumnType.Byte -> PropertyBuffer.fixed(columnIndex, 1, (bb) -> bb.put(((Number) value).byteValue()));
            case ColumnType.UByte -> PropertyBuffer.fixed(columnIndex, 1, (bb) -> bb.put((byte) ((Number) value).intValue()));
            case ColumnType.Bool -> PropertyBuffer.fixed(columnIndex, 1, (bb) -> bb.put(boolToByte(value)));
            case ColumnType.Short -> PropertyBuffer.fixed(columnIndex, Short.BYTES, (bb) -> bb.putShort(((Number) value).shortValue()));
            case ColumnType.UShort -> PropertyBuffer.fixed(columnIndex, Short.BYTES, (bb) -> bb.putShort((short) ((Number) value).intValue()));
            case ColumnType.Int -> PropertyBuffer.fixed(columnIndex, Integer.BYTES, (bb) -> bb.putInt(((Number) value).intValue()));
            case ColumnType.UInt -> PropertyBuffer.fixed(columnIndex, Integer.BYTES, (bb) -> bb.putInt((int) ((Number) value).longValue()));
            case ColumnType.Long -> PropertyBuffer.fixed(columnIndex, Long.BYTES, (bb) -> bb.putLong(((Number) value).longValue()));
            case ColumnType.ULong -> PropertyBuffer.fixed(columnIndex, Long.BYTES, (bb) -> bb.putLong(((Number) value).longValue()));
            case ColumnType.Float -> PropertyBuffer.fixed(columnIndex, Float.BYTES, (bb) -> bb.putFloat(((Number) value).floatValue()));
            case ColumnType.Double -> PropertyBuffer.fixed(columnIndex, Double.BYTES, (bb) -> bb.putDouble(((Number) value).doubleValue()));
            case ColumnType.String, ColumnType.Json, ColumnType.DateTime -> {
                String text = normalizeString(value, columnType == ColumnType.DateTime, column.dateOnly());
                byte[] bytes = text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
                buffer = ByteBuffer.allocate(Integer.BYTES + bytes.length).order(ByteOrder.LITTLE_ENDIAN);
                buffer.putInt(bytes.length);
                buffer.put(bytes);
                yield PropertyBuffer.variable(columnIndex, buffer.array());
            }
            case ColumnType.Binary -> {
                byte[] bytes = asBinary(value);
                buffer = ByteBuffer.allocate(Integer.BYTES + bytes.length).order(ByteOrder.LITTLE_ENDIAN);
                buffer.putInt(bytes.length);
                buffer.put(bytes);
                yield PropertyBuffer.variable(columnIndex, buffer.array());
            }
            default -> throw new SQLException("Unsupported column type: " + columnType);
        };
    }

    private static String normalizeString(Object value, boolean dateTime, boolean dateOnly) {
        if (dateOnly) {
            return normalizeDate(value);
        }
        if (!dateTime) {
            return value.toString();
        }
        if (value instanceof java.sql.Timestamp timestamp) {
            return timestamp.toInstant().atOffset(ZoneOffset.UTC).toString();
        }
        if (value instanceof java.sql.Date date) {
            return date.toLocalDate().toString();
        }
        if (value instanceof java.sql.Time time) {
            return time.toLocalTime().toString();
        }
        if (value instanceof OffsetDateTime offsetDateTime) {
            return offsetDateTime.toString();
        }
        return value.toString();
    }

    private static String normalizeDate(Object value) {
        if (value instanceof java.sql.Date date) {
            return date.toLocalDate().toString();
        }
        if (value instanceof java.time.LocalDate localDate) {
            return localDate.toString();
        }
        if (value instanceof java.util.Date date) {
            return Instant.ofEpochMilli(date.getTime()).atZone(ZoneOffset.UTC).toLocalDate().toString();
        }
        return value.toString();
    }

    private static byte boolToByte(Object value) {
        if (value instanceof Boolean booleanValue) {
            return (byte) (booleanValue ? 1 : 0);
        }
        if (value instanceof Number number) {
            return (byte) (number.intValue() != 0 ? 1 : 0);
        }
        return (byte) (!value.toString().equalsIgnoreCase("false") ? 1 : 0);
    }

    private static byte[] asBinary(Object value) throws SQLException {
        if (value instanceof byte[] bytes) {
            return bytes;
        }
        if (value instanceof Blob blob) {
            return blob.getBytes(1, (int) blob.length());
        }
        throw new SQLException("Unsupported binary value: " + value.getClass());
    }

    private record PropertyBuffer(int columnIndex, byte[] bytes, java.util.function.Consumer<ByteBuffer> writer) {
        static PropertyBuffer variable(int columnIndex, byte[] bytes) {
            return new PropertyBuffer(columnIndex, bytes, null);
        }

        static PropertyBuffer fixed(int columnIndex, int size, java.util.function.Consumer<ByteBuffer> writer) {
            return new PropertyBuffer(columnIndex, new byte[size], writer);
        }

        int size() {
            return Short.BYTES + bytes.length;
        }

        void writeTo(ByteBuffer output) {
            output.putShort((short) columnIndex);
            if (writer != null) {
                writer.accept(output);
            } else {
                output.put(bytes);
            }
        }
    }
}

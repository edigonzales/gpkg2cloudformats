package ch.so.agi.parquet;

import ch.so.agi.cloudformats.GeometryReader;
import ch.so.agi.cloudformats.TableDescriptor;
import ch.so.agi.cloudformats.TableWriter;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.schema.EdgeInterpolationAlgorithm;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.RecordConsumer;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types.PrimitiveBuilder;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.WKBWriter;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

public class ParquetTableWriter implements TableWriter<ParquetTableWriter.ParquetWriteOptions> {
    private final GeometryReader geometryReader;
    private final WKBWriter wkbWriter = new WKBWriter();

    public ParquetTableWriter(GeometryReader geometryReader) {
        this.geometryReader = geometryReader;
    }

    @Override
    public String fileExtension() {
        return "parquet";
    }

    @Override
    public ParquetWriteOptions defaultOptions() {
        return ParquetWriteOptions.builder().build();
    }

    @Override
    public void writeTable(Connection connection, TableDescriptor table, Path outputFile, ParquetWriteOptions options)
            throws SQLException, IOException {
        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + table.tableName());
             ResultSet resultSet = statement.executeQuery()) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            List<ParquetField> fields = buildFields(metaData, table, options);
            MessageType schema = buildSchema(table.tableName(), fields);
            ParquetTableWriteSupport writeSupport = new ParquetTableWriteSupport(schema, fields);
            try (ParquetWriter<ParquetRow> writer = new RowParquetWriterBuilder(new LocalOutputFile(outputFile), writeSupport)
                    .withRowGroupSize(options.rowGroupSize())
                    .withWriterVersion(ParquetProperties.WriterVersion.PARQUET_2_0)
                    .build()) {
                while (resultSet.next()) {
                    writer.write(readRow(resultSet, fields, table));
                }
            }
        }
    }

    private ParquetRow readRow(ResultSet resultSet, List<ParquetField> fields, TableDescriptor table) throws SQLException {
        Object[] values = new Object[fields.size()];
        for (int i = 0; i < fields.size(); i++) {
            ParquetField field = fields.get(i);
            if (field.geometry()) {
                Geometry geometry = geometryReader.readGeometry(resultSet, table.geometryColumn());
                values[i] = geometry == null ? null : wkbWriter.write(geometry);
            } else {
                values[i] = field.extractor().extract(resultSet);
            }
        }
        return new ParquetRow(values);
    }

    private List<ParquetField> buildFields(ResultSetMetaData metaData, TableDescriptor table, ParquetWriteOptions options)
            throws SQLException {
        List<ParquetField> fields = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String name = metaData.getColumnName(i);
            if (name.equalsIgnoreCase(table.geometryColumn())) {
                continue;
            }
            int sqlType = metaData.getColumnType(i);
            boolean required = metaData.isNullable(i) == ResultSetMetaData.columnNoNulls;
            fields.add(buildField(name, sqlType, required));
        }
        fields.add(buildGeometryField(table, options));
        return fields;
    }

    private static MessageType buildSchema(String tableName, List<ParquetField> fields) {
        List<org.apache.parquet.schema.Type> types = new ArrayList<>();
        for (ParquetField field : fields) {
            PrimitiveBuilder<PrimitiveType> builder = org.apache.parquet.schema.Types.primitive(field.primitiveType(), field.repetition());
            if (field.logicalType() != null) {
                builder = builder.as(field.logicalType());
            }
            types.add(builder.named(field.name()));
        }
        return new MessageType(tableName, types);
    }

    private static ParquetField buildField(String name, int sqlType, boolean required) {
        return switch (sqlType) {
            case Types.TINYINT, Types.SMALLINT, Types.INTEGER -> new ParquetField(name, required,
                    PrimitiveTypeName.INT32, null, false, rs -> {
                        Number value = (Number) rs.getObject(name);
                        return value == null ? null : value.intValue();
                    }, (consumer, value) -> consumer.addInteger((Integer) value));
            case Types.BIGINT -> new ParquetField(name, required,
                    PrimitiveTypeName.INT64, null, false, rs -> {
                        Number value = (Number) rs.getObject(name);
                        return value == null ? null : value.longValue();
                    }, (consumer, value) -> consumer.addLong((Long) value));
            case Types.FLOAT, Types.REAL -> new ParquetField(name, required,
                    PrimitiveTypeName.FLOAT, null, false, rs -> {
                        Number value = (Number) rs.getObject(name);
                        return value == null ? null : value.floatValue();
                    }, (consumer, value) -> consumer.addFloat((Float) value));
            case Types.DOUBLE, Types.NUMERIC, Types.DECIMAL -> new ParquetField(name, required,
                    PrimitiveTypeName.DOUBLE, null, false, rs -> {
                        Number value = (Number) rs.getObject(name);
                        return value == null ? null : value.doubleValue();
                    }, (consumer, value) -> consumer.addDouble((Double) value));
            case Types.BOOLEAN, Types.BIT -> new ParquetField(name, required,
                    PrimitiveTypeName.BOOLEAN, null, false, rs -> {
                        Object value = rs.getObject(name);
                        if (value == null) {
                            return null;
                        }
                        if (value instanceof Boolean bool) {
                            return bool;
                        }
                        if (value instanceof Number number) {
                            return number.intValue() != 0;
                        }
                        return Boolean.parseBoolean(value.toString());
                    }, (consumer, value) -> consumer.addBoolean((Boolean) value));
            case Types.DATE -> new ParquetField(name, required,
                    PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType(), false, rs -> {
                        Object raw = rs.getObject(name);
                        if (raw == null) {
                            return null;
                        }
                        LocalDate date = coerceDate(raw);
                        return date == null ? null : (int) date.toEpochDay();
                    }, (consumer, value) -> consumer.addInteger((Integer) value));
            case Types.TIME -> new ParquetField(name, required,
                    PrimitiveTypeName.INT32, LogicalTypeAnnotation.timeType(true, LogicalTypeAnnotation.TimeUnit.MILLIS), false, rs -> {
                        Object raw = rs.getObject(name);
                        if (raw == null) {
                            return null;
                        }
                        LocalTime localTime = coerceTime(raw);
                        if (localTime == null) {
                            return null;
                        }
                        return localTime.toSecondOfDay() * 1000 + localTime.getNano() / 1_000_000;
                    }, (consumer, value) -> consumer.addInteger((Integer) value));
            case Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> new ParquetField(name, required,
                    PrimitiveTypeName.INT64, LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS), false, rs -> {
                        Object raw = rs.getObject(name);
                        if (raw == null) {
                            return null;
                        }
                        Instant instant = coerceTimestamp(raw);
                        return instant == null ? null : instant.toEpochMilli();
                    }, (consumer, value) -> consumer.addLong((Long) value));
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> new ParquetField(name, required,
                    PrimitiveTypeName.BINARY, null, false, rs -> rs.getBytes(name),
                    (consumer, value) -> consumer.addBinary(Binary.fromConstantByteArray((byte[]) value)));
            default -> new ParquetField(name, required,
                    PrimitiveTypeName.BINARY, LogicalTypeAnnotation.stringType(), false, rs -> rs.getString(name),
                    (consumer, value) -> consumer.addBinary(Binary.fromString((String) value)));
        };
    }

    private static ParquetField buildGeometryField(TableDescriptor table, ParquetWriteOptions options) {
        LogicalTypeAnnotation logicalType = options.geometryLogicalType() == GeometryLogicalType.GEOGRAPHY
                ? LogicalTypeAnnotation.geographyType(options.geometryEncoding(), options.edgeInterpolationAlgorithm())
                : LogicalTypeAnnotation.geometryType(options.geometryEncoding());
        return new ParquetField(table.geometryColumn(), false,
                PrimitiveTypeName.BINARY, logicalType, true, rs -> null,
                (consumer, value) -> consumer.addBinary(Binary.fromConstantByteArray((byte[]) value)));
    }

    private static LocalDate coerceDate(Object raw) {
        if (raw instanceof java.sql.Date date) {
            return date.toLocalDate();
        }
        if (raw instanceof java.util.Date date) {
            return Instant.ofEpochMilli(date.getTime()).atZone(ZoneOffset.UTC).toLocalDate();
        }
        if (raw instanceof Number number) {
            return LocalDate.ofEpochDay(number.longValue());
        }
        String text = raw.toString();
        try {
            return LocalDate.parse(text, DateTimeFormatter.ISO_LOCAL_DATE);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static LocalTime coerceTime(Object raw) {
        if (raw instanceof java.sql.Time time) {
            return time.toLocalTime();
        }
        if (raw instanceof Number number) {
            return LocalTime.ofSecondOfDay(number.longValue());
        }
        String text = raw.toString();
        try {
            return LocalTime.parse(text, DateTimeFormatter.ISO_LOCAL_TIME);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static Instant coerceTimestamp(Object raw) {
        if (raw instanceof java.sql.Timestamp ts) {
            return ts.toInstant();
        }
        if (raw instanceof java.util.Date date) {
            return Instant.ofEpochMilli(date.getTime());
        }
        if (raw instanceof Number number) {
            return Instant.ofEpochMilli(number.longValue());
        }
        String text = raw.toString();
        try {
            return Instant.parse(text);
        } catch (DateTimeParseException ignored) {
            try {
                return OffsetDateTime.parse(text, DateTimeFormatter.ISO_OFFSET_DATE_TIME).toInstant();
            } catch (DateTimeParseException ignoredAgain) {
                try {
                    return LocalDateTime.parse(text, DateTimeFormatter.ISO_LOCAL_DATE_TIME).toInstant(ZoneOffset.UTC);
                } catch (DateTimeParseException ignoredThird) {
                    return null;
                }
            }
        }
    }

    public enum GeometryLogicalType {
        GEOMETRY,
        GEOGRAPHY
    }

    public record ParquetWriteOptions(long rowGroupSize,
                                      GeometryLogicalType geometryLogicalType,
                                      String geometryEncoding,
                                      EdgeInterpolationAlgorithm edgeInterpolationAlgorithm) {
        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private Long rowGroupSize;
            private GeometryLogicalType geometryLogicalType = GeometryLogicalType.GEOMETRY;
            private String geometryEncoding = "WKB";
            private EdgeInterpolationAlgorithm edgeInterpolationAlgorithm = LogicalTypeAnnotation.DEFAULT_ALGO;

            public Builder rowGroupSize(long rowGroupSize) {
                if (rowGroupSize <= 0) {
                    throw new IllegalArgumentException("rowGroupSize must be > 0");
                }
                this.rowGroupSize = rowGroupSize;
                return this;
            }

            public Builder geometryLogicalType(GeometryLogicalType geometryLogicalType) {
                this.geometryLogicalType = geometryLogicalType;
                return this;
            }

            public Builder geometryEncoding(String geometryEncoding) {
                this.geometryEncoding = geometryEncoding;
                return this;
            }

            public Builder edgeInterpolationAlgorithm(EdgeInterpolationAlgorithm edgeInterpolationAlgorithm) {
                this.edgeInterpolationAlgorithm = edgeInterpolationAlgorithm;
                return this;
            }

            public ParquetWriteOptions build() {
                long resolvedRowGroupSize = rowGroupSize == null
                        ? ParquetWriter.DEFAULT_BLOCK_SIZE
                        : rowGroupSize;
                return new ParquetWriteOptions(resolvedRowGroupSize, geometryLogicalType, geometryEncoding,
                        edgeInterpolationAlgorithm);
            }
        }
    }

    record ParquetRow(Object[] values) {
    }

    record ParquetField(String name,
                        boolean required,
                        PrimitiveTypeName primitiveType,
                        LogicalTypeAnnotation logicalType,
                        boolean geometry,
                        ValueExtractor extractor,
                        ValueWriter writer) {
        org.apache.parquet.schema.Type.Repetition repetition() {
            return required ? org.apache.parquet.schema.Type.Repetition.REQUIRED
                    : org.apache.parquet.schema.Type.Repetition.OPTIONAL;
        }
    }

    interface ValueExtractor {
        Object extract(ResultSet resultSet) throws SQLException;
    }

    interface ValueWriter {
        void write(RecordConsumer consumer, Object value);
    }

    static class ParquetTableWriteSupport extends WriteSupport<ParquetRow> {
        private final MessageType schema;
        private final List<ParquetField> fields;
        private RecordConsumer recordConsumer;

        ParquetTableWriteSupport(MessageType schema, List<ParquetField> fields) {
            this.schema = schema;
            this.fields = fields;
        }

        @Override
        public WriteContext init(org.apache.hadoop.conf.Configuration configuration) {
            return new WriteContext(schema, java.util.Map.of());
        }

        @Override
        public void prepareForWrite(RecordConsumer recordConsumer) {
            this.recordConsumer = recordConsumer;
        }

        @Override
        public void write(ParquetRow record) {
            recordConsumer.startMessage();
            Object[] values = record.values();
            for (int i = 0; i < fields.size(); i++) {
                Object value = values[i];
                if (value == null) {
                    continue;
                }
                ParquetField field = fields.get(i);
                recordConsumer.startField(field.name(), i);
                field.writer().write(recordConsumer, value);
                recordConsumer.endField(field.name(), i);
            }
            recordConsumer.endMessage();
        }
    }

    static class RowParquetWriterBuilder extends ParquetWriter.Builder<ParquetRow, RowParquetWriterBuilder> {
        private final ParquetTableWriteSupport writeSupport;

        RowParquetWriterBuilder(org.apache.parquet.io.OutputFile file, ParquetTableWriteSupport writeSupport) {
            super(file);
            this.writeSupport = writeSupport;
        }

        @Override
        protected RowParquetWriterBuilder self() {
            return this;
        }

        @Override
        protected WriteSupport<ParquetRow> getWriteSupport(org.apache.hadoop.conf.Configuration configuration) {
            return writeSupport;
        }
    }
}

package ch.so.agi.flatgeobuf;

import ch.so.agi.cloudformats.GeometryReader;
import ch.so.agi.cloudformats.TableDescriptor;
import ch.so.agi.cloudformats.TableWriter;
import com.google.flatbuffers.FlatBufferBuilder;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.wololo.flatgeobuf.ColumnMeta;
import org.wololo.flatgeobuf.Constants;
import org.wololo.flatgeobuf.GeometryConversions;
import org.wololo.flatgeobuf.HeaderMeta;
import org.wololo.flatgeobuf.NodeItem;
import org.wololo.flatgeobuf.PackedRTree;
import org.wololo.flatgeobuf.PackedRTree.FeatureItem;
import org.wololo.flatgeobuf.generated.ColumnType;
import org.wololo.flatgeobuf.generated.Feature;
import org.wololo.flatgeobuf.generated.GeometryType;

public class FlatGeobufTableWriter implements TableWriter<FlatGeobufTableWriter.FlatGeobufWriteOptions> {
    private static final int DEFAULT_NODE_SIZE = 16;

    private final GeometryReader geometryReader;

    public FlatGeobufTableWriter(GeometryReader geometryReader) {
        this.geometryReader = geometryReader;
    }

    @Override
    public String fileExtension() {
        return "fgb";
    }

    @Override
    public FlatGeobufWriteOptions defaultOptions() {
        return FlatGeobufWriteOptions.builder().build();
    }

    @Override
    public void writeTable(Connection connection, TableDescriptor table, Path outputFile, FlatGeobufWriteOptions options)
            throws SQLException, IOException {
        try (OutputStream out = Files.newOutputStream(outputFile)) {
            writeTable(connection, table, out, options.indexNodeSize());
        }
    }

    public void writeTable(Connection connection, TableDescriptor table, OutputStream outputStream)
            throws SQLException, IOException {
        writeTable(connection, table, outputStream, DEFAULT_NODE_SIZE);
    }

    public void writeTable(Connection connection, TableDescriptor table, OutputStream outputStream, int indexNodeSize)
            throws SQLException, IOException {
        List<ColumnSpec> columnSpecs = new ArrayList<>();
        List<FeatureItem> items = new ArrayList<>();
        List<FeatureOffset> featureOffsets = new ArrayList<>();
        Envelope datasetEnvelope = new Envelope();
        File tempFile = File.createTempFile("flatgeobuf", ".tmp");
        tempFile.deleteOnExit();

        try (PreparedStatement statement = connection.prepareStatement("SELECT * FROM " + table.tableName());
             ResultSet resultSet = statement.executeQuery();
             FileOutputStream tmpOut = new FileOutputStream(tempFile)) {
            ResultSetMetaData metaData = resultSet.getMetaData();
            columnSpecs = buildColumns(metaData, table.geometryColumn());
            FlatGeobufPropertiesWriter propertiesWriter = new FlatGeobufPropertiesWriter(columnSpecs);
            int rowIndex = 0;
            while (resultSet.next()) {
                Geometry geometry = geometryReader.readGeometry(resultSet, table.geometryColumn());
                if (geometry == null) {
                    continue;
                }
                Geometry normalized = normalizeGeometry(geometry, table.geometryType());
                Envelope envelope = normalized.getEnvelopeInternal();
                if (rowIndex == 0) {
                    datasetEnvelope.init(envelope);
                } else {
                    datasetEnvelope.expandToInclude(envelope);
                }
                byte[] featureBytes = encodeFeature(normalized, table.geometryType(), propertiesWriter, resultSet);
                FeatureOffset offset = new FeatureOffset(rowIndex == 0 ? 0 : featureOffsets.get(rowIndex - 1).offset + featureOffsets.get(rowIndex - 1).size,
                        featureBytes.length);
                featureOffsets.add(offset);
                tmpOut.write(featureBytes);

                FeatureItem item = new FeatureItem();
                item.nodeItem = new NodeItem(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY());
                item.offset = rowIndex;
                item.size = featureBytes.length;
                items.add(item);
                rowIndex++;
            }
        }

        writeFlatGeobuf(table, indexNodeSize, columnSpecs, items, featureOffsets, datasetEnvelope, tempFile, outputStream);
    }

    public record FlatGeobufWriteOptions(int indexNodeSize) {
        public static Builder builder() {
            return new Builder();
        }

        public static final class Builder {
            private Integer indexNodeSize;

            public Builder indexNodeSize(int indexNodeSize) {
                if (indexNodeSize < 0) {
                    throw new IllegalArgumentException("indexNodeSize must be >= 0");
                }
                this.indexNodeSize = indexNodeSize;
                return this;
            }

            public FlatGeobufWriteOptions build() {
                int resolved = indexNodeSize == null ? DEFAULT_NODE_SIZE : indexNodeSize;
                return new FlatGeobufWriteOptions(resolved);
            }
        }
    }

    private static List<ColumnSpec> buildColumns(ResultSetMetaData metaData, String geometryColumn) throws SQLException {
        List<ColumnSpec> columns = new ArrayList<>();
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String name = metaData.getColumnName(i);
            if (name.equalsIgnoreCase(geometryColumn)) {
                continue;
            }
            int sqlType = metaData.getColumnType(i);
            int columnType = mapColumnType(sqlType);
            ColumnMeta columnMeta = new ColumnMeta();
            columnMeta.name = name;
            columnMeta.type = (byte) columnType;
            columnMeta.nullable = metaData.isNullable(i) != ResultSetMetaData.columnNoNulls;
            columnMeta.width = metaData.getColumnDisplaySize(i);
            columnMeta.precision = metaData.getPrecision(i);
            columnMeta.scale = metaData.getScale(i);
            columns.add(new ColumnSpec(name, sqlType, columnType, columnMeta));
        }
        return columns;
    }

    private static int mapColumnType(int sqlType) {
        return switch (sqlType) {
            case Types.TINYINT -> ColumnType.Byte;
            case Types.SMALLINT -> ColumnType.Short;
            case Types.INTEGER -> ColumnType.Int;
            case Types.BIGINT -> ColumnType.Long;
            case Types.FLOAT, Types.REAL -> ColumnType.Float;
            case Types.DOUBLE, Types.NUMERIC, Types.DECIMAL -> ColumnType.Double;
            case Types.BOOLEAN, Types.BIT -> ColumnType.Bool;
            case Types.DATE, Types.TIME, Types.TIMESTAMP, Types.TIMESTAMP_WITH_TIMEZONE -> ColumnType.DateTime;
            case Types.BINARY, Types.VARBINARY, Types.LONGVARBINARY, Types.BLOB -> ColumnType.Binary;
            default -> ColumnType.String;
        };
    }

    private static Geometry normalizeGeometry(Geometry geometry, byte geometryType) throws SQLException {
        return switch (geometryType) {
            case GeometryType.Point -> ensureType(geometry, Point.class);
            case GeometryType.LineString -> ensureType(geometry, LineString.class);
            case GeometryType.Polygon -> ensureType(geometry, Polygon.class);
            case GeometryType.MultiPoint -> promoteToMulti(geometry, MultiPoint.class);
            case GeometryType.MultiLineString -> promoteToMulti(geometry, MultiLineString.class);
            case GeometryType.MultiPolygon -> promoteToMulti(geometry, MultiPolygon.class);
            default -> geometry;
        };
    }

    private static Geometry ensureType(Geometry geometry, Class<? extends Geometry> expected) throws SQLException {
        if (expected.isInstance(geometry)) {
            return geometry;
        }
        throw new SQLException("Unexpected geometry type: " + geometry.getGeometryType());
    }

    private static Geometry promoteToMulti(Geometry geometry, Class<? extends Geometry> expected) throws SQLException {
        if (expected.isInstance(geometry)) {
            return geometry;
        }
        GeometryFactory factory = geometry.getFactory();
        if (expected.equals(MultiPoint.class) && geometry instanceof Point point) {
            return factory.createMultiPoint(new Point[] { point });
        }
        if (expected.equals(MultiLineString.class) && geometry instanceof LineString lineString) {
            return factory.createMultiLineString(new LineString[] { lineString });
        }
        if (expected.equals(MultiPolygon.class) && geometry instanceof Polygon polygon) {
            return factory.createMultiPolygon(new Polygon[] { polygon });
        }
        throw new SQLException("Unexpected geometry type: " + geometry.getGeometryType());
    }

    private static byte[] encodeFeature(Geometry geometry, byte geometryType, FlatGeobufPropertiesWriter propertiesWriter,
                                        ResultSet resultSet) throws SQLException {
        FlatBufferBuilder builder = new FlatBufferBuilder();
        int geometryOffset;
        try {
            geometryOffset = GeometryConversions.serialize(builder, geometry, geometryType);
        } catch (IOException e) {
            throw new SQLException("Unable to serialize geometry.", e);
        }
        byte[] properties = propertiesWriter.write(resultSet);
        int propertiesOffset = properties.length == 0 ? 0 : Feature.createPropertiesVector(builder, properties);
        Feature.startFeature(builder);
        Feature.addGeometry(builder, geometryOffset);
        if (properties.length > 0) {
            Feature.addProperties(builder, propertiesOffset);
        }
        int featureOffset = Feature.endFeature(builder);
        Feature.finishSizePrefixedFeatureBuffer(builder, featureOffset);
        return builder.sizedByteArray();
    }

    private static void writeFlatGeobuf(TableDescriptor table,
                                        int indexNodeSize,
                                        List<ColumnSpec> columnSpecs,
                                        List<FeatureItem> items,
                                        List<FeatureOffset> featureOffsets,
                                        Envelope datasetEnvelope,
                                        File tempFile,
                                        OutputStream outputStream) throws IOException {
        try (BufferedOutputStream bufferedOut = new BufferedOutputStream(outputStream)) {
            bufferedOut.write(Constants.MAGIC_BYTES);

            HeaderMeta header = new HeaderMeta();
            header.name = table.tableName();
            header.geometryType = table.geometryType();
            header.srid = table.srid();
            header.envelope = items.isEmpty() ? null : datasetEnvelope;
            header.featuresCount = items.size();
            header.indexNodeSize = items.isEmpty() ? 0 : indexNodeSize;
            header.columns = columnSpecs.stream().map(ColumnSpec::columnMeta).toList();
            FlatBufferBuilder builder = new FlatBufferBuilder();
            HeaderMeta.write(header, bufferedOut, builder);

            List<FeatureItem> sortedItems = items;
            if (header.indexNodeSize > 0) {
                NodeItem extent = PackedRTree.calcExtent(sortedItems);
                PackedRTree.hilbertSort(sortedItems, extent);
                long offset = 0;
                for (FeatureItem item : sortedItems) {
                    FeatureOffset featureOffset = featureOffsets.get((int) item.offset);
                    item.nodeItem.offset = offset;
                    offset += featureOffset.size;
                }
                PackedRTree tree = new PackedRTree(sortedItems, (short) header.indexNodeSize);
                tree.write(bufferedOut);
            }

            bufferedOut.flush();
            try (var randomAccessFile = new java.io.RandomAccessFile(tempFile, "r")) {
                for (FeatureItem item : sortedItems) {
                    FeatureOffset featureOffset = featureOffsets.get((int) item.offset);
                    byte[] buffer = new byte[featureOffset.size];
                    randomAccessFile.seek(featureOffset.offset);
                    randomAccessFile.readFully(buffer);
                    bufferedOut.write(buffer);
                }
            }
        } finally {
            tempFile.delete();
        }
    }

    record ColumnSpec(String name, int sqlType, int columnType, ColumnMeta columnMeta) {
    }

    record FeatureOffset(long offset, int size) {
    }
}

package ch.so.agi.cloudformats;

import java.util.Objects;
public final class TableDescriptor {
    private final String tableName;
    private final String geometryColumn;
    private final int srid;
    private final byte geometryType;

    public TableDescriptor(String tableName, String geometryColumn, int srid, byte geometryType) {
        this.tableName = Objects.requireNonNull(tableName, "tableName");
        this.geometryColumn = geometryColumn;
        this.srid = srid;
        this.geometryType = geometryType;
    }

    public String tableName() {
        return tableName;
    }

    public String geometryColumn() {
        return geometryColumn;
    }

    public boolean hasGeometry() {
        return geometryColumn != null && !geometryColumn.isBlank();
    }

    public int srid() {
        return srid;
    }

    public byte geometryType() {
        return geometryType;
    }

    public static TableDescriptor of(String tableName, String geometryColumn, int srid, int geometryType) {
        return new TableDescriptor(tableName, geometryColumn, srid, (byte) geometryType);
    }
}

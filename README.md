# flatgeobuf-java

Bibliothek zum Exportieren von Geodaten aus JDBC-Tabellen in FlatGeobuf-Dateien (inklusive Spatial Index) mit Unterstützung für GeoPackage-Geometry-Blobs.

## Umsetzung

- **Separation of Concern**: Die Export-Logik arbeitet mit generischen Tabellen-Deskriptoren (`TableDescriptorProvider`) und einem austauschbaren `GeometryReader`. Dadurch ist der Export nicht an GeoPackage gebunden.【F:src/main/java/org/example/flatgeobuf/TableDescriptorProvider.java†L1-L9】【F:src/main/java/org/example/flatgeobuf/GeometryReader.java†L1-L9】
- **GeoPackage**: `GeoPackageGeometryReader` extrahiert WKB aus GPKG-Geometry-Blobs (Magic-Bytes, Flags, Envelope) und konvertiert sie nach JTS, weil GPKG-WKB nicht direkt in FlatGeobuf geschrieben werden kann.【F:src/main/java/org/example/flatgeobuf/GeoPackageGeometryReader.java†L1-L60】
- **ili2db-Tabellen**: `Ili2dbTableDescriptorProvider` nutzt die vorgegebene SQL-Abfrage und liefert Tabellenname, Geometriespalte, SRID und GeometryType.【F:src/main/java/org/example/flatgeobuf/Ili2dbTableDescriptorProvider.java†L1-L53】
- **FlatGeobuf**: `FlatGeobufTableWriter` erstellt Header/Features und schreibt einen Hilbert-sortierten `PackedRTree` Index für effiziente Streaming- und Range-Requests.【F:src/main/java/org/example/flatgeobuf/FlatGeobufTableWriter.java†L37-L238】

## Verwendung

### Export aus GeoPackage (ili2db-Layout)

```java
try (Connection connection = DriverManager.getConnection("jdbc:sqlite:your.gpkg")) {
    FlatGeobufExporter exporter = new FlatGeobufExporter(new GeoPackageGeometryReader());
    exporter.exportTables(connection, new Ili2dbTableDescriptorProvider(), Path.of("output"));
}
```

- Erzeugt pro Tabelle eine `<tablename>.fgb` Datei.
- Tabellen werden per SQL-Abfrage aus `T_ILI2DB_TABLE_PROP` selektiert.
- Spatial Index ist standardmäßig aktiv (Node Size 16).【F:src/main/java/org/example/flatgeobuf/FlatGeobufExporter.java†L11-L27】【F:src/main/java/org/example/flatgeobuf/FlatGeobufTableWriter.java†L37-L238】

### Export aus beliebigen JDBC-Tabellen (WKB in BLOB)

```java
FlatGeobufTableWriter writer = new FlatGeobufTableWriter(new WkbGeometryReader());
TableDescriptor table = new TableDescriptor("my_table", "geom", 4326, (byte) GeometryType.MultiPolygon);
try (OutputStream out = Files.newOutputStream(Path.of("my_table.fgb"))) {
    writer.writeTable(connection, table, out);
}
```

### Hinweise für Streaming/HTTP Range Requests

- Die erzeugten FlatGeobuf-Dateien enthalten einen Spatial Index (`PackedRTree`).
- Der Index erlaubt effiziente Abfragen per Byte-Range (z.B. HTTP Range Requests).【F:src/test/java/org/example/flatgeobuf/FlatGeobufStreamingTest.java†L1-L89】

# gpkg2cloudformats

Bibliothek zum Exportieren von Geodaten aus JDBC-Tabellen in FlatGeobuf- **und** Parquet-Dateien (inklusive Spatial Index für FlatGeobuf) mit Unterstützung für GeoPackage-Geometry-Blobs. Das Projekt ist in zwei Artefakte aufgeteilt:

- `library`: wiederverwendbare Export- und Writer-Logik
- `cli`: ausführbares CLI-Tool (`gpkg2cloudformat.jar`)

## CLI-Tool

Das CLI-Tool erzeugt pro Tabelle eine Datei im gewünschten Format.

```bash
java -jar gpkg2cloudformat.jar --input /path/to/data.gpkg --output /path/to/out --format flatgeobuf
```

Optionen:

- `--input`: Geopackage-Datei
- `--output`: Verzeichnis in das die resultierenden Dateien geschrieben werden (muss existieren)
- `--tables`: optional. Semikolon-separierte Liste von Tabellennamen, mit doppelten Anfuehrungszeichen (z. B. `"abbaustelle";"surfacestructure"`)
- `--format`: `flatgeobuf` oder `parquet`

## CI/CD (GitHub Actions)

Workflow: `.github/workflows/build-test-publish.yml`

- Baut und testet zuerst die `library`, danach sequentiell das `cli`.
- Bei Fehlern werden Gradle-Reports als Artefakte hochgeladen.
- Veröffentlicht die `library` nach `https://jars.sogeo.services/snapshots` (nur auf `main`, nicht bei Pull Requests).
- Erstellt beim manuellen Start (`workflow_dispatch`) einen GitHub Draft-Release für das CLI.

Benötigte GitHub Secrets für Maven-Publishing:

- `MAVEN_USERNAME`
- `MAVEN_PASSWORD`

## Umsetzung

- **Separation of Concern**: Die Export-Logik arbeitet mit generischen Tabellen-Deskriptoren (`TableDescriptorProvider`) und einem austauschbaren `GeometryReader` im neutralen Paket `ch.so.agi.cloudformats`. Dadurch ist der Export nicht an GeoPackage gebunden.
- **GeoPackage**: `GeoPackageGeometryReader` extrahiert WKB aus GPKG-Geometry-Blobs (Magic-Bytes, Flags, Envelope) und konvertiert sie nach JTS.
- **GeoPackage-Tabellen**: `GeoPackageTableDescriptorProvider` liest Tabelleninformationen direkt aus `gpkg_contents`/`gpkg_geometry_columns` und erlaubt optional eine Filterung auf konkrete Tabellen.
- **ili2db-Tabellen**: `Ili2dbTableDescriptorProvider` nutzt die vorgegebene SQL-Abfrage und liefert Tabellenname, Geometriespalte, SRID und GeometryType.
- **FlatGeobuf**: `FlatGeobufTableWriter` erstellt Header/Features und schreibt einen Hilbert-sortierten `PackedRTree` Index für effiziente Streaming- und Range-Requests.
- **Parquet**: `ParquetTableWriter` schreibt Parquet-Dateien mit Geometry/Geography Logical Types (ab Parquet 1.17.0) und unterstützt konfigurierbare Row Group Sizes.

## Verwendung (Library)

### Export aus GeoPackage (ili2db-Layout) nach FlatGeobuf

```java
try (Connection connection = DriverManager.getConnection("jdbc:sqlite:your.gpkg")) {
    FlatGeobufExporter exporter = new FlatGeobufExporter(new GeoPackageGeometryReader());
    exporter.exportTables(connection, new Ili2dbTableDescriptorProvider(), Path.of("output"));
}
```

- Erzeugt pro Tabelle eine `<tablename>.fgb` Datei.
- Tabellen werden per SQL-Abfrage aus `T_ILI2DB_TABLE_PROP` selektiert.
- Spatial Index ist standardmäßig aktiv (Node Size 16).

### Export aus beliebigen JDBC-Tabellen (WKB in BLOB) nach FlatGeobuf

```java
FlatGeobufTableWriter writer = new FlatGeobufTableWriter(new WkbGeometryReader());
TableDescriptor table = new TableDescriptor("my_table", "geom", 4326, (byte) GeometryType.MultiPolygon);
Path target = Path.of("my_table.fgb");
writer.writeTable(connection, table, target, writer.defaultOptions());
```

### Export nach Parquet

```java
try (Connection connection = DriverManager.getConnection("jdbc:sqlite:your.db")) {
    ParquetExporter exporter = new ParquetExporter(new WkbGeometryReader());
    exporter.exportTables(connection, tableDescriptorProvider, Path.of("output"));
}
```

- Erzeugt pro Tabelle eine `<tablename>.parquet` Datei.
- Geometry/Geography Logical Types werden im Parquet-Schema gesetzt (WKB-Encoding).
- Die Row Group Size kann über `ParquetWriteOptions.builder().rowGroupSize(...)` konfiguriert werden.

### Export aus beliebigen JDBC-Tabellen nach Parquet (direkter Writer)

```java
ParquetTableWriter writer = new ParquetTableWriter(new WkbGeometryReader());
TableDescriptor table = new TableDescriptor("my_table", "geom", 4326, (byte) GeometryType.MultiPolygon);
Path target = Path.of("my_table.parquet");
ParquetTableWriter.ParquetWriteOptions options = ParquetTableWriter.ParquetWriteOptions.builder()
        .rowGroupSize(128 * 1024 * 1024)
        .build();
writer.writeTable(connection, table, target, options);
```

### Hinweise fuer Streaming/HTTP Range Requests

- Die erzeugten FlatGeobuf-Dateien enthalten einen Spatial Index (`PackedRTree`).
- Der Index erlaubt effiziente Abfragen per Byte-Range (z.B. HTTP Range Requests).

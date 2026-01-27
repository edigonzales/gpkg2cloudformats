package ch.so.agi.flatgeobuf;

import ch.so.agi.cloudformats.GeoPackageGeometryReader;
import ch.so.agi.cloudformats.Ili2dbTableDescriptorProvider;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.FileInputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import org.junit.jupiter.api.Test;
import org.wololo.flatgeobuf.HeaderMeta;
import org.wololo.flatgeobuf.generated.GeometryType;

class GeoPackageExportIntegrationTest {
    @Test
    void exportsIli2dbTablesToFlatGeobuf() throws Exception {
        Path outputDir = Files.createTempDirectory("flatgeobuf-out");
        Path geopackage = Path.of(getClass().getResource("/data/ch.so.afu.abbaustellen.gpkg").toURI());
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite:" + geopackage)) {
            FlatGeobufExporter exporter = new FlatGeobufExporter(new GeoPackageGeometryReader());
            exporter.exportTables(connection, new Ili2dbTableDescriptorProvider(), outputDir);
        }

        Path output = outputDir.resolve("abbaustelle.fgb");
        assertThat(output).exists();
        HeaderMeta header;
        try (FileInputStream in = new FileInputStream(output.toFile())) {
            header = HeaderMeta.read(in);
        }
        assertThat(header.geometryType).isEqualTo((byte) GeometryType.MultiPolygon);
        assertThat(header.srid).isEqualTo(2056);
        assertThat(header.featuresCount).isPositive();
    }
}

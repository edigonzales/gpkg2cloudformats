package ch.so.agi.flatgeobuf;

import ch.so.agi.cloudformats.TableDescriptor;
import ch.so.agi.cloudformats.WkbGeometryReader;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.io.WKBWriter;
import org.wololo.flatgeobuf.HeaderMeta;
import org.wololo.flatgeobuf.generated.ColumnType;
import org.wololo.flatgeobuf.generated.GeometryType;

class FlatGeobufDateTimeTest {
    @Test
    void writesDateAndTimeAttributes() throws Exception {
        Instant instant = Instant.parse("2024-05-12T09:15:30Z");
        LocalDate date = LocalDate.of(2024, 5, 12);
        LocalTime time = LocalTime.of(9, 15, 30);
        try (Connection connection = DriverManager.getConnection("jdbc:sqlite::memory:");
             PreparedStatement create = connection.prepareStatement(
                     "CREATE TABLE events (id INTEGER, created_date DATE, created_time TIME, created_ts TIMESTAMP, geom BLOB)")) {
            create.execute();
            try (PreparedStatement insert = connection.prepareStatement(
                    "INSERT INTO events (id, created_date, created_time, created_ts, geom) VALUES (?, ?, ?, ?, ?)")) {
                insert.setInt(1, 1);
                insert.setDate(2, java.sql.Date.valueOf(date));
                insert.setTime(3, java.sql.Time.valueOf(time));
                insert.setTimestamp(4, Timestamp.from(instant));
                Point point = new GeometryFactory().createPoint(new org.locationtech.jts.geom.Coordinate(7, 47));
                insert.setBytes(5, new WKBWriter().write(point));
                insert.executeUpdate();
            }

            FlatGeobufTableWriter writer = new FlatGeobufTableWriter(new WkbGeometryReader());
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            writer.writeTable(connection, new TableDescriptor("events", "geom", 4326, (byte) GeometryType.Point), out);

            HeaderMeta header = FlatGeobufTestSupport.readHeader(out.toByteArray());
            var dateColumn = header.columns.stream()
                    .filter(column -> column.name.equals("created_date"))
                    .findFirst()
                    .orElseThrow();
            assertThat(dateColumn.type).isEqualTo((byte) ColumnType.String);

            List<Map<String, Object>> props = FlatGeobufTestSupport.readProperties(out.toByteArray());
            assertThat(props).hasSize(1);
            Map<String, Object> first = props.get(0);
            assertThat(first).containsKeys("created_date", "created_time", "created_ts");
            assertDateValue(first.get("created_date"));
            assertTimeValue(first.get("created_time"));
            assertTimestampValue(first.get("created_ts"));
        }
    }

    private static void assertDateValue(Object value) {
        if (value instanceof Number number) {
            assertThat(number.longValue()).isPositive();
            return;
        }
        assertThat(value.toString()).isNotBlank();
    }

    private static void assertTimeValue(Object value) {
        if (value instanceof Number number) {
            assertThat(number.longValue()).isPositive();
            return;
        }
        assertThat(value.toString()).isNotBlank();
    }

    private static void assertTimestampValue(Object value) {
        if (value instanceof Number number) {
            assertThat(number.longValue()).isPositive();
            return;
        }
        String text = value.toString();
        assertThat(text).isNotBlank();
    }
}

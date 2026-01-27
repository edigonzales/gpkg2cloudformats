package ch.so.agi.parquet;

import java.lang.reflect.Method;
import java.sql.Types;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ParquetAttributeTypesTest {
    @Test
    void mapsDateTimeAndTimestampSqlTypes() throws Exception {
        Object dateField = invokeBuildField("event_date", Types.DATE, true);
        Object timeField = invokeBuildField("event_time", Types.TIME, true);
        Object timestampField = invokeBuildField("event_ts", Types.TIMESTAMP, true);

        LogicalTypeAnnotation dateType = (LogicalTypeAnnotation) dateField.getClass().getMethod("logicalType").invoke(dateField);
        LogicalTypeAnnotation timeType = (LogicalTypeAnnotation) timeField.getClass().getMethod("logicalType").invoke(timeField);
        LogicalTypeAnnotation timestampType = (LogicalTypeAnnotation) timestampField.getClass().getMethod("logicalType").invoke(timestampField);

        assertThat(dateType).isInstanceOf(LogicalTypeAnnotation.DateLogicalTypeAnnotation.class);
        assertThat(timeType).isInstanceOf(LogicalTypeAnnotation.TimeLogicalTypeAnnotation.class);
        assertThat(timestampType).isInstanceOf(LogicalTypeAnnotation.TimestampLogicalTypeAnnotation.class);

        LogicalTypeAnnotation.TimeLogicalTypeAnnotation timeAnnotation = (LogicalTypeAnnotation.TimeLogicalTypeAnnotation) timeType;
        assertThat(timeAnnotation.getUnit()).isEqualTo(LogicalTypeAnnotation.TimeUnit.MILLIS);
        assertThat(timeAnnotation.isAdjustedToUTC()).isTrue();

        LogicalTypeAnnotation.TimestampLogicalTypeAnnotation timestampAnnotation =
                (LogicalTypeAnnotation.TimestampLogicalTypeAnnotation) timestampType;
        assertThat(timestampAnnotation.getUnit()).isEqualTo(LogicalTypeAnnotation.TimeUnit.MILLIS);
        assertThat(timestampAnnotation.isAdjustedToUTC()).isTrue();
    }

    private static Object invokeBuildField(String name, int sqlType, boolean required) throws Exception {
        Method method = ParquetTableWriter.class.getDeclaredMethod("buildField", String.class, int.class, boolean.class);
        method.setAccessible(true);
        return method.invoke(null, name, sqlType, required);
    }
}

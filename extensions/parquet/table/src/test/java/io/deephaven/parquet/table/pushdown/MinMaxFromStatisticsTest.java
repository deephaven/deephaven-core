//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.pushdown;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.parquet.base.NullStatistics;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.location.ParquetTableLocationKey;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.BinaryStatistics;
import org.apache.parquet.column.statistics.BooleanStatistics;
import org.apache.parquet.column.statistics.DoubleStatistics;
import org.apache.parquet.column.statistics.FloatStatistics;
import org.apache.parquet.column.statistics.IntStatistics;
import org.apache.parquet.column.statistics.LongStatistics;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Optional;

import static io.deephaven.engine.util.TableTools.booleanCol;
import static io.deephaven.engine.util.TableTools.byteCol;
import static io.deephaven.engine.util.TableTools.charCol;
import static io.deephaven.engine.util.TableTools.doubleCol;
import static io.deephaven.engine.util.TableTools.floatCol;
import static io.deephaven.engine.util.TableTools.instantCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static io.deephaven.engine.util.TableTools.shortCol;
import static io.deephaven.engine.util.TableTools.stringCol;
import static io.deephaven.parquet.table.ParquetTools.writeTable;
import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_SHORT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

@Category(OutOfBandTest.class)
public class MinMaxFromStatisticsTest {

    private static final String ROOT_FILENAME = MinMaxFromStatisticsTest.class.getName() + "_root";

    private static File rootFile;

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Before
    public void setUp() {
        rootFile = new File(ROOT_FILENAME);
        if (rootFile.exists()) {
            FileUtils.deleteRecursively(rootFile);
        }
        // noinspection ResultOfMethodCallIgnored
        rootFile.mkdirs();
    }

    @After
    public void tearDown() {
        FileUtils.deleteRecursively(rootFile);
    }

    @Test
    public void nullStatisticsTest() {
        {
            final Statistics<?> statistics = null;
            final Optional<MinMax<?>> minMax = MinMaxFromStatistics.get(statistics, Boolean.class);
            assertEquals(Optional.empty(), minMax);
        }
        {
            final Statistics<?> statistics = NullStatistics.INSTANCE;
            final Optional<MinMax<?>> minMax = MinMaxFromStatistics.get(statistics, Boolean.class);
            assertEquals(Optional.empty(), minMax);
        }
    }

    @Test
    public void booleanColumnTest() {
        final File dest = new File(rootFile, "booleanColumn.parquet");
        final String columnName = "Booleans";
        {
            writeTable(newTable(booleanCol(columnName, null, false, true, false, null, true)), dest.toString());
            // Read as Boolean
            assertMinMax(dest, columnName, BooleanStatistics.class, 2, false, true, Boolean.class);
            // Read as Short
            assertMinMax(dest, columnName, BooleanStatistics.class, 2, (short) 0, (short) 1, Short.class);
            // Read as Integer
            assertMinMax(dest, columnName, BooleanStatistics.class, 2, 0, 1, Integer.class);
            // Read as Long
            assertMinMax(dest, columnName, BooleanStatistics.class, 2, 0L, 1L, Long.class);
            // Read as float
            assertEmptyMinMax(dest, columnName, BooleanStatistics.class, 2, Float.class);
        }
        {
            // all nulls
            writeTable(newTable(booleanCol(columnName, null, null, null)), dest.toString());
            assertEmptyMinMax(dest, columnName, BooleanStatistics.class, 3, Boolean.class);
        }
    }

    @Test
    public void charColumnTest() {
        final File dest = new File(rootFile, "charColumn.parquet");
        final String columnName = "Characters";
        {
            writeTable(newTable(charCol(columnName, NULL_CHAR, 'A', 'z', 'M', NULL_CHAR)), dest.toString());
            // Read as Character
            assertMinMax(dest, columnName, IntStatistics.class, 2, 'A', 'z', Character.class);
            // Read as Integer
            assertMinMax(dest, columnName, IntStatistics.class, 2, 65, 122, Integer.class);
            // Read as Long
            assertMinMax(dest, columnName, IntStatistics.class, 2, 65L, 122L, Long.class);
            // Read as Short (unsupported)
            assertEmptyMinMax(dest, columnName, IntStatistics.class, 2, Short.class);
            // Read as Float (unsupported)
            assertEmptyMinMax(dest, columnName, IntStatistics.class, 2, Float.class);
        }
        {
            // all nulls
            writeTable(newTable(charCol(columnName, NULL_CHAR, NULL_CHAR, NULL_CHAR)), dest.toString());
            assertEmptyMinMax(dest, columnName, IntStatistics.class, 3, Character.class);
        }
    }

    @Test
    public void byteColumnTest() {
        final File dest = new File(rootFile, "byteColumn.parquet");
        final String columnName = "Bytes";
        {
            writeTable(newTable(byteCol(columnName, NULL_BYTE, (byte) -10, (byte) 100, (byte) 0)), dest.toString());
            // Read as Byte
            assertMinMax(dest, columnName, IntStatistics.class, 1, (byte) -10, (byte) 100, Byte.class);
            // Read as byte
            assertMinMax(dest, columnName, IntStatistics.class, 1, (byte) -10, (byte) 100, byte.class);
            // Read as Short
            assertMinMax(dest, columnName, IntStatistics.class, 1, (short) -10, (short) 100, Short.class);
            // Read as Integer
            assertMinMax(dest, columnName, IntStatistics.class, 1, -10, 100, Integer.class);
            // Read as Long
            assertMinMax(dest, columnName, IntStatistics.class, 1, -10L, 100L, Long.class);
            // Read as Boolean (unsupported)
            assertEmptyMinMax(dest, columnName, IntStatistics.class, 1, Boolean.class);
        }
        {
            // all nulls
            writeTable(newTable(byteCol(columnName, NULL_BYTE, NULL_BYTE, NULL_BYTE)), dest.toString());
            assertEmptyMinMax(dest, columnName, IntStatistics.class, 3, Byte.class);
        }
    }

    @Test
    public void shortColumnTest() {
        final File dest = new File(rootFile, "shortColumn.parquet");
        final String columnName = "Shorts";
        {
            writeTable(newTable(shortCol(columnName, NULL_SHORT, (short) -1024, (short) 2048)), dest.toString());
            // Read as Short
            assertMinMax(dest, columnName, IntStatistics.class, 1, (short) -1024, (short) 2048, Short.class);
            // Read as short
            assertMinMax(dest, columnName, IntStatistics.class, 1, (short) -1024, (short) 2048, short.class);
            // Read as Integer
            assertMinMax(dest, columnName, IntStatistics.class, 1, -1024, 2048, Integer.class);
            // Read as int
            assertMinMax(dest, columnName, IntStatistics.class, 1, -1024, 2048, int.class);
            // Read as Long
            assertMinMax(dest, columnName, IntStatistics.class, 1, -1024L, 2048L, Long.class);
            // Read as long
            assertMinMax(dest, columnName, IntStatistics.class, 1, -1024L, 2048L, long.class);
            // Read as Byte (unsupported)
            assertEmptyMinMax(dest, columnName, IntStatistics.class, 1, Byte.class);
        }
        {
            // all nulls
            writeTable(newTable(shortCol(columnName, NULL_SHORT, NULL_SHORT)), dest.toString());
            assertEmptyMinMax(dest, columnName, IntStatistics.class, 2, Short.class);
        }
    }

    @Test
    public void intColumnTest() {
        final File dest = new File(rootFile, "intColumn.parquet");
        final String columnName = "Integers";
        {
            writeTable(
                    newTable(intCol(columnName, NULL_INT, 42, -1, 100, 200, NULL_INT, 300, 400, NULL_INT, 500)),
                    dest.toString());
            // Read as Integer
            assertMinMax(dest, columnName, IntStatistics.class, 3, -1, 500, Integer.class);
            // Read as int
            assertMinMax(dest, columnName, IntStatistics.class, 3, -1, 500, int.class);
            // Read as Long
            assertMinMax(dest, columnName, IntStatistics.class, 3, -1L, 500L, Long.class);
            // Read as long
            assertMinMax(dest, columnName, IntStatistics.class, 3, -1L, 500L, long.class);
            // Read as Short (unsupported)
            assertEmptyMinMax(dest, columnName, IntStatistics.class, 3, Short.class);
        }
        {
            // all nulls
            writeTable(newTable(intCol(columnName, NULL_INT, NULL_INT, NULL_INT)), dest.toString());
            assertEmptyMinMax(dest, columnName, IntStatistics.class, 3, Integer.class);
        }
    }


    @Test
    public void longColumnTest() {
        final File dest = new File(rootFile, "longColumn.parquet");
        final String columnName = "Longs";
        {
            writeTable(newTable(longCol(columnName, NULL_LONG,
                    -10_000_000_000L, 100L, 10_000_000_000L)), dest.toString());
            // Read as Long
            assertMinMax(dest, columnName, LongStatistics.class, 1,
                    -10_000_000_000L, 10_000_000_000L, Long.class);
            // Read as long
            assertMinMax(dest, columnName, LongStatistics.class, 1,
                    -10_000_000_000L, 10_000_000_000L, long.class);
            // Read as Integer (unsupported)
            assertEmptyMinMax(dest, columnName, LongStatistics.class, 1, Integer.class);
        }
        {
            // all nulls
            writeTable(newTable(longCol(columnName, NULL_LONG, NULL_LONG)), dest.toString());
            assertEmptyMinMax(dest, columnName, LongStatistics.class, 2, Long.class);
        }
    }

    @Test
    public void floatColumnTest() {
        final File dest = new File(rootFile, "floatColumn.parquet");
        final String columnName = "Floats";
        {
            writeTable(newTable(floatCol(columnName, NULL_FLOAT, -1.5f, 3f, 2.7f)), dest.toString());
            // Read as Float
            assertMinMax(dest, columnName, FloatStatistics.class, 1,
                    -1.5f, 3f, Float.class);
            // Read as float
            assertMinMax(dest, columnName, FloatStatistics.class, 1,
                    -1.5f, 3f, float.class);
            // Read as Double
            assertMinMax(dest, columnName, FloatStatistics.class, 1,
                    -1.5d, 3d, Double.class);
            // Read as double
            assertMinMax(dest, columnName, FloatStatistics.class, 1,
                    -1.5d, 3d, double.class);
            // Read as Long (unsupported)
            assertEmptyMinMax(dest, columnName, FloatStatistics.class, 1, Long.class);
        }
        {
            // all nulls
            writeTable(newTable(floatCol(columnName, NULL_FLOAT, NULL_FLOAT, NULL_FLOAT)), dest.toString());
            assertEmptyMinMax(dest, columnName, FloatStatistics.class, 3, Float.class);
        }
    }

    @Test
    public void doubleColumnTest() {
        final File dest = new File(rootFile, "doubleColumn.parquet");
        final String columnName = "Doubles";
        {
            writeTable(newTable(doubleCol(columnName, NULL_DOUBLE, -1.5, 2.718, 3.14)), dest.toString());
            // Read as Double
            assertMinMax(dest, columnName, DoubleStatistics.class, 1,
                    -1.5, 3.14, Double.class);
            // Read as double
            assertMinMax(dest, columnName, DoubleStatistics.class, 1,
                    -1.5, 3.14, double.class);
            // Read as Float (unsupported)
            assertEmptyMinMax(dest, columnName, DoubleStatistics.class, 1, Float.class);
        }
        {
            // all nulls
            writeTable(newTable(doubleCol(columnName, NULL_DOUBLE, NULL_DOUBLE)), dest.toString());
            assertEmptyMinMax(dest, columnName, DoubleStatistics.class, 2, Double.class);
        }
    }


    @Test
    public void stringColumnTest() {
        final File dest = new File(rootFile, "stringColumn.parquet");
        final String columnName = "Strings";
        {
            writeTable(newTable(stringCol(columnName, null, "aaa", "zzz", "mmm", null)), dest.toString());
            // Read as String
            assertMinMax(dest, columnName, BinaryStatistics.class, 2,
                    "aaa", "zzz", String.class);
            // Read as Object (unsupported)
            assertEmptyMinMax(dest, columnName, BinaryStatistics.class, 2, Object.class);
        }
        {
            // all nulls
            writeTable(newTable(stringCol(columnName, null, null, null)), dest.toString());
            assertEmptyMinMax(dest, columnName, BinaryStatistics.class, 3, String.class);
        }
    }

    @Test
    public void instantColumnTest() {
        final File dest = new File(rootFile, "instantColumn.parquet");
        final String columnName = "Instants";
        {
            final Instant i1 = Instant.ofEpochMilli(1_234L);
            final Instant i2 = Instant.ofEpochMilli(10_000L);
            final Instant i3 = Instant.ofEpochMilli(5_532L);
            writeTable(newTable(instantCol(columnName, null, i1, i2, i3)), dest.toString());
            // Read as Instant
            assertMinMax(dest, columnName, LongStatistics.class, 1, i1, i2, Instant.class);
            // Read as LocalDateTime (unsupported)
            assertEmptyMinMax(dest, columnName, LongStatistics.class, 1, LocalDateTime.class);
        }
        {
            // all nulls
            writeTable(newTable(instantCol(columnName, null, null, null)), dest.toString());
            assertEmptyMinMax(dest, columnName, LongStatistics.class, 3, Instant.class);
        }
    }


    @Test
    public void localDateColumnTest() {
        final File dest = new File(rootFile, "localDateColumn.parquet");
        final String columnName = "LocalDates";
        {
            final LocalDate d1 = LocalDate.of(2020, 1, 1);
            final LocalDate d2 = LocalDate.of(2020, 12, 31);
            writeTable(newTable(new ColumnHolder<>(columnName, LocalDate.class, null, false,
                    d1, null, d2)), dest.toString());
            // Read as LocalDate
            assertMinMax(dest, columnName, IntStatistics.class, 1, d1, d2, LocalDate.class);
            // Read as Instant (unsupported)
            assertEmptyMinMax(dest, columnName, IntStatistics.class, 1, Instant.class);
        }
        {
            // all nulls
            writeTable(newTable(new ColumnHolder<>(columnName, LocalDate.class, null, false,
                    null, null)), dest.toString());
            assertEmptyMinMax(dest, columnName, IntStatistics.class, 2, LocalDate.class);
        }
    }


    @Test
    public void localTimeColumnTest() {
        final File dest = new File(rootFile, "localTimeColumn.parquet");
        final String columnName = "LocalTimes";
        {
            final LocalTime t1 = LocalTime.of(1, 2, 3, 4);
            final LocalTime t2 = LocalTime.of(23, 59, 59);
            writeTable(newTable(new ColumnHolder<>(columnName, LocalTime.class, null, false,
                    t1, null, t2)), dest.toString());
            // Read as LocalTime
            assertMinMax(dest, columnName, LongStatistics.class, 1, t1, t2, LocalTime.class);
            // Read as Integer (unsupported)
            assertEmptyMinMax(dest, columnName, LongStatistics.class, 1, Integer.class);
        }
        {
            // all nulls
            writeTable(newTable(new ColumnHolder<>(columnName, LocalTime.class, null, false,
                    null, null)), dest.toString());
            assertEmptyMinMax(dest, columnName, LongStatistics.class, 2, LocalTime.class);
        }
    }

    @Test
    public void localDateTimeColumnTest() {
        final File dest = new File(rootFile, "localDateTimeColumn.parquet");
        final String columnName = "LocalDateTimes";
        {
            final LocalDateTime dt1 = LocalDateTime.of(2020, 1, 1, 1, 2);
            final LocalDateTime dt2 = LocalDateTime.of(2020, 12, 31, 23, 59, 59);
            writeTable(newTable(new ColumnHolder<>(columnName, LocalDateTime.class, null, false,
                    dt1, null, dt2)), dest.toString());
            // Read as LocalDateTime
            assertMinMax(dest, columnName, LongStatistics.class, 1, dt1, dt2, LocalDateTime.class);
            // Read as Instant (unsupported)
            assertEmptyMinMax(dest, columnName, LongStatistics.class, 1, Instant.class);
        }
        {
            // all nulls
            writeTable(newTable(new ColumnHolder<>(columnName, LocalDateTime.class, null, false,
                    null, null)), dest.toString());
            assertEmptyMinMax(dest, columnName, LongStatistics.class, 2, LocalDateTime.class);
        }
    }

    @Test
    public void decimalColumnTest() {
        final File dest = new File(rootFile, "decimalColumn.parquet");
        final String columnName = "Decimals";
        {
            final BigDecimal bd1 = BigDecimal.valueOf(123_423_367_532L);
            final BigDecimal bd2 = BigDecimal.valueOf(422_123_132_234L);
            writeTable(newTable(new ColumnHolder<>(columnName, BigDecimal.class, null, false,
                    bd1, null, bd2)), dest.toString());
            // Decimal statistics are not supported yet
            assertEmptyMinMax(dest, columnName, BinaryStatistics.class, 1, BigDecimal.class);
        }
        {
            writeTable(newTable(new ColumnHolder<>(columnName, BigDecimal.class, null, false,
                    null, null)), dest.toString());
            assertEmptyMinMax(dest, columnName, BinaryStatistics.class, 2, BigDecimal.class);
        }
    }

    /**
     * <pre>
     * import pyarrow as pa
     * import pyarrow.parquet as pq
     *
     * def time_array(values, unit):
     * return pa.array(values, type=pa.time32(unit) if unit == "ms" else pa.time64(unit))
     *
     * table = pa.Table.from_arrays(
     * [
     * # MILLIS
     * time_array([None, 1_000, 50_000], "ms"),
     * time_array([None, None, None], "ms"),
     *
     * # MICROS
     * time_array([None, 1_000_000, 5_000_000], "us"),
     * time_array([None, None, None], "us"),
     *
     * # NANOS
     * time_array([None, 100_000_000, 500_000_000], "ns"),
     * time_array([None, None, None], "ns"),
     * ],
     * names=[
     * "time_ms_mix", "time_ms_null",
     * "time_us_mix", "time_us_null",
     * "time_ns_mix", "time_ns_null",
     * ],
     * )
     *
     * pq.write_table(table, "ReferenceTimeColumn.parquet")
     * </pre>
     */
    @Test
    public void timeColumnTest() {
        final File dest = new File(
                MinMaxFromStatisticsTest.class.getResource("/ReferenceTimeColumn.parquet").getFile());

        // TIME millis
        assertMinMax(dest, "time_ms_mix", IntStatistics.class, 1,
                LocalTime.ofNanoOfDay(1_000 * DateTimeUtils.MILLI),
                LocalTime.ofNanoOfDay(50_000 * DateTimeUtils.MILLI),
                LocalTime.class);
        assertEmptyMinMax(dest, "time_ms_null", IntStatistics.class, 3, LocalTime.class);

        // TIME micros
        assertMinMax(dest, "time_us_mix", LongStatistics.class, 1,
                LocalTime.ofNanoOfDay(1_000_000 * DateTimeUtils.MICRO),
                LocalTime.ofNanoOfDay(5_000_000 * DateTimeUtils.MICRO),
                LocalTime.class);
        assertEmptyMinMax(dest, "time_us_null", LongStatistics.class, 3, LocalTime.class);

        // TIME nanos
        assertMinMax(dest, "time_ns_mix", LongStatistics.class, 1,
                LocalTime.ofNanoOfDay(100_000_000),
                LocalTime.ofNanoOfDay(500_000_000),
                LocalTime.class);
        assertEmptyMinMax(dest, "time_ns_null", LongStatistics.class, 3, LocalTime.class);
    }

    /**
     * <pre>
     * import pyarrow as pa
     * import pyarrow.parquet as pq
     *
     * def ts_array(values, unit):
     * return pa.array(values, type=pa.timestamp(unit))
     *
     * table = pa.Table.from_arrays(
     * [
     * # MILLIS
     * ts_array([None, 1_000, 50_000], "ms"),
     * ts_array([None, None, None], "ms"),
     *
     * # MICROS
     * ts_array([None, 1_000_000, 5_000_000], "us"),
     * ts_array([None, None, None], "us"),
     *
     * # NANOS
     * ts_array([None, 100_000_000, 500_000_000], "ns"),
     * ts_array([None, None, None], "ns"),
     * ],
     * names=[
     * "ts_ms_mix", "ts_ms_null",
     * "ts_us_mix", "ts_us_null",
     * "ts_ns_mix", "ts_ns_null",
     * ],
     * )
     *
     * pq.write_table(table, "ReferenceTimestampNonUTCColumn.parquet")
     * </pre>
     */
    @Test
    public void timestampNonUTCColumnTest() {
        final File dest = new File(
                MinMaxFromStatisticsTest.class.getResource("/ReferenceTimestampNonUTCColumn.parquet").getFile());

        // TIMESTAMP millis
        assertMinMax(dest, "ts_ms_mix", LongStatistics.class, 1,
                LocalDateTime.ofInstant(Instant.ofEpochMilli(1_000L), ZoneOffset.UTC),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(50_000L), ZoneOffset.UTC),
                LocalDateTime.class);
        assertEmptyMinMax(dest, "ts_ms_null", LongStatistics.class, 3, LocalDateTime.class);

        // TIMESTAMP micros
        assertMinMax(dest, "ts_us_mix", LongStatistics.class, 1,
                LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC),
                LocalDateTime.ofEpochSecond(5, 0, ZoneOffset.UTC),
                LocalDateTime.class);
        assertEmptyMinMax(dest, "ts_us_null", LongStatistics.class, 3, LocalDateTime.class);

        // TIMESTAMP nanos
        assertMinMax(dest, "ts_ns_mix", LongStatistics.class, 1,
                LocalDateTime.ofEpochSecond(0, 100_000_000, ZoneOffset.UTC),
                LocalDateTime.ofEpochSecond(0, 500_000_000, ZoneOffset.UTC),
                LocalDateTime.class);
        assertEmptyMinMax(dest, "ts_ns_null", LongStatistics.class, 3, LocalDateTime.class);
    }

    /**
     * <pre>
     * import pyarrow as pa
     * import pyarrow.parquet as pq
     *
     * def ts_array(values, unit):
     * return pa.array(values, type=pa.timestamp(unit, tz="UTC"))
     *
     * table = pa.Table.from_arrays(
     * [
     * # MILLIS
     * ts_array([None, 0, 50_000], "ms"),
     * ts_array([None, None, None], "ms"),
     *
     * # MICROS
     * ts_array([None, 1_000_000, 5_000_000], "us"),
     * ts_array([None, None, None], "us"),
     *
     * # NANOS
     * ts_array([None, 100_000_000, 500_000_000], "ns"),
     * ts_array([None, None, None], "ns"),
     * ],
     * names=[
     * "ts_ms_mix", "ts_ms_null",
     * "ts_us_mix", "ts_us_null",
     * "ts_ns_mix", "ts_ns_null",
     * ],
     * )
     *
     * pq.write_table(table, "ReferenceTimestampUTCColumn.parquet")
     * </pre>
     */
    @Test
    public void timestampUTCColumnTest() {
        final File dest = new File(
                MinMaxFromStatisticsTest.class.getResource("/ReferenceTimestampUTCColumn.parquet").getFile());

        // TIMESTAMP millis
        assertMinMax(dest, "ts_ms_mix", LongStatistics.class, 1,
                Instant.ofEpochMilli(0L),
                Instant.ofEpochMilli(50_000L),
                Instant.class);
        assertEmptyMinMax(dest, "ts_ms_null", LongStatistics.class, 3, Instant.class);

        // TIMESTAMP micros
        assertMinMax(dest, "ts_us_mix", LongStatistics.class, 1,
                Instant.ofEpochMilli(1_000L),
                Instant.ofEpochMilli(5_000L),
                Instant.class);
        assertEmptyMinMax(dest, "ts_us_null", LongStatistics.class, 3, Instant.class);

        // TIMESTAMP nanos
        assertMinMax(dest, "ts_ns_mix", LongStatistics.class, 1,
                Instant.ofEpochMilli(100L),
                Instant.ofEpochMilli(500L),
                Instant.class);
        assertEmptyMinMax(dest, "ts_ns_null", LongStatistics.class, 3, Instant.class);
    }

    /**
     * Helper method to get the column statistics for the specified column name.
     */
    private static Statistics<?> getColumnStatistics(final File dest, final String columnName) {
        final ParquetMetadata metadata = new ParquetTableLocationKey(
                dest.toURI(), 0, null, ParquetInstructions.EMPTY).getMetadata();
        final MessageType schema = metadata.getFileMetaData().getSchema();
        final int colIdx = schema.getFieldIndex(columnName);
        final ColumnChunkMetaData columnChunkMetaData = metadata.getBlocks().get(0).getColumns().get(colIdx);
        return columnChunkMetaData.getStatistics();
    }

    /**
     * Helper method to assert the min/max values from the statistics.
     */
    private static void assertMinMax(
            final File dest,
            final String columnName,
            final Class<? extends Statistics<?>> statsClass,
            final int expectedNulls,
            final Object expectedMin,
            final Object expectedMax,
            final Class<?> dhColumnType) {
        final Optional<MinMax<?>> minMax =
                getMinMaxFromStatisticsHelper(dest, columnName, statsClass, expectedNulls, dhColumnType);
        assertTrue(minMax.isPresent());
        assertEquals(expectedMin, minMax.get().min());
        assertEquals(expectedMax, minMax.get().max());
    }

    /**
     * Helper method to assert that min-max derived from statistics is empty (which means unusable by DH).
     */
    private static void assertEmptyMinMax(
            final File dest,
            final String columnName,
            final Class<? extends Statistics<?>> statsClass,
            final int expectedNulls,
            final Class<?> dhColumnType) {
        final Optional<MinMax<?>> minMax =
                getMinMaxFromStatisticsHelper(dest, columnName, statsClass, expectedNulls, dhColumnType);
        assertTrue(minMax.isEmpty());
    }

    private static Optional<MinMax<?>> getMinMaxFromStatisticsHelper(
            final File dest,
            final String columnName,
            final Class<? extends Statistics<?>> statsClass,
            final int expectedNulls,
            final Class<?> dhColumnType) {
        final Statistics<?> statistics = getColumnStatistics(dest, columnName);
        assertTrue(statsClass.getSimpleName(), statsClass.isInstance(statistics));
        assertEquals(expectedNulls, statistics.getNumNulls());
        return MinMaxFromStatistics.get(statistics, dhColumnType);
    }

    /**
     * This test verifies that the statistics builder logic for NaN values automatically handles NaN values. This
     * behavior is important because DH currently writes NaN values to statistics.
     */
    // TODO (DH-10771): DH should not write NaN values to statistics.
    @Test
    public void testStatisticsWithNaN() {
        final Statistics.Builder builder = Statistics.getBuilderForReading(
                new PrimitiveType(Type.Repetition.REQUIRED, FLOAT, "floatColumn"));
        builder.withMin(BytesUtils.intToBytes(Float.floatToIntBits(Float.NaN)));
        builder.withMax(BytesUtils.intToBytes(Float.floatToIntBits(1.2f)));
        builder.withNumNulls(0);
        final Statistics<?> statistics = builder.build();
        assertFalse(statistics.hasNonNullValue());

        // MinMaxFromStatistics should return Optional.empty() regardless of dhColumnType
        assertTrue(MinMaxFromStatistics.get(statistics, Float.class).isEmpty());
    }
}

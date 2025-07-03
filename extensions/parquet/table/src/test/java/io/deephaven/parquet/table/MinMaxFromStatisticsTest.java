//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.util.ColumnHolder;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.parquet.base.NullStatistics;
import io.deephaven.parquet.table.pushdown.MinMax;
import io.deephaven.parquet.table.pushdown.MinMaxFromStatistics;
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
            final Optional<MinMax<?>> minMax = MinMaxFromStatistics.get(statistics);
            assertEquals(Optional.empty(), minMax);
        }
        {
            final Statistics<?> statistics = NullStatistics.INSTANCE;
            final Optional<MinMax<?>> minMax = MinMaxFromStatistics.get(statistics);
            assertEquals(Optional.empty(), minMax);
        }
    }

    @Test
    public void booleanColumnTest() {
        final File dest = new File(rootFile, "booleanColumn.parquet");
        final String columnName = "Booleans";

        writeTable(newTable(booleanCol(columnName, null, false, true, false, null, true)), dest.toString());
        assertMinMax(dest, columnName, BooleanStatistics.class, 2, false, true);

        writeTable(newTable(booleanCol(columnName, null, null, null)), dest.toString());
        assertEmptyStatistics(dest, columnName, BooleanStatistics.class, 3);
    }

    @Test
    public void charColumnTest() {
        final File dest = new File(rootFile, "charColumn.parquet");
        final String columnName = "Characters";

        writeTable(newTable(charCol(columnName, NULL_CHAR, 'A', 'z', 'M', NULL_CHAR)), dest.toString());
        assertMinMax(dest, columnName, IntStatistics.class, 2, (long) 'A', (long) 'z');

        writeTable(newTable(charCol(columnName, NULL_CHAR, NULL_CHAR, NULL_CHAR)), dest.toString());
        assertEmptyStatistics(dest, columnName, IntStatistics.class, 3);
    }

    @Test
    public void byteColumnTest() {
        final File dest = new File(rootFile, "byteColumn.parquet");
        final String columnName = "Bytes";

        writeTable(newTable(byteCol(columnName, NULL_BYTE, (byte) -10, (byte) 100, (byte) 0)), dest.toString());
        assertMinMax(dest, columnName, IntStatistics.class, 1, -10, 100);

        writeTable(newTable(byteCol(columnName, NULL_BYTE, NULL_BYTE, NULL_BYTE)), dest.toString());
        assertEmptyStatistics(dest, columnName, IntStatistics.class, 3);
    }

    @Test
    public void shortColumnTest() {
        final File dest = new File(rootFile, "shortColumn.parquet");
        final String columnName = "Shorts";

        writeTable(newTable(shortCol(columnName, NULL_SHORT, (short) -1024, (short) 2048)), dest.toString());
        assertMinMax(dest, columnName, IntStatistics.class, 1, -1024, 2048);

        writeTable(newTable(shortCol(columnName, NULL_SHORT, NULL_SHORT)), dest.toString());
        assertEmptyStatistics(dest, columnName, IntStatistics.class, 2);
    }

    @Test
    public void intColumnTest() {
        final File dest = new File(rootFile, "intColumn.parquet");
        final String columnName = "Integers";

        writeTable(newTable(intCol(columnName, NULL_INT, 42, -1, 100, 200, NULL_INT, 300, 400, NULL_INT, 500)),
                dest.toString());
        assertMinMax(dest, columnName, IntStatistics.class, 3, -1, 500);

        writeTable(newTable(intCol(columnName, NULL_INT, NULL_INT, NULL_INT)), dest.toString());
        assertEmptyStatistics(dest, columnName, IntStatistics.class, 3);
    }

    @Test
    public void longColumnTest() {
        final File dest = new File(rootFile, "longColumn.parquet");
        final String columnName = "Longs";

        writeTable(newTable(longCol(columnName, NULL_LONG, -10_000_000_000L, 100L, 10_000_000_000L)),
                dest.toString());
        assertMinMax(dest, columnName, LongStatistics.class, 1, -10_000_000_000L, 10_000_000_000L);

        writeTable(newTable(longCol(columnName, NULL_LONG, NULL_LONG)), dest.toString());
        assertEmptyStatistics(dest, columnName, LongStatistics.class, 2);
    }

    @Test
    public void floatColumnTest() {
        final File dest = new File(rootFile, "floatColumn.parquet");
        final String columnName = "Floats";

        writeTable(newTable(floatCol(columnName, NULL_FLOAT, -1.5f, 3.14f, 2.7f)), dest.toString());
        assertMinMax(dest, columnName, FloatStatistics.class, 1, -1.5f, 3.14f);

        writeTable(newTable(floatCol(columnName, NULL_FLOAT, NULL_FLOAT, NULL_FLOAT)), dest.toString());
        assertEmptyStatistics(dest, columnName, FloatStatistics.class, 3);
    }

    @Test
    public void doubleColumnTest() {
        final File dest = new File(rootFile, "doubleColumn.parquet");
        final String columnName = "Doubles";

        writeTable(newTable(doubleCol(columnName, NULL_DOUBLE, -1.5, 2.718, 3.14)), dest.toString());
        assertMinMax(dest, columnName, DoubleStatistics.class, 1, -1.5, 3.14);

        writeTable(newTable(doubleCol(columnName, NULL_DOUBLE, NULL_DOUBLE)), dest.toString());
        assertEmptyStatistics(dest, columnName, DoubleStatistics.class, 2);
    }

    @Test
    public void stringColumnTest() {
        final File dest = new File(rootFile, "stringColumn.parquet");
        final String columnName = "Strings";

        writeTable(newTable(stringCol(columnName, null, "aaa", "zzz", "mmm", null)), dest.toString());
        assertMinMax(dest, columnName, BinaryStatistics.class, 2, "aaa", "zzz");

        writeTable(newTable(stringCol(columnName, null, null, null)), dest.toString());
        assertEmptyStatistics(dest, columnName, BinaryStatistics.class, 3);
    }

    @Test
    public void instantColumnTest() {
        final File dest = new File(rootFile, "instantColumn.parquet");
        final String columnName = "Instants";

        final Instant instant1 = Instant.ofEpochMilli(1_234L);
        final Instant instant2 = Instant.ofEpochMilli(10_000L);
        final Instant instant3 = Instant.ofEpochMilli(5_532L);
        writeTable(newTable(instantCol(columnName, null, instant1, instant2, instant3)), dest.toString());
        assertMinMax(dest, columnName, LongStatistics.class, 1, instant1, instant2);

        writeTable(newTable(instantCol(columnName, null, null, null)), dest.toString());
        assertEmptyStatistics(dest, columnName, LongStatistics.class, 3);
    }

    @Test
    public void localDateColumnTest() {
        final File dest = new File(rootFile, "localDateColumn.parquet");
        final String columnName = "LocalDates";

        final LocalDate localDate1 = LocalDate.of(2020, 1, 1);
        final LocalDate localDate2 = LocalDate.of(2020, 12, 31);
        final Table table = newTable(new ColumnHolder<>(columnName, LocalDate.class, null, false,
                localDate1, null, localDate2));
        writeTable(table, dest.toString());
        assertMinMax(dest, columnName, IntStatistics.class, 1, localDate1, localDate2);

        writeTable(newTable(new ColumnHolder<>(columnName, LocalDate.class, null, false, null, null)),
                dest.toString());
        assertEmptyStatistics(dest, columnName, IntStatistics.class, 2);
    }

    @Test
    public void localTimeColumnTest() {
        final File dest = new File(rootFile, "localTimeColumn.parquet");
        final String columnName = "LocalTimes";

        final LocalTime localTime1 = LocalTime.of(1, 2, 3, 4);
        final LocalTime localTime2 = LocalTime.of(23, 59, 59);
        final Table table = newTable(new ColumnHolder<>(columnName, LocalTime.class, null, false,
                localTime1, null, localTime2));
        writeTable(table, dest.toString());
        assertMinMax(dest, columnName, LongStatistics.class, 1, localTime1, localTime2);

        writeTable(newTable(new ColumnHolder<>(columnName, LocalTime.class, null, false, null, null)),
                dest.toString());
        assertEmptyStatistics(dest, columnName, LongStatistics.class, 2);
    }

    @Test
    public void localDateTimeColumnTest() {
        final File dest = new File(rootFile, "localDateTimeColumn.parquet");
        final String columnName = "LocalDateTimes";

        final LocalDateTime localDateTime1 = LocalDateTime.of(2020, 1, 1, 1, 2);
        final LocalDateTime localDateTime2 = LocalDateTime.of(2020, 12, 31, 23, 59, 59);
        final Table table = newTable(new ColumnHolder<>("LocalDateTimes", LocalDateTime.class, null, false,
                localDateTime1, null, localDateTime2));
        writeTable(table, dest.toString());
        assertMinMax(dest, columnName, LongStatistics.class, 1, localDateTime1, localDateTime2);

        writeTable(newTable(new ColumnHolder<>("LocalDateTimes", LocalDateTime.class, null, false, null, null)),
                dest.toString());
        assertEmptyStatistics(dest, columnName, LongStatistics.class, 2);
    }

    @Test
    public void decimalColumnTest() {
        final File dest = new File(rootFile, "decimalColumn.parquet");
        final String columnName = "Decimals";

        final BigDecimal decimal1 = BigDecimal.valueOf(123_423_367_532L);
        final BigDecimal decimal2 = BigDecimal.valueOf(422_123_132_234L);
        writeTable(newTable(new ColumnHolder<>(columnName, BigDecimal.class, null, false,
                decimal1, null, decimal2)), dest.toString());
        // TODO (DH-19666) Add tests for decimal statistics
        assertEmptyStatistics(dest, columnName, BinaryStatistics.class, 1);

        writeTable(newTable(new ColumnHolder<>(columnName, BigDecimal.class, null, false, null, null)),
                dest.toString());
        assertEmptyStatistics(dest, columnName, BinaryStatistics.class, 2);
    }

    /**
     * <pre>
     * import pyarrow as pa
     * import pyarrow.parquet as pq
     *
     * def time_array(values, unit):
     *     return pa.array(values, type=pa.time32(unit) if unit == "ms" else pa.time64(unit))
     *
     * table = pa.Table.from_arrays(
     *     [
     *         # MILLIS
     *         time_array([None, 1_000, 50_000], "ms"),
     *         time_array([None, None, None], "ms"),
     *
     *         # MICROS
     *         time_array([None, 1_000_000, 5_000_000], "us"),
     *         time_array([None, None, None], "us"),
     *
     *         # NANOS
     *         time_array([None, 100_000_000, 500_000_000], "ns"),
     *         time_array([None, None, None], "ns"),
     *     ],
     *     names=[
     *         "time_ms_mix", "time_ms_null",
     *         "time_us_mix", "time_us_null",
     *         "time_ns_mix", "time_ns_null",
     *     ],
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
        assertMinMax(
                dest,
                "time_ms_mix",
                IntStatistics.class,
                1,
                LocalTime.ofNanoOfDay(1_000 * DateTimeUtils.MILLI),
                LocalTime.ofNanoOfDay(50_000 * DateTimeUtils.MILLI));
        assertEmptyStatistics(dest, "time_ms_null", IntStatistics.class, 3);

        // TIME micros
        assertMinMax(dest,
                "time_us_mix",
                LongStatistics.class,
                1,
                LocalTime.ofNanoOfDay(1_000_000 * DateTimeUtils.MICRO),
                LocalTime.ofNanoOfDay(5_000_000 * DateTimeUtils.MICRO));
        assertEmptyStatistics(dest, "time_us_null", LongStatistics.class, 3);

        // TIME nanos
        assertMinMax(dest,
                "time_ns_mix",
                LongStatistics.class,
                1,
                LocalTime.ofNanoOfDay(100_000_000),
                LocalTime.ofNanoOfDay(500_000_000));
        assertEmptyStatistics(dest, "time_ns_null", LongStatistics.class, 3);
    }

    /**
     * <pre>
     * import pyarrow as pa
     * import pyarrow.parquet as pq
     *
     * def ts_array(values, unit):
     *     return pa.array(values, type=pa.timestamp(unit))
     *
     * table = pa.Table.from_arrays(
     *     [
     *         # MILLIS
     *         ts_array([None, 1_000, 50_000], "ms"),
     *         ts_array([None, None, None], "ms"),
     *
     *         # MICROS
     *         ts_array([None, 1_000_000, 5_000_000], "us"),
     *         ts_array([None, None, None], "us"),
     *
     *         # NANOS
     *         ts_array([None, 100_000_000, 500_000_000], "ns"),
     *         ts_array([None, None, None], "ns"),
     *     ],
     *     names=[
     *         "ts_ms_mix", "ts_ms_null",
     *         "ts_us_mix", "ts_us_null",
     *         "ts_ns_mix", "ts_ns_null",
     *     ],
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
        assertMinMax(dest,
                "ts_ms_mix",
                LongStatistics.class,
                1,
                LocalDateTime.ofInstant(Instant.ofEpochMilli(1_000L), ZoneOffset.UTC),
                LocalDateTime.ofInstant(Instant.ofEpochMilli(50_000L), ZoneOffset.UTC));
        assertEmptyStatistics(dest, "ts_ms_null", LongStatistics.class, 3);

        // TIMESTAMP micros
        assertMinMax(dest,
                "ts_us_mix",
                LongStatistics.class,
                1,
                LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC),
                LocalDateTime.ofEpochSecond(5, 0, ZoneOffset.UTC));
        assertEmptyStatistics(dest, "ts_us_null", LongStatistics.class, 3);

        // TIMESTAMP nanos
        assertMinMax(dest,
                "ts_ns_mix",
                LongStatistics.class,
                1,
                LocalDateTime.ofEpochSecond(0, 100_000_000, ZoneOffset.UTC),
                LocalDateTime.ofEpochSecond(0, 500_000_000, ZoneOffset.UTC));
        assertEmptyStatistics(dest, "ts_ns_null", LongStatistics.class, 3);
    }

    /**
     * <pre>
     * import pyarrow as pa
     * import pyarrow.parquet as pq
     *
     * def ts_array(values, unit):
     *     return pa.array(values, type=pa.timestamp(unit, tz="UTC"))
     *
     * table = pa.Table.from_arrays(
     *     [
     *         # MILLIS
     *         ts_array([None, 0, 50_000], "ms"),
     *         ts_array([None, None, None], "ms"),
     *
     *         # MICROS
     *         ts_array([None, 1_000_000, 5_000_000], "us"),
     *         ts_array([None, None, None], "us"),
     *
     *         # NANOS
     *         ts_array([None, 100_000_000, 500_000_000], "ns"),
     *         ts_array([None, None, None], "ns"),
     *     ],
     *     names=[
     *         "ts_ms_mix", "ts_ms_null",
     *         "ts_us_mix", "ts_us_null",
     *         "ts_ns_mix", "ts_ns_null",
     *     ],
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
        assertMinMax(
                dest,
                "ts_ms_mix",
                LongStatistics.class,
                1,
                Instant.ofEpochMilli(0L),
                Instant.ofEpochMilli(50_000L));
        assertEmptyStatistics(dest, "ts_ms_null", LongStatistics.class, 3);

        // TIMESTAMP micros
        assertMinMax(
                dest,
                "ts_us_mix",
                LongStatistics.class,
                1,
                Instant.ofEpochMilli(1_000L),
                Instant.ofEpochMilli(5_000L));
        assertEmptyStatistics(dest, "ts_us_null", LongStatistics.class, 3);

        // TIMESTAMP nanos
        assertMinMax(
                dest,
                "ts_ns_mix",
                LongStatistics.class,
                1,
                Instant.ofEpochMilli(100L),
                Instant.ofEpochMilli(500L));
        assertEmptyStatistics(dest, "ts_ns_null", LongStatistics.class, 3);
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
            final Object expectedMax) {
        final Optional<MinMax<?>> minMax = getMinMaxFromStatisticsHelper(dest, columnName, statsClass, expectedNulls);
        assertTrue(minMax.isPresent());
        assertEquals(expectedMin, minMax.get().min());
        assertEquals(expectedMax, minMax.get().max());
    }

    /**
     * Helper method to assert that statistics is empty (which means unusable by DH).
     */
    private static void assertEmptyStatistics(
            final File dest,
            final String columnName,
            final Class<? extends Statistics<?>> statsClass,
            final int expectedNulls) {
        final Optional<MinMax<?>> minMax = getMinMaxFromStatisticsHelper(dest, columnName, statsClass, expectedNulls);
        assertTrue(minMax.isEmpty());
    }

    private static Optional<MinMax<?>> getMinMaxFromStatisticsHelper(
            final File dest,
            final String columnName,
            final Class<? extends Statistics<?>> statsClass,
            final int expectedNulls) {
        final Statistics<?> statistics = getColumnStatistics(dest, columnName);
        assertTrue(statsClass.getSimpleName(), statsClass.isInstance(statistics));
        assertEquals(expectedNulls, statistics.getNumNulls());
        return MinMaxFromStatistics.get(statistics);
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
    }
}

//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.base.FileUtils;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
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

    /*---- Helpers ----*/
    private static Statistics<?> buildStats(
            final PrimitiveType colType,
            final byte[] min,
            final byte[] max,
            final long nullCount) {
        return Statistics.getBuilderForReading(colType)
                .withMin(min)
                .withMax(max)
                .withNumNulls(nullCount)
                .build();
    }

    private static Statistics<?> buildIntStats(
            final PrimitiveType colType, final byte[] min, final byte[] max, final long nulls) {
        return buildStats(colType, min, max, nulls);
    }

    private static Statistics<?> buildPrimitiveInt32Stats(
            final int min, final int max, final long nulls) {
        final PrimitiveType colType = Types.required(INT32).named("int32Primitive");
        return buildStats(colType,
                BytesUtils.intToBytes(min),
                BytesUtils.intToBytes(max),
                nulls);
    }

    private static Statistics<?> buildBooleanStats(
            final boolean minVal, final boolean maxVal, final long nulls) {
        final PrimitiveType colType = Types.required(BOOLEAN).named("boolCol");
        return buildStats(colType,
                new byte[] {(byte) (minVal ? 1 : 0)},
                new byte[] {(byte) (maxVal ? 1 : 0)},
                nulls);
    }

    private static Statistics<?> buildFloatStats(
            final float min, final float max, final long nulls) {
        final PrimitiveType colType = Types.required(FLOAT).named("floatPrimitive");
        return buildStats(colType,
                BytesUtils.intToBytes(Float.floatToIntBits(min)),
                BytesUtils.intToBytes(Float.floatToIntBits(max)),
                nulls);
    }

    private static Statistics<?> buildDoubleStats(
            final double min, final double max, final long nulls) {
        final PrimitiveType colType = Types.required(DOUBLE).named("doublePrimitive");
        return buildStats(colType,
                BytesUtils.longToBytes(Double.doubleToLongBits(min)),
                BytesUtils.longToBytes(Double.doubleToLongBits(max)),
                nulls);
    }

    private static Statistics<?> buildTimestampStats(
            final LogicalTypeAnnotation.TimeUnit unit,
            final boolean utc,
            final long min, final long max) {
        final PrimitiveType colType = Types.required(INT64)
                .as(LogicalTypeAnnotation.timestampType(utc, unit))
                .named("tsCol");
        return buildStats(colType,
                BytesUtils.longToBytes(min),
                BytesUtils.longToBytes(max),
                0L);
    }

    private static Statistics<?> buildTimeStats(
            final LogicalTypeAnnotation.TimeUnit unit,
            final long min, final long max) {
        final PrimitiveType colType = (unit == LogicalTypeAnnotation.TimeUnit.MILLIS
                ? Types.required(INT32)
                : Types.required(INT64))
                .as(LogicalTypeAnnotation.timeType(false, unit))
                .named("timeCol");
        return buildStats(colType,
                unit == LogicalTypeAnnotation.TimeUnit.MILLIS
                        ? BytesUtils.intToBytes((int) min)
                        : BytesUtils.longToBytes(min),
                unit == LogicalTypeAnnotation.TimeUnit.MILLIS
                        ? BytesUtils.intToBytes((int) max)
                        : BytesUtils.longToBytes(max),
                0L);
    }

    private static <T> void assertMatches(
            final boolean ok,
            final MutableObject<T> min,
            final T expectedMin,
            final MutableObject<T> max,
            final T expectedMax) {
        assertTrue(ok);
        assertEquals(min.getValue(), expectedMin);
        assertEquals(max.getValue(), expectedMax);
    }

    private static <T> void assertRejected(
            final boolean ok,
            final MutableObject<T> min,
            final MutableObject<T> max) {
        assertFalse(ok);
        assertNull(min.getValue());
        assertNull(max.getValue());
    }

    /*---- Tests ----*/

    // Bytes

    @Test
    public void signedByteStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(8, /* signed */ true))
                .named("byteColumn");
        final Statistics<?> stats = buildIntStats(colType, BytesUtils.intToBytes(-10), BytesUtils.intToBytes(10), 0L);
        final MutableObject<Byte> min = new MutableObject<>();
        final MutableObject<Byte> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForBytes(stats, min::setValue, max::setValue),
                min, (byte) -10,
                max, (byte) 10);
    }

    @Test
    public void unsignedByteStatisticsAreRejected() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(8, /* signed */ false))
                .named("byteColumn");
        final Statistics<?> stats = buildIntStats(colType, BytesUtils.intToBytes(5), BytesUtils.intToBytes(10), 0L);
        final MutableObject<Byte> min = new MutableObject<>();
        final MutableObject<Byte> max = new MutableObject<>();
        assertRejected(MinMaxFromStatistics.getMinMaxForBytes(stats, min::setValue, max::setValue), min, max);
    }

    // Characters

    @Test
    public void unsignedByteCharStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(8, /* signed */ false))
                .named("charUINT8");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(65), BytesUtils.intToBytes(122), 0L);

        final MutableObject<Character> min = new MutableObject<>();
        final MutableObject<Character> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForChars(stats, min::setValue, max::setValue),
                min, (char) 65,
                max, (char) 122);
    }

    @Test
    public void unsignedShortCharStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(16, /* signed */ false))
                .named("charUINT16");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(65), BytesUtils.intToBytes(1024), 0L);

        final MutableObject<Character> min = new MutableObject<>();
        final MutableObject<Character> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForChars(stats, min::setValue, max::setValue),
                min, (char) 65,
                max, (char) 1024);
    }

    @Test
    public void signedByteCharStatisticsAreRejected() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(8, /* signed */ true))
                .named("charINT8");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(5), BytesUtils.intToBytes(10), 0L);

        final MutableObject<Character> min = new MutableObject<>();
        final MutableObject<Character> max = new MutableObject<>();
        assertRejected(
                MinMaxFromStatistics.getMinMaxForChars(stats, min::setValue, max::setValue),
                min, max);
    }

    // Shorts

    @Test
    public void signedByteShortStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(8, /* signed */ true))
                .named("shortINT8");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(-10), BytesUtils.intToBytes(100), 0L);
        final MutableObject<Short> min = new MutableObject<>();
        final MutableObject<Short> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForShorts(stats, min::setValue, max::setValue),
                min, (short) -10,
                max, (short) 100);
    }

    @Test
    public void signedShortStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(16, /* signed */ true))
                .named("shortINT16");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(-1024), BytesUtils.intToBytes(2048), 0L);
        final MutableObject<Short> min = new MutableObject<>();
        final MutableObject<Short> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForShorts(stats, min::setValue, max::setValue),
                min, (short) -1024,
                max, (short) 2048);
    }

    @Test
    public void unsignedByteShortStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(8, /* signed */ false))
                .named("shortUINT8");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(0), BytesUtils.intToBytes(255), 0L);
        final MutableObject<Short> min = new MutableObject<>();
        final MutableObject<Short> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForShorts(stats, min::setValue, max::setValue),
                min, (short) 0,
                max, (short) 255);
    }

    @Test
    public void booleanShortStatisticsAreMaterialised() {
        final Statistics<?> stats = buildBooleanStats(false, true, 0L);
        final MutableObject<Short> min = new MutableObject<>();
        final MutableObject<Short> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForShorts(stats, min::setValue, max::setValue),
                min, (short) 0,
                max, (short) 1);
    }

    @Test
    public void unsignedShortStatisticsAreRejected() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(16, /* signed */ false))
                .named("shortUINT16");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(5), BytesUtils.intToBytes(10), 0L);
        final MutableObject<Short> min = new MutableObject<>();
        final MutableObject<Short> max = new MutableObject<>();
        assertRejected(
                MinMaxFromStatistics.getMinMaxForShorts(stats, min::setValue, max::setValue),
                min, max);
    }

    // Integers

    @Test
    public void signedByteIntStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(8, /* signed */ true))
                .named("intINT8");
        final Statistics<?> stats = buildIntStats(colType, BytesUtils.intToBytes(-10), BytesUtils.intToBytes(100), 0L);
        final MutableObject<Integer> min = new MutableObject<>();
        final MutableObject<Integer> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForInts(stats, min::setValue, max::setValue),
                min, -10,
                max, 100);
    }

    @Test
    public void signedShortIntStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(16, /* signed */ true))
                .named("intINT16");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(-1024), BytesUtils.intToBytes(2048), 0L);
        final MutableObject<Integer> min = new MutableObject<>();
        final MutableObject<Integer> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForInts(stats, min::setValue, max::setValue),
                min, -1024,
                max, 2048);
    }

    @Test
    public void signedInt32StatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(32, /* signed */ true))
                .named("intINT32");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(-1_000_000), BytesUtils.intToBytes(1_000_000), 0L);
        final MutableObject<Integer> min = new MutableObject<>();
        final MutableObject<Integer> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForInts(stats, min::setValue, max::setValue),
                min, -1_000_000,
                max, 1_000_000);
    }

    @Test
    public void unsignedByteIntStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(8, /* signed */ false))
                .named("intUINT8");
        final Statistics<?> stats = buildIntStats(colType, BytesUtils.intToBytes(0), BytesUtils.intToBytes(255), 0L);
        final MutableObject<Integer> min = new MutableObject<>();
        final MutableObject<Integer> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForInts(stats, min::setValue, max::setValue),
                min, 0,
                max, 255);
    }

    @Test
    public void unsignedShortIntStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(16, /* signed */ false))
                .named("intUINT16");
        final Statistics<?> stats = buildIntStats(colType, BytesUtils.intToBytes(0), BytesUtils.intToBytes(65_535), 0L);
        final MutableObject<Integer> min = new MutableObject<>();
        final MutableObject<Integer> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForInts(stats, min::setValue, max::setValue),
                min, 0,
                max, 65_535);
    }

    @Test
    public void booleanIntStatisticsAreMaterialised() {
        final Statistics<?> stats = buildBooleanStats(false, true, 0L);
        final MutableObject<Integer> min = new MutableObject<>();
        final MutableObject<Integer> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForInts(stats, min::setValue, max::setValue),
                min, 0,
                max, 1);
    }

    @Test
    public void primitiveInt32StatisticsAreMaterialised() {
        final Statistics<?> stats = buildPrimitiveInt32Stats(-42, 2042, 0L);
        final MutableObject<Integer> min = new MutableObject<>();
        final MutableObject<Integer> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForInts(stats, min::setValue, max::setValue),
                min, -42,
                max, 2042);
    }

    @Test
    public void unsignedInt32StatisticsAreRejected() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(32, /* signed */ false))
                .named("intUINT32");
        final Statistics<?> stats = buildIntStats(colType, BytesUtils.intToBytes(5), BytesUtils.intToBytes(10), 0L);
        final MutableObject<Integer> min = new MutableObject<>();
        final MutableObject<Integer> max = new MutableObject<>();
        assertRejected(
                MinMaxFromStatistics.getMinMaxForInts(stats, min::setValue, max::setValue),
                min, max);
    }

    // Longs

    @Test
    public void signedByteLongStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(8, /* signed */ true))
                .named("longINT8");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(-10), BytesUtils.intToBytes(100), 0L);
        final MutableObject<Long> min = new MutableObject<>();
        final MutableObject<Long> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLongs(stats, min::setValue, max::setValue),
                min, -10L,
                max, 100L);
    }

    @Test
    public void signedShortLongStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(16, /* signed */ true))
                .named("longINT16");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(-1024), BytesUtils.intToBytes(2048), 0L);
        final MutableObject<Long> min = new MutableObject<>();
        final MutableObject<Long> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLongs(stats, min::setValue, max::setValue),
                min, -1024L,
                max, 2048L);
    }

    @Test
    public void signedIntLongStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(32, /* signed */ true))
                .named("longINT32");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(-1_000_000), BytesUtils.intToBytes(1_000_000), 0L);
        final MutableObject<Long> min = new MutableObject<>();
        final MutableObject<Long> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLongs(stats, min::setValue, max::setValue),
                min, -1_000_000L,
                max, 1_000_000L);
    }

    @Test
    public void signedLongStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT64)
                .as(LogicalTypeAnnotation.intType(64, /* signed */ true))
                .named("longINT64");
        final Statistics<?> stats =
                buildStats(colType, BytesUtils.longToBytes(-5_000_000_000L), BytesUtils.longToBytes(5_000_000_000L),
                        0L);
        final MutableObject<Long> min = new MutableObject<>();
        final MutableObject<Long> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLongs(stats, min::setValue, max::setValue),
                min, -5_000_000_000L,
                max, 5_000_000_000L);
    }

    @Test
    public void unsignedByteLongStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(8, /* signed */ false))
                .named("longUINT8");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(0), BytesUtils.intToBytes(255), 0L);
        final MutableObject<Long> min = new MutableObject<>();
        final MutableObject<Long> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLongs(stats, min::setValue, max::setValue),
                min, 0L,
                max, 255L);
    }

    @Test
    public void unsignedShortLongStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(16, /* signed */ false))
                .named("longUINT16");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(0), BytesUtils.intToBytes(65_535), 0L);
        final MutableObject<Long> min = new MutableObject<>();
        final MutableObject<Long> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLongs(stats, min::setValue, max::setValue),
                min, 0L,
                max, 65_535L);
    }

    @Test
    public void unsignedIntLongStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(32, /* signed */ false))
                .named("longUINT32");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(0), BytesUtils.intToBytes(Integer.MAX_VALUE), 0L);
        final MutableObject<Long> min = new MutableObject<>();
        final MutableObject<Long> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLongs(stats, min::setValue, max::setValue),
                min, 0L,
                max, (long) Integer.MAX_VALUE);
    }

    @Test
    public void booleanLongStatisticsAreMaterialised() {
        final Statistics<?> stats = buildBooleanStats(false, true, 0L);
        final MutableObject<Long> min = new MutableObject<>();
        final MutableObject<Long> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLongs(stats, min::setValue, max::setValue),
                min, 0L,
                max, 1L);
    }

    @Test
    public void primitiveInt32LongStatisticsAreMaterialised() {
        final Statistics<?> stats = buildPrimitiveInt32Stats(-42, 2_042, 0L);
        final MutableObject<Long> min = new MutableObject<>();
        final MutableObject<Long> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLongs(stats, min::setValue, max::setValue),
                min, -42L,
                max, 2_042L);
    }

    @Test
    public void primitiveInt64LongStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT64).named("int64Primitive");
        final Statistics<?> stats =
                buildStats(colType, BytesUtils.longToBytes(-10_000L), BytesUtils.longToBytes(99_999L), 0L);
        final MutableObject<Long> min = new MutableObject<>();
        final MutableObject<Long> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLongs(stats, min::setValue, max::setValue),
                min, -10_000L,
                max, 99_999L);
    }

    @Test
    public void unsignedLongStatisticsAreRejected() {
        final PrimitiveType colType = Types.required(INT64)
                .as(LogicalTypeAnnotation.intType(64, /* signed */ false))
                .named("longUINT64");
        final Statistics<?> stats =
                buildStats(colType, BytesUtils.longToBytes(5L), BytesUtils.longToBytes(10L), 0L);
        final MutableObject<Long> min = new MutableObject<>();
        final MutableObject<Long> max = new MutableObject<>();
        assertRejected(
                MinMaxFromStatistics.getMinMaxForLongs(stats, min::setValue, max::setValue),
                min, max);
    }

    // Floats

    @Test
    public void floatStatisticsAreMaterialised() {
        final Statistics<?> stats = buildFloatStats(-1.5f, 3.0f, 0L);
        final MutableObject<Float> min = new MutableObject<>();
        final MutableObject<Float> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForFloats(stats, min::setValue, max::setValue),
                min, -1.5f,
                max, 3.0f);
    }

    @Test
    public void doublePrimitiveIsRejectedByGetMinMaxForFloats() {
        final Statistics<?> stats = buildDoubleStats(-2.0, 2.0, 0L);
        final MutableObject<Float> min = new MutableObject<>();
        final MutableObject<Float> max = new MutableObject<>();
        assertRejected(
                MinMaxFromStatistics.getMinMaxForFloats(stats, min::setValue, max::setValue),
                min, max);
    }

    // Doubles

    @Test
    public void floatPrimitiveDoublesAreMaterialised() {
        final Statistics<?> stats = buildFloatStats(-1.5f, 3.0f, 0L);
        final MutableObject<Double> min = new MutableObject<>();
        final MutableObject<Double> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForDoubles(stats, min::setValue, max::setValue),
                min, -1.5,
                max, 3.0);
    }

    @Test
    public void doublePrimitiveDoublesAreMaterialised() {
        final Statistics<?> stats = buildDoubleStats(-2.0, 4.0, 0L);
        final MutableObject<Double> min = new MutableObject<>();
        final MutableObject<Double> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForDoubles(stats, min::setValue, max::setValue),
                min, -2.0,
                max, 4.0);
    }

    // Strings

    @Test
    public void stringLogicalStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(PrimitiveType.PrimitiveTypeName.BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("strBinary");
        final Statistics<?> stats = buildStats(
                colType, "aaa".getBytes(StandardCharsets.UTF_8), "zzz".getBytes(StandardCharsets.UTF_8), 0L);
        final MutableObject<String> min = new MutableObject<>();
        final MutableObject<String> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForStrings(stats, min::setValue, max::setValue),
                min, "aaa",
                max, "zzz");
    }

    @Test
    public void binaryWithoutStringLogicalTypeIsRejected() {
        final PrimitiveType colType = Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named("rawBinary");
        final Statistics<?> stats = buildStats(
                colType, "foo".getBytes(StandardCharsets.UTF_8), "qux".getBytes(StandardCharsets.UTF_8), 0L);
        final MutableObject<String> min = new MutableObject<>();
        final MutableObject<String> max = new MutableObject<>();
        assertRejected(
                MinMaxFromStatistics.getMinMaxForStrings(stats, min::setValue, max::setValue),
                min, max);
    }

    // Instants

    @Test
    public void timestampMillisUTCStatisticsAreMaterialised() {
        final Statistics<?> stats = buildTimestampStats(
                LogicalTypeAnnotation.TimeUnit.MILLIS, /* adjustedToUTC */ true,
                0L, 50_000L);
        final MutableObject<Instant> min = new MutableObject<>();
        final MutableObject<Instant> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForInstants(stats, min::setValue, max::setValue),
                min, Instant.ofEpochMilli(0L),
                max, Instant.ofEpochMilli(50_000L));
    }

    @Test
    public void timestampMicrosUTCStatisticsAreMaterialised() {
        final Statistics<?> stats = buildTimestampStats(
                LogicalTypeAnnotation.TimeUnit.MICROS, /* adjustedToUTC */ true,
                1_000_000L, 5_000_000L);
        final MutableObject<Instant> min = new MutableObject<>();
        final MutableObject<Instant> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForInstants(stats, min::setValue, max::setValue),
                min, Instant.ofEpochSecond(1),
                max, Instant.ofEpochSecond(5));
    }

    @Test
    public void timestampNanosUTCStatisticsAreMaterialised() {
        final Statistics<?> stats = buildTimestampStats(
                LogicalTypeAnnotation.TimeUnit.NANOS, /* adjustedToUTC */ true,
                100_000_000L, 500_000_000L);
        final MutableObject<Instant> min = new MutableObject<>();
        final MutableObject<Instant> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForInstants(stats, min::setValue, max::setValue),
                min, Instant.ofEpochSecond(0, 100_000_000L),
                max, Instant.ofEpochSecond(0, 500_000_000L));
    }

    @Test
    public void timestampNonUTCStatisticsAreRejected() {
        final Statistics<?> stats = buildTimestampStats(
                LogicalTypeAnnotation.TimeUnit.MILLIS, /* adjustedToUTC */ false,
                0L, 1_000L);
        final MutableObject<Instant> min = new MutableObject<>();
        final MutableObject<Instant> max = new MutableObject<>();
        assertRejected(
                MinMaxFromStatistics.getMinMaxForInstants(stats, min::setValue, max::setValue),
                min, max);
    }

    // LocalDateTime

    @Test
    public void timestampMillisNonUTCStatisticsAreMaterialised() {
        final Statistics<?> stats = buildTimestampStats(
                LogicalTypeAnnotation.TimeUnit.MILLIS, /* adjustedToUTC */ false,
                1_000L, 50_000L);
        final MutableObject<LocalDateTime> min = new MutableObject<>();
        final MutableObject<LocalDateTime> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLocalDateTimes(stats, min::setValue, max::setValue),
                min, LocalDateTime.ofInstant(Instant.ofEpochMilli(1_000L), ZoneOffset.UTC),
                max, LocalDateTime.ofInstant(Instant.ofEpochMilli(50_000L), ZoneOffset.UTC));
    }

    @Test
    public void timestampMicrosNonUTCStatisticsAreMaterialised() {
        final Statistics<?> stats = buildTimestampStats(
                LogicalTypeAnnotation.TimeUnit.MICROS, /* adjustedToUTC */ false,
                1_000_000L, 5_000_000L);
        final MutableObject<LocalDateTime> min = new MutableObject<>();
        final MutableObject<LocalDateTime> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLocalDateTimes(stats, min::setValue, max::setValue),
                min, LocalDateTime.ofEpochSecond(1, 0, ZoneOffset.UTC),
                max, LocalDateTime.ofEpochSecond(5, 0, ZoneOffset.UTC));
    }

    @Test
    public void timestampNanosNonUTCStatisticsAreMaterialised() {
        final Statistics<?> stats = buildTimestampStats(
                LogicalTypeAnnotation.TimeUnit.NANOS, /* adjustedToUTC */ false,
                100_000_000L, 500_000_000L);
        final MutableObject<LocalDateTime> min = new MutableObject<>();
        final MutableObject<LocalDateTime> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLocalDateTimes(stats, min::setValue, max::setValue),
                min, LocalDateTime.ofEpochSecond(0, 100_000_000, ZoneOffset.UTC),
                max, LocalDateTime.ofEpochSecond(0, 500_000_000, ZoneOffset.UTC));
    }

    @Test
    public void timestampUTCStatisticsAreRejectedForLocalDateTime() {
        final Statistics<?> stats = buildTimestampStats(
                LogicalTypeAnnotation.TimeUnit.MILLIS, /* adjustedToUTC */ true,
                0L, 1_000L);
        final MutableObject<LocalDateTime> min = new MutableObject<>();
        final MutableObject<LocalDateTime> max = new MutableObject<>();
        assertRejected(
                MinMaxFromStatistics.getMinMaxForLocalDateTimes(stats, min::setValue, max::setValue),
                min, max);
    }

    // LocalDate

    @Test
    public void dateStatisticsAreMaterialised() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.dateType())
                .named("dateCol");
        final int minDays = (int) LocalDate.of(2020, 1, 1).toEpochDay();
        final int maxDays = (int) LocalDate.of(2020, 12, 31).toEpochDay();
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(minDays), BytesUtils.intToBytes(maxDays), 0L);
        final MutableObject<LocalDate> min = new MutableObject<>();
        final MutableObject<LocalDate> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLocalDates(stats, min::setValue, max::setValue),
                min, LocalDate.of(2020, 1, 1),
                max, LocalDate.of(2020, 12, 31));
    }

    @Test
    public void nonDateStatisticsAreRejected() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(32, /* signed */ true))
                .named("intCol");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(0), BytesUtils.intToBytes(10), 0L);
        final MutableObject<LocalDate> min = new MutableObject<>();
        final MutableObject<LocalDate> max = new MutableObject<>();
        assertRejected(
                MinMaxFromStatistics.getMinMaxForLocalDates(stats, min::setValue, max::setValue),
                min, max);
    }

    // LocalTime

    @Test
    public void timeMillisStatisticsAreMaterialised() {
        final Statistics<?> stats = buildTimeStats(
                LogicalTypeAnnotation.TimeUnit.MILLIS,
                1_000L, 50_000L);
        final MutableObject<LocalTime> min = new MutableObject<>();
        final MutableObject<LocalTime> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLocalTimes(stats, min::setValue, max::setValue),
                min, LocalTime.ofNanoOfDay(1_000_000_000L),
                max, LocalTime.ofNanoOfDay(50_000_000_000L));
    }

    @Test
    public void timeMicrosStatisticsAreMaterialised() {
        final Statistics<?> stats = buildTimeStats(
                LogicalTypeAnnotation.TimeUnit.MICROS,
                1_000_000L, 5_000_000L);
        final MutableObject<LocalTime> min = new MutableObject<>();
        final MutableObject<LocalTime> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLocalTimes(stats, min::setValue, max::setValue),
                min, LocalTime.ofNanoOfDay(1_000_000_000L),
                max, LocalTime.ofNanoOfDay(5_000_000_000L));
    }

    @Test
    public void timeNanosStatisticsAreMaterialised() {
        final Statistics<?> stats = buildTimeStats(
                LogicalTypeAnnotation.TimeUnit.NANOS,
                100_000_000L, 500_000_000L);
        final MutableObject<LocalTime> min = new MutableObject<>();
        final MutableObject<LocalTime> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForLocalTimes(stats, min::setValue, max::setValue),
                min, LocalTime.ofNanoOfDay(100_000_000L),
                max, LocalTime.ofNanoOfDay(500_000_000L));
    }

    @Test
    public void nonTimeStatisticsAreRejected() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(32, /* signed */ true))
                .named("intCol");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(0), BytesUtils.intToBytes(10), 0L);
        final MutableObject<LocalTime> min = new MutableObject<>();
        final MutableObject<LocalTime> max = new MutableObject<>();
        assertRejected(
                MinMaxFromStatistics.getMinMaxForLocalTimes(stats, min::setValue, max::setValue),
                min, max);
    }

    // Comparables generic

    @Test
    public void comparableInstantStatisticsAreMaterialised() {
        final Statistics<?> stats = buildTimestampStats(
                LogicalTypeAnnotation.TimeUnit.MILLIS, /* adjustedToUTC */ true,
                0L, 50_000L);
        final MutableObject<Comparable<?>> min = new MutableObject<>();
        final MutableObject<Comparable<?>> max = new MutableObject<>();
        assertMatches(
                MinMaxFromStatistics.getMinMaxForComparable(stats, min::setValue, max::setValue, Instant.class),
                min, Instant.ofEpochMilli(0L),
                max, Instant.ofEpochMilli(50_000L));
    }

    @Test
    public void comparableUnsupportedTypeIsRejected() {
        final PrimitiveType colType = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(32, /* signed */ true))
                .named("intCol");
        final Statistics<?> stats =
                buildIntStats(colType, BytesUtils.intToBytes(1), BytesUtils.intToBytes(2), 0L);
        final MutableObject<Comparable<?>> min = new MutableObject<>();
        final MutableObject<Comparable<?>> max = new MutableObject<>();
        assertRejected(
                MinMaxFromStatistics.getMinMaxForComparable(stats, min::setValue, max::setValue, BigDecimal.class),
                min, max);
    }

    /**
     * This test verifies that the statistics builder logic for NaN values automatically handles NaN values. This
     * behavior is important because DH currently writes NaN values to statistics which automatically gets fixed by the
     * statistics builder.
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

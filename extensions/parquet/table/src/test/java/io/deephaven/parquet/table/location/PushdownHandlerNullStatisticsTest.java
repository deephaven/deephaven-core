//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.DoubleRangeFilter;
import io.deephaven.engine.table.impl.select.InstantRangeFilter;
import io.deephaven.engine.table.impl.select.IntRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.jetbrains.annotations.Nullable;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.StandardCharsets;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Pins the division of labour around nulls.
 * <p>
 * The pushdown handlers answer only from {@code min}/{@code max}, which describe a row group's <i>non-null</i> values.
 * They must therefore be entirely indifferent to the null count: whatever a handler answers for {@code numNulls == 0}
 * it must answer for any other count. Deciding what nulls mean for a given filter belongs to
 * {@code ParquetTableLocation.pushdownRowGroupMetadata}, which consults the filter's null behaviour and keeps the row
 * group unless {@link ParquetPushdownUtils#isKnownFreeOfNulls} proves there are none.
 * <p>
 * Without this, a well-meaning change that taught a handler to read {@code numNulls} would look like an improvement and
 * would quietly double-count the guard above it. Every statistics object in this folder's suites was built with
 * {@code withNumNulls(0L)} before this test existed, which is exactly the gap the null-handling defect lived in.
 */
@Category(OutOfBandTest.class)
public class PushdownHandlerNullStatisticsTest {

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("strCol"));

    /** Null counts that must all produce the same answer, including the "writer omitted it" case. */
    private static final Long[] NULL_COUNTS = {0L, 1L, 1_000L, null};

    private static Statistics<?> build(
            final PrimitiveType colType, final byte[] min, final byte[] max, @Nullable final Long numNulls) {
        final Statistics.Builder builder = Statistics.getBuilderForReading(colType).withMin(min).withMax(max);
        if (numNulls != null) {
            builder.withNumNulls(numNulls);
        }
        return builder.build();
    }

    private static Statistics<?> intStats(final int min, final int max, @Nullable final Long numNulls) {
        return build(Types.required(INT32).named("intCol"),
                BytesUtils.intToBytes(min), BytesUtils.intToBytes(max), numNulls);
    }

    private static Statistics<?> doubleStats(final double min, final double max, @Nullable final Long numNulls) {
        return build(Types.required(DOUBLE).named("doubleCol"),
                BytesUtils.longToBytes(Double.doubleToLongBits(min)),
                BytesUtils.longToBytes(Double.doubleToLongBits(max)), numNulls);
    }

    private static Statistics<?> stringStats(final String min, final String max, @Nullable final Long numNulls) {
        return build(Types.required(BINARY).as(LogicalTypeAnnotation.stringType()).named("strCol"),
                min.getBytes(StandardCharsets.UTF_8), max.getBytes(StandardCharsets.UTF_8), numNulls);
    }

    private static Statistics<?> instantStats(final long minNanos, final long maxNanos, @Nullable final Long numNulls) {
        return build(
                Types.required(INT64)
                        .as(LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.NANOS))
                        .named("instantCol"),
                BytesUtils.longToBytes(minNanos), BytesUtils.longToBytes(maxNanos), numNulls);
    }

    /**
     * Asserts that {@code handler} gives the same verdict for every null count. The scenario is chosen so the verdict
     * is {@code false} at zero nulls -- an answer that would flip if a handler started treating a null count as
     * evidence -- rather than a vacuous {@code true}.
     */
    private static void assertIndifferentToNullCount(
            final String description,
            final java.util.function.Function<Long, Boolean> handler,
            final boolean expected) {
        for (final Long numNulls : NULL_COUNTS) {
            assertEquals(description + " with numNulls=" + numNulls,
                    expected, handler.apply(numNulls));
        }
    }

    @Test
    public void integralHandlersIgnoreNullCount() {
        assertIndifferentToNullCount("int range entirely above the statistics",
                n -> IntPushdownHandler.maybeOverlaps(
                        new IntRangeFilter("intCol", 100, 200, true, true), intStats(1, 10, n)),
                false);
        assertIndifferentToNullCount("int range overlapping the statistics",
                n -> IntPushdownHandler.maybeOverlaps(
                        new IntRangeFilter("intCol", 5, 200, true, true), intStats(1, 10, n)),
                true);
        assertIndifferentToNullCount("int match outside the statistics",
                n -> IntPushdownHandler.maybeOverlaps(
                        new MatchFilter(MatchOptions.REGULAR, "intCol", 50), intStats(1, 10, n)),
                false);
    }

    @Test
    public void floatingPointHandlersIgnoreNullCount() {
        assertIndifferentToNullCount("double range entirely below the statistics",
                n -> DoublePushdownHandler.maybeOverlaps(
                        new DoubleRangeFilter("doubleCol", -5.0, -1.0, true, true), doubleStats(1.0, 10.0, n)),
                false);
        assertIndifferentToNullCount("double match inside the statistics",
                n -> DoublePushdownHandler.maybeOverlaps(
                        new MatchFilter(MatchOptions.REGULAR, "doubleCol", 5.0), doubleStats(1.0, 10.0, n)),
                true);
    }

    @Test
    public void instantHandlerIgnoresNullCount() {
        assertIndifferentToNullCount("instant range entirely above the statistics",
                n -> InstantPushdownHandler.maybeOverlaps(
                        new InstantRangeFilter("instantCol", 5_000L, 9_000L, true, true),
                        instantStats(1_000L, 2_000L, n)),
                false);
    }

    @Test
    public void stringHandlerIgnoresNullCount() {
        final MatchFilter outside = new MatchFilter(MatchOptions.REGULAR, "strCol", "zzz");
        outside.init(TABLE_DEFINITION);
        final StatisticsEvaluator evaluator = StringPushdownHandler.maybeCreateEvaluator(outside);
        assertNotNull(evaluator);

        assertIndifferentToNullCount("string match outside the statistics",
                n -> evaluator.maybeOverlaps(stringStats("aaa", "mmm", n)),
                false);
    }

    /**
     * An all-null row group reports no min/max at all. It never reaches a handler, because
     * {@link ParquetPushdownUtils#areStatisticsUsable} rejects it first -- which is also why no handler needs to cope
     * with absent extremes.
     */
    @Test
    public void allNullStatisticsAreRejectedBeforeAnyHandler() {
        final Statistics<?> allNull = Statistics.getBuilderForReading(Types.required(INT32).named("intCol"))
                .withNumNulls(1_000L)
                .build();

        assertFalse("an all-null row group has no non-null value", allNull.hasNonNullValue());
        assertFalse("and so its statistics are unusable", ParquetPushdownUtils.areStatisticsUsable(allNull));

        // The null count is still readable, and still proves nothing about the absence of nulls.
        assertFalse(ParquetPushdownUtils.isKnownFreeOfNulls(allNull, 0));
    }
}

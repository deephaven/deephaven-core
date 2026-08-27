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
import io.deephaven.util.QueryConstants;
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
import static org.junit.Assert.assertTrue;

/**
 * Pins the division of labour around nulls.
 * <p>
 * The pushdown handlers answer only from {@code min}/{@code max}, which describe a row group's <i>non-null</i> values.
 * No handler may read {@code numNulls} out of the statistics: whatever it answers for {@code numNulls == 0} it must
 * answer for any other count. Deciding what nulls mean for a given filter belongs to
 * {@code ParquetTableLocation.pushdownRowGroupMetadata}, which consults the filter's null behaviour and keeps the row
 * group unless {@link ParquetPushdownUtils#isProvenFreeOfNulls} proves there are none.
 * <p>
 * Parquet nulls are not a handler's business at all -- the gate in {@code StatisticsEvaluator.maybeMakeForFilter} owns
 * them, and handlers take no flag about them. Deephaven has a second null source that the gate says nothing about: for
 * the primitive types a null is a sentinel <i>value</i>, so a stored value equal to the sentinel reads back as null and
 * has to be found in {@code min}/{@code max} like any other value. That half is the handlers'.
 * <p>
 * Without this, a well-meaning change that taught a handler to read {@code numNulls} would look like an improvement and
 * would quietly double-count the gate above it.
 * <p>
 * The gate itself is inlined into {@code StatisticsEvaluator.maybeMakeForFilter} and has no seam to unit-test, so it is
 * covered end to end instead: {@code ParquetTableFilterTest}'s {@code nullRowsSurviveInvertedMatchStatisticsPushdown},
 * {@code nullRowsSurviveRangeFilterStatisticsPushdown}, {@code nullExcludingFiltersOverNullableColumn} and
 * {@code isNullPrunesRowGroupsProvenFreeOfNulls} all fail if it stops working. Every statistics object in this folder's
 * suites was built with {@code withNumNulls(0L)} before this test existed, which is exactly the gap the null-handling
 * defect lived in.
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
                n -> IntPushdownHandler.maybeCreateEvaluator(new IntRangeFilter("intCol", 100, 200, true, true))
                        .maybeOverlaps(intStats(1, 10, n)),
                false);
        assertIndifferentToNullCount("int range overlapping the statistics",
                n -> IntPushdownHandler.maybeCreateEvaluator(new IntRangeFilter("intCol", 5, 200, true, true))
                        .maybeOverlaps(intStats(1, 10, n)),
                true);
        assertIndifferentToNullCount("int match outside the statistics",
                n -> IntPushdownHandler.maybeCreateEvaluator(new MatchFilter(MatchOptions.REGULAR, "intCol", 50))
                        .maybeOverlaps(intStats(1, 10, n)),
                false);
    }

    @Test
    public void floatingPointHandlersIgnoreNullCount() {
        assertIndifferentToNullCount("double range entirely below the statistics",
                n -> DoublePushdownHandler
                        .maybeCreateEvaluator(new DoubleRangeFilter("doubleCol", -5.0, -1.0, true, true))
                        .maybeOverlaps(doubleStats(1.0, 10.0, n)),
                false);
        assertIndifferentToNullCount("double match inside the statistics",
                n -> DoublePushdownHandler.maybeCreateEvaluator(new MatchFilter(MatchOptions.REGULAR, "doubleCol", 5.0))
                        .maybeOverlaps(doubleStats(1.0, 10.0, n)),
                true);
    }

    @Test
    public void instantHandlerIgnoresNullCount() {
        assertIndifferentToNullCount("instant range entirely above the statistics",
                n -> InstantPushdownHandler
                        .maybeCreateEvaluator(new InstantRangeFilter("instantCol", 5_000L, 9_000L, true, true))
                        .maybeOverlaps(instantStats(1_000L, 2_000L, n)),
                false);
    }

    @Test
    public void stringHandlerIgnoresNullCount() {
        final MatchFilter outside = new MatchFilter(MatchOptions.REGULAR, "strCol", "zzz");
        outside.init(TABLE_DEFINITION);
        final StatisticsEvaluator evaluator = StringPushdownHandler.maybeCreateEvaluator(outside);
        assertNotNull(evaluator);

        assertIndifferentToNullCount("string match outside the statistics",
                n -> apply(evaluator, stringStats("aaa", "mmm", n)),
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
        assertFalse(ParquetPushdownUtils.isProvenFreeOfNulls(allNull));
    }

    /**
     * Applies {@code evaluator} to one row group, deriving whether the group is free of nulls from the statistics as
     * {@link ParquetTableLocation} does for a flat column.
     */
    private static boolean apply(final StatisticsEvaluator evaluator, final Statistics<?> stats) {
        return evaluator.maybeOverlaps(stats);
    }


    /**
     * Deephaven's <i>other</i> null source stays with the handlers, because it is an ordinary value to Parquet. For the
     * primitive types a stored value equal to the sentinel reads back as null, so it has to be found in
     * {@code min}/{@code max} -- the gate above says nothing about it.
     */
    @Test
    public void storedNullSentinelIsTheHandlersToFind() {
        final StatisticsEvaluator evaluator = IntPushdownHandler.maybeCreateEvaluator(
                new MatchFilter(MatchOptions.REGULAR, "intCol", QueryConstants.NULL_INT));

        // The sentinel is outside [10, 30], so no stored value here can read back as null.
        assertFalse(evaluator.maybeOverlaps(intStats(10, 30, 0L)));

        // The statistics reach the sentinel, so a stored value may read back as null.
        assertTrue(evaluator.maybeOverlaps(intStats(QueryConstants.NULL_INT, 30, 0L)));
    }

    /**
     * A String has no sentinel encoding -- a Deephaven null String comes only from a Parquet null -- so there is
     * nothing for the handler to find in {@code min}/{@code max} and {@code X == null} excludes outright once the gate
     * has let it through.
     */
    @Test
    public void stringHandlerHasNoSentinelToFind() {
        final MatchFilter isNull = new MatchFilter(MatchOptions.REGULAR, "strCol", new Object[] {null});
        isNull.init(TABLE_DEFINITION);
        final StatisticsEvaluator evaluator = StringPushdownHandler.maybeCreateEvaluator(isNull);
        assertNotNull(evaluator);

        for (final Long numNulls : NULL_COUNTS) {
            assertFalse("numNulls=" + numNulls, evaluator.maybeOverlaps(stringStats("aaa", "mmm", numNulls)));
        }
    }

}

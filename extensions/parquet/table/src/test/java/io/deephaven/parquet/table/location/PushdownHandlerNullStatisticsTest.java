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
import static org.junit.Assert.assertSame;
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
 * Parquet nulls are not a handler's business at all -- {@link NullAwareEvaluator} owns them, and handlers take no flag
 * about them. Deephaven has a second null source that it says nothing about: for the primitive types a null is a
 * sentinel <i>value</i>, so a stored value equal to the sentinel reads back as null and has to be found in
 * {@code min}/{@code max} like any other value. That half is the handlers'.
 * <p>
 * Without this, a well-meaning change that taught a handler to read {@code numNulls} would look like an improvement and
 * would quietly double-count the check above it.
 * <p>
 * The null-aware check is a class of its own, so {@link #nullAwareEvaluatorKeepsRowGroupsThatMayHoldNulls} pins it
 * directly; the end-to-end behaviour it produces is covered by {@code ParquetTableFilterTest}'s
 * {@code nullRowsSurviveInvertedMatchStatisticsPushdown}, {@code nullRowsSurviveRangeFilterStatisticsPushdown},
 * {@code nullExcludingFiltersOverNullableColumn} and {@code isNullPrunesRowGroupsProvenFreeOfNulls}. Every statistics
 * object in this folder's suites was built with {@code withNumNulls(0L)} before this test existed, which is exactly the
 * gap the null-handling defect lived in.
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
     * An all-null row group reports no min/max at all. It never reaches a handler, because {@link UsabilityEvaluator}
     * rejects it first -- which is also why no handler needs to cope with absent extremes.
     */
    @Test
    public void allNullStatisticsAreRejectedBeforeAnyHandler() {
        final Statistics<?> allNull = allNullStats();

        assertFalse("an all-null row group has no non-null value", allNull.hasNonNullValue());
        assertFalse("and so its statistics are unusable", ParquetPushdownUtils.areStatisticsUsable(allNull));

        // The null count is still readable, and still proves nothing about the absence of nulls.
        assertFalse(ParquetPushdownUtils.isProvenFreeOfNulls(allNull));
    }

    /**
     * The usability gate around a handler, exercised on its own. A handler that has already said "no" is the only
     * interesting case: statistics this code cannot read are no evidence, so the wrapper must override that "no", and
     * must leave it standing once they can be read.
     */
    @Test
    public void usableStatisticsEvaluatorKeepsRowGroupsItCannotRead() {
        final StatisticsEvaluator gated = new UsabilityEvaluator(StatisticsEvaluator.ALWAYS_NO_OVERLAP);

        assertTrue("unreadable statistics prove nothing, so the row group stays",
                gated.maybeOverlaps(allNullStats()));
        assertFalse("readable statistics leave the handler's verdict alone",
                gated.maybeOverlaps(intStats(1, 10, 0L)));
    }

    /**
     * The statistics-independent constants are handed back unwrapped. They read nothing, so they have no precondition
     * to enforce, and {@code ParquetTableLocation} recognizes them by identity to skip the row groups altogether --
     * which a wrapper would defeat.
     */
    @Test
    public void usableStatisticsEvaluatorLeavesTheConstantsAlone() {
        assertSame(StatisticsEvaluator.ALWAYS_MAYBE,
                UsabilityEvaluator.maybeWrap(StatisticsEvaluator.ALWAYS_MAYBE));
        assertSame(StatisticsEvaluator.ALWAYS_NO_OVERLAP,
                UsabilityEvaluator.maybeWrap(StatisticsEvaluator.ALWAYS_NO_OVERLAP));
    }

    /** An all-null row group: a null count, and no extremes at all. */
    private static Statistics<?> allNullStats() {
        return Statistics.getBuilderForReading(Types.required(INT32).named("intCol"))
                .withNumNulls(1_000L)
                .build();
    }

    /**
     * The null-aware wrapper around a handler, exercised on its own. A handler that has already said "no" is the only
     * interesting case: the wrapper must override it for a row group whose nulls the filter would have matched, and
     * must leave it alone once the statistics prove there are none to lose.
     * <p>
     * Only {@code numNulls == 0} is such a proof. A positive count plainly is not, and an <i>absent</i> count must not
     * be read as zero -- that mistake would re-open the defect this whole suite exists to pin, since the field is
     * optional and plenty of writers omit it.
     */
    @Test
    public void nullAwareEvaluatorKeepsRowGroupsThatMayHoldNulls() {
        final StatisticsEvaluator nullAware = new NullAwareEvaluator(StatisticsEvaluator.ALWAYS_NO_OVERLAP);

        assertFalse("numNulls=0 proves there is nothing to lose, so the handler's verdict stands",
                nullAware.maybeOverlaps(intStats(1, 10, 0L)));
        assertTrue("a null row may be there and may match, so the row group stays",
                nullAware.maybeOverlaps(intStats(1, 10, 1L)));
        assertTrue("and again for a larger count",
                nullAware.maybeOverlaps(intStats(1, 10, 1_000L)));
        assertTrue("an absent count proves nothing and must never be read as zero",
                nullAware.maybeOverlaps(intStats(1, 10, null)));
    }

    /**
     * The wrapper can only ever be <i>more</i> permissive than the handler inside it: it turns "no" into "maybe" for a
     * row group that may hold nulls, and never turns a "maybe" into a "no".
     */
    @Test
    public void nullAwareEvaluatorNeverExcludesWhatTheHandlerKept() {
        final StatisticsEvaluator nullAware = new NullAwareEvaluator(StatisticsEvaluator.ALWAYS_MAYBE);
        for (final Long numNulls : NULL_COUNTS) {
            assertTrue("null-aware ALWAYS_MAYBE with numNulls=" + numNulls,
                    nullAware.maybeOverlaps(intStats(1, 10, numNulls)));
        }
    }

    /**
     * Applies {@code evaluator} to one row group's statistics and nothing more. The centralized null-aware check is
     * deliberately left out: these tests exist to pin what a handler decides <i>on its own</i>, from {@code min}/
     * {@code max} alone, so they must not route through {@code StatisticsEvaluator.makeForFilter}. What
     * {@link NullAwareEvaluator} then adds on top is covered end to end by {@code ParquetTableFilterTest}.
     */
    private static boolean apply(final StatisticsEvaluator evaluator, final Statistics<?> stats) {
        return evaluator.maybeOverlaps(stats);
    }


    /**
     * Deephaven's <i>other</i> null source stays with the handlers, because it is an ordinary value to Parquet. For the
     * primitive types a stored value equal to the sentinel reads back as null, so it has to be found in
     * {@code min}/{@code max} -- {@link NullAwareEvaluator} says nothing about it.
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
     * nothing for the handler to find in {@code min}/{@code max} and {@code X == null} excludes outright once
     * {@link NullAwareEvaluator} has let it through.
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

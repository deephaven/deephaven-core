//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.MatchOptions;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.ComparableRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.qst.type.Type;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class ComparablePushdownHandlerTest {

    private static Statistics<?> dateStats(final LocalDate minInc, final LocalDate maxInc) {
        final PrimitiveType col = Types.required(INT32)
                .as(LogicalTypeAnnotation.dateType())
                .named("dateCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.intToBytes((int) minInc.toEpochDay()))
                .withMax(BytesUtils.intToBytes((int) maxInc.toEpochDay()))
                .withNumNulls(0L)
                .build();
    }

    private static Statistics<?> dateTimeStats(final LocalDateTime minInc, final LocalDateTime maxInc) {
        final PrimitiveType col = Types.required(INT64)
                .as(LogicalTypeAnnotation.timestampType(/* adjustedToUTC */ false,
                        LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("localDateTimeCol");
        final long minMillis = minInc.toInstant(ZoneOffset.UTC).toEpochMilli();
        final long maxMillis = maxInc.toInstant(ZoneOffset.UTC).toEpochMilli();
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.longToBytes(minMillis))
                .withMax(BytesUtils.longToBytes(maxMillis))
                .withNumNulls(0L)
                .build();
    }

    /**
     * Well-formed statistics that this handler cannot decode for a {@code LocalDate} column: the Parquet column is
     * {@code INT32} annotated as a plain signed int rather than a date, so
     * {@code MinMaxFromStatistics.getMinMaxForLocalDates} declines them. It carries the name of the {@code LocalDate}
     * column so a filter over that column resolves against it.
     */
    private static Statistics<?> undecodableDateStats() {
        final PrimitiveType col = Types.required(INT32)
                .as(LogicalTypeAnnotation.intType(32, /* signed */ true))
                .named("dateCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.intToBytes(0))
                .withMax(BytesUtils.intToBytes(10))
                .withNumNulls(0L)
                .build();
    }

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.of("dateCol", Type.find(LocalDate.class)),
            ColumnDefinition.of("localDateTimeCol", Type.find(LocalDateTime.class)),
            ColumnDefinition.ofString("strCol"));

    private static ComparableRangeFilter makeComparableRangeFilter(
            final String columnName, final Comparable<?> lower, final Comparable<?> upper,
            final boolean lowerInclusive, final boolean upperInclusive) {
        final ComparableRangeFilter filter = ComparableRangeFilter.makeForTest(
                columnName, lower, upper, lowerInclusive, upperInclusive);
        filter.init(TABLE_DEFINITION);
        return filter;
    }

    private static MatchFilter makeMatchFilter(
            @NotNull final MatchOptions matchOptions,
            @NotNull final String columnName,
            @NotNull final Object... values) {
        final MatchFilter filter = new MatchFilter(matchOptions, columnName, values);
        filter.init(TABLE_DEFINITION);
        return filter;
    }

    @Test
    public void localDateFilterScenarios() {
        {
            final Statistics<?> stats2020 = dateStats(
                    LocalDate.of(2020, 1, 1),
                    LocalDate.of(2020, 12, 31));

            assertTrue(evaluate(
                    makeComparableRangeFilter("dateCol",
                            LocalDate.of(2020, 3, 1),
                            LocalDate.of(2020, 6, 1), true, true),
                    stats2020));

            assertFalse(evaluate(
                    makeComparableRangeFilter("dateCol",
                            LocalDate.of(2019, 1, 1),
                            LocalDate.of(2019, 12, 31), true, true),
                    stats2020));
        }
        {
            final Statistics<?> stats = dateStats(
                    LocalDate.of(2020, 6, 1),
                    LocalDate.of(2020, 6, 30));

            assertTrue(evaluate(
                    makeMatchFilter(MatchOptions.REGULAR, "dateCol",
                            LocalDate.of(2020, 6, 15), LocalDate.of(2021, 1, 1)),
                    stats));

            assertFalse(evaluate(
                    makeMatchFilter(MatchOptions.REGULAR, "dateCol",
                            LocalDate.of(2019, 12, 31), LocalDate.of(2021, 1, 1)),
                    stats));

            assertTrue(evaluate(
                    makeMatchFilter(MatchOptions.INVERTED, "dateCol",
                            LocalDate.of(2020, 6, 15)),
                    stats));

            assertFalse(evaluate(
                    makeMatchFilter(MatchOptions.INVERTED,
                            "dateCol", LocalDate.of(2020, 6, 1)),
                    dateStats(LocalDate.of(2020, 6, 1),
                            LocalDate.of(2020, 6, 1))));

            // The same single-valued row group, excluding a date it does not hold: every row still matches.
            assertTrue(evaluate(
                    makeMatchFilter(MatchOptions.INVERTED,
                            "dateCol", LocalDate.of(2020, 6, 2)),
                    dateStats(LocalDate.of(2020, 6, 1),
                            LocalDate.of(2020, 6, 1))));
        }
    }

    @Test
    public void localDateTimeFilterScenarios() {
        {
            final LocalDateTime dtStart = LocalDateTime.of(2021, 3, 1, 0, 0);
            final LocalDateTime dtEnd = LocalDateTime.of(2021, 3, 31, 23, 59, 59);
            final Statistics<?> statsMarch = dateTimeStats(dtStart, dtEnd);

            assertTrue(evaluate(
                    makeComparableRangeFilter("localDateTimeCol",
                            LocalDateTime.of(2021, 3, 10, 0, 0),
                            LocalDateTime.of(2021, 3, 20, 0, 0), true, true),
                    statsMarch));

            assertFalse(evaluate(
                    makeComparableRangeFilter("localDateTimeCol",
                            LocalDateTime.of(2021, 2, 1, 0, 0),
                            LocalDateTime.of(2021, 2, 28, 23, 59, 59), true, true),
                    statsMarch));
        }
        {
            final Statistics<?> stats = dateTimeStats(
                    LocalDateTime.of(2022, 1, 1, 0, 0),
                    LocalDateTime.of(2022, 1, 1, 12, 0));

            assertTrue(evaluate(
                    makeMatchFilter(MatchOptions.REGULAR, "localDateTimeCol",
                            LocalDateTime.of(2022, 1, 1, 6, 0)),
                    stats));

            assertFalse(evaluate(
                    makeMatchFilter(MatchOptions.REGULAR, "localDateTimeCol",
                            LocalDateTime.of(2021, 12, 31, 23, 59)),
                    stats));

            // single-point stats excluded
            assertFalse(evaluate(
                    makeMatchFilter(MatchOptions.INVERTED, "localDateTimeCol",
                            LocalDateTime.of(2022, 1, 1, 0, 0)),
                    dateTimeStats(LocalDateTime.of(2022, 1, 1, 0, 0),
                            LocalDateTime.of(2022, 1, 1, 0, 0))));

            // exclusion miss
            assertTrue(evaluate(
                    makeMatchFilter(MatchOptions.INVERTED, "localDateTimeCol",
                            LocalDateTime.of(2021, 12, 31, 23, 59)),
                    stats));
        }
    }

    /**
     * The {@code WhereFilter} entry point decides serving from the column type alone: a type
     * {@link MinMaxFromStatistics#canDecodeComparable} can decode is claimed, anything else is declined with
     * {@code null} so the dispatcher can offer it to another handler. {@link String} is the deliberate exclusion --
     * Parquet orders those statistics by unsigned bytes, and {@link StringPushdownHandler} owns them.
     */
    @Test
    public void entryPointServesDecodableColumnTypesOnly() {
        final WhereFilter dateRange = makeComparableRangeFilter("dateCol",
                LocalDate.of(2020, 1, 1), LocalDate.of(2020, 12, 31), true, true);
        assertNotNull(ComparablePushdownHandler.maybeCreateEvaluator(dateRange));

        final WhereFilter dateMatch = makeMatchFilter(MatchOptions.REGULAR, "dateCol", LocalDate.of(2020, 6, 1));
        assertNotNull(ComparablePushdownHandler.maybeCreateEvaluator(dateMatch));

        final WhereFilter stringRange = makeComparableRangeFilter("strCol", "aaa", "zzz", true, true);
        assertNull(ComparablePushdownHandler.maybeCreateEvaluator(stringRange));

        final WhereFilter stringMatch = makeMatchFilter(MatchOptions.REGULAR, "strCol", "aaa");
        assertNull(ComparablePushdownHandler.maybeCreateEvaluator(stringMatch));
    }

    /**
     * A null range bound is not reachable from a parsed comparison and has no agreed meaning here, so it is answered
     * once rather than per row group.
     */
    @Test
    public void nullRangeBoundsDisablePushdown() {
        assertSame(StatisticsEvaluator.ALWAYS_MAYBE, ComparablePushdownHandler.maybeCreateEvaluator(
                makeComparableRangeFilter("dateCol", null, LocalDate.of(2020, 1, 1), true, true)));
        assertSame(StatisticsEvaluator.ALWAYS_MAYBE, ComparablePushdownHandler.maybeCreateEvaluator(
                makeComparableRangeFilter("dateCol", LocalDate.of(2020, 1, 1), null, true, true)));
    }

    /**
     * Match filter shapes whose answer does not depend on any row group's statistics, resolved once at creation time so
     * the caller can skip the row groups entirely.
     */
    @Test
    public void matchFilterShapesResolvedWithoutStatistics() {
        // No values at all: a regular match can find nothing, an inverted one matches every row.
        assertSame(StatisticsEvaluator.ALWAYS_NO_OVERLAP,
                ComparablePushdownHandler.maybeCreateEvaluator(makeMatchFilter(MatchOptions.REGULAR, "dateCol")));
        assertSame(StatisticsEvaluator.ALWAYS_MAYBE,
                ComparablePushdownHandler.maybeCreateEvaluator(makeMatchFilter(MatchOptions.INVERTED, "dateCol")));

        // Nothing but null, once nulls are dropped. `X == null` says nothing about min/max -- the null-aware check in
        // NullAwareEvaluator answers for those rows -- while `X != null` matches any non-null value.
        assertSame(StatisticsEvaluator.ALWAYS_NO_OVERLAP, ComparablePushdownHandler.maybeCreateEvaluator(
                makeMatchFilter(MatchOptions.REGULAR, "dateCol", (Object) null)));
        assertSame(StatisticsEvaluator.ALWAYS_MAYBE, ComparablePushdownHandler.maybeCreateEvaluator(
                makeMatchFilter(MatchOptions.INVERTED, "dateCol", (Object) null)));

        // A value that is not Comparable has no place in the ordering, so it cannot be tested against min/max at all.
        assertSame(StatisticsEvaluator.ALWAYS_MAYBE, ComparablePushdownHandler.maybeCreateEvaluator(
                makeMatchFilter(MatchOptions.REGULAR, "dateCol", new Object())));
    }

    /**
     * Statistics this handler cannot decode are no evidence about the row group, so every evaluator shape keeps it.
     */
    @Test
    public void undecodableStatisticsKeepTheRowGroup() {
        final Statistics<?> undecodable = undecodableDateStats();

        assertTrue(evaluate(makeComparableRangeFilter("dateCol",
                LocalDate.of(2020, 1, 1), LocalDate.of(2020, 12, 31), true, true), undecodable));
        assertTrue(evaluate(
                makeMatchFilter(MatchOptions.REGULAR, "dateCol", LocalDate.of(2020, 6, 1)), undecodable));
        assertTrue(evaluate(
                makeMatchFilter(MatchOptions.INVERTED, "dateCol", LocalDate.of(2020, 6, 1)), undecodable));
    }

    /** Equal bounds held exclusively describe an empty interval, which no row group can intersect. */
    @Test
    public void emptyFilterRangeExcludesEveryRowGroup() {
        final Statistics<?> stats = dateStats(LocalDate.of(2020, 1, 1), LocalDate.of(2020, 12, 31));
        final LocalDate midYear = LocalDate.of(2020, 6, 1);

        assertFalse(evaluate(makeComparableRangeFilter("dateCol", midYear, midYear, false, false), stats));
        // The same bounds held inclusively are the single point, which this row group does contain.
        assertTrue(evaluate(makeComparableRangeFilter("dateCol", midYear, midYear, true, true), stats));
    }

    /**
     * A filter that never reached {@code init} has no column type, and the handler refuses to guess one rather than
     * decode the statistics as some default.
     */
    @Test
    public void uninitializedFilterIsRejected() {
        final ComparableRangeFilter uninitializedRange = ComparableRangeFilter.makeForTest(
                "dateCol", LocalDate.of(2020, 1, 1), LocalDate.of(2020, 12, 31), true, true);
        try {
            ComparablePushdownHandler.maybeCreateEvaluator(uninitializedRange);
            fail("expected an IllegalStateException for a filter with no column type");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("not initialized with a column type"));
        }

        final MatchFilter uninitializedMatch =
                new MatchFilter(MatchOptions.REGULAR, "dateCol", LocalDate.of(2020, 6, 1));
        try {
            ComparablePushdownHandler.maybeCreateEvaluator(uninitializedMatch);
            fail("expected an IllegalStateException for a filter with no column type");
        } catch (final IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("not initialized with a column type"));
        }
    }

    /**
     * Resolves the filter to an evaluator and applies it to one row group's statistics, as
     * {@code StatisticsEvaluator.makeForFilter} does per location.
     */
    private static boolean evaluate(final ComparableRangeFilter filter, final Statistics<?> stats) {
        return ComparablePushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

    private static boolean evaluate(final MatchFilter filter, final Statistics<?> stats) {
        return ComparablePushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

}

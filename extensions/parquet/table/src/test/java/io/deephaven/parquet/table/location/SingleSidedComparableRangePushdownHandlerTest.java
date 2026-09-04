//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.SingleSidedComparableRangeFilter;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.qst.type.Type;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.*;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class SingleSidedComparableRangePushdownHandlerTest {

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
                .as(LogicalTypeAnnotation.timestampType(
                        /* adjustedToUTC = */ false,
                        LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("ldtCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.longToBytes(minInc.toInstant(ZoneOffset.UTC).toEpochMilli()))
                .withMax(BytesUtils.longToBytes(maxInc.toInstant(ZoneOffset.UTC).toEpochMilli()))
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

    private static final TableDefinition TABLE_DEF = TableDefinition.of(
            ColumnDefinition.of("dateCol", Type.find(LocalDate.class)),
            ColumnDefinition.of("ldtCol", Type.find(LocalDateTime.class)),
            ColumnDefinition.ofString("strCol"));

    private static SingleSidedComparableRangeFilter ssFilter(
            final String column,
            final Comparable<?> pivot,
            final boolean inclusive,
            final boolean isGreaterThan) {
        final SingleSidedComparableRangeFilter sscrf =
                SingleSidedComparableRangeFilter.makeForTest(column, pivot, inclusive, isGreaterThan);
        sscrf.init(TABLE_DEF);
        return sscrf;
    }

    // String columns are served by StringPushdownHandler; see StringPushdownHandlerTest.

    @Test
    public void dateGreaterThanScenarios() {
        final Statistics<?> stats = dateStats(
                LocalDate.of(2020, 1, 1),
                LocalDate.of(2020, 12, 31));

        assertTrue(evaluate(
                ssFilter("dateCol", LocalDate.of(2020, 6, 15), true, true), stats));
        assertFalse(evaluate(
                ssFilter("dateCol", LocalDate.of(2020, 12, 31), false, true), stats));
        assertFalse(evaluate(
                ssFilter("dateCol", LocalDate.of(2021, 1, 1), true, true), stats));
    }

    @Test
    public void dateTimeGreaterThanScenarios() {
        final Statistics<?> stats = dateTimeStats(
                LocalDateTime.of(2022, 1, 1, 0, 0),
                LocalDateTime.of(2022, 1, 1, 12, 0));

        assertTrue(evaluate(
                ssFilter("ldtCol", LocalDateTime.of(2022, 1, 1, 6, 0), true, true), stats));
        assertFalse(evaluate(
                ssFilter("ldtCol", LocalDateTime.of(2022, 1, 1, 12, 0), false, true), stats));
    }

    @Test
    public void nullPivotDisablesPushdown() {
        final Statistics<?> stats = dateStats(LocalDate.of(2020, 1, 1), LocalDate.of(2020, 12, 31));

        assertTrue(evaluate(
                ssFilter("dateCol", null, true, true), stats));
    }

    /**
     * {@code X < v} matches null rows, because Deephaven orders null below every value. That used to be reason enough
     * to decline it here; the null guard in {@code pushdownRowGroupMetadata} now accounts for those rows centrally, so
     * the comparison is evaluated.
     */
    @Test
    public void lessThanFiltersAreEvaluated() {
        final Statistics<?> stats = dateStats(LocalDate.of(2020, 6, 1), LocalDate.of(2020, 12, 31));

        // Nothing in this row group precedes 2020-01-01.
        assertFalse(evaluate(
                ssFilter("dateCol", LocalDate.of(2020, 1, 1), true, false), stats));

        // But plenty precedes 2020-09-01.
        assertTrue(evaluate(
                ssFilter("dateCol", LocalDate.of(2020, 9, 1), true, false), stats));

        // Boundary at the minimum itself.
        assertTrue(evaluate(
                ssFilter("dateCol", LocalDate.of(2020, 6, 1), true, false), stats));
        assertFalse(evaluate(
                ssFilter("dateCol", LocalDate.of(2020, 6, 1), false, false), stats));
    }

    @Test
    public void dateTimeLessThanScenarios() {
        final Statistics<?> stats = dateTimeStats(
                LocalDateTime.of(2022, 1, 1, 0, 0),
                LocalDateTime.of(2022, 1, 1, 12, 0));

        // Nothing in this row group precedes its own minimum, so both of these exclude it. Both assertions were
        // previously assertTrue, pinning the blanket decline that less-than filters used to receive.
        assertFalse(evaluate(
                ssFilter("ldtCol", LocalDateTime.of(2021, 12, 31, 23, 59), true, false), stats));
        assertFalse(evaluate(
                ssFilter("ldtCol", LocalDateTime.of(2022, 1, 1, 0, 0), false, false), stats));

        // Inclusive of the minimum, and above it, there is something to find.
        assertTrue(evaluate(
                ssFilter("ldtCol", LocalDateTime.of(2022, 1, 1, 0, 0), true, false), stats));
        assertTrue(evaluate(
                ssFilter("ldtCol", LocalDateTime.of(2022, 1, 1, 6, 0), true, false), stats));
    }

    /**
     * The {@code WhereFilter} entry point, which the scenarios above bypass by calling the typed overload directly. A
     * single-sided comparison over a column type {@link MinMaxFromStatistics#canDecodeComparable} can decode is
     * claimed; a String column is declined with {@code null}, since {@link StringPushdownHandler} -- offered the filter
     * first -- compares those byte-ordered statistics as bytes.
     */
    @Test
    public void entryPointServesDecodableColumnTypesOnly() {
        final WhereFilter dateComparison = ssFilter("dateCol", LocalDate.of(2020, 6, 1), true, true);
        assertNotNull(SingleSidedComparableRangePushdownHandler.maybeCreateEvaluator(dateComparison));

        final WhereFilter dateTimeComparison = ssFilter("ldtCol", LocalDateTime.of(2022, 1, 1, 6, 0), true, false);
        assertNotNull(SingleSidedComparableRangePushdownHandler.maybeCreateEvaluator(dateTimeComparison));

        final WhereFilter stringComparison = ssFilter("strCol", "mmm", true, true);
        assertNull(SingleSidedComparableRangePushdownHandler.maybeCreateEvaluator(stringComparison));
    }

    /**
     * Statistics this handler cannot decode are no evidence about the row group, so it is kept in either direction.
     */
    @Test
    public void undecodableStatisticsKeepTheRowGroup() {
        final Statistics<?> undecodable = undecodableDateStats();

        assertTrue(evaluate(ssFilter("dateCol", LocalDate.of(2020, 6, 1), true, true), undecodable));
        assertTrue(evaluate(ssFilter("dateCol", LocalDate.of(2020, 6, 1), true, false), undecodable));
    }

    /**
     * Resolves the filter to an evaluator and applies it to one row group's statistics, as
     * {@code StatisticsEvaluator.makeForFilter} does per location.
     */
    private static boolean evaluate(final SingleSidedComparableRangeFilter filter, final Statistics<?> stats) {
        return SingleSidedComparableRangePushdownHandler.maybeCreateEvaluator(filter).maybeOverlaps(stats);
    }

}

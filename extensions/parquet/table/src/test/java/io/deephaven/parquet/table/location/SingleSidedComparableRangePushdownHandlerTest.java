//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.SingleSidedComparableRangeFilter;
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

    private static final TableDefinition TABLE_DEF = TableDefinition.of(
            ColumnDefinition.of("dateCol", Type.find(LocalDate.class)),
            ColumnDefinition.of("ldtCol", Type.find(LocalDateTime.class)));

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

        assertTrue(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("dateCol", LocalDate.of(2020, 6, 15), true, true), stats));
        assertFalse(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("dateCol", LocalDate.of(2020, 12, 31), false, true), stats));
        assertFalse(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("dateCol", LocalDate.of(2021, 1, 1), true, true), stats));
    }

    @Test
    public void dateTimeGreaterThanScenarios() {
        final Statistics<?> stats = dateTimeStats(
                LocalDateTime.of(2022, 1, 1, 0, 0),
                LocalDateTime.of(2022, 1, 1, 12, 0));

        assertTrue(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("ldtCol", LocalDateTime.of(2022, 1, 1, 6, 0), true, true), stats));
        assertFalse(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("ldtCol", LocalDateTime.of(2022, 1, 1, 12, 0), false, true), stats));
    }

    @Test
    public void nullPivotDisablesPushdown() {
        final Statistics<?> stats = dateStats(LocalDate.of(2020, 1, 1), LocalDate.of(2020, 12, 31));

        assertTrue(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
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
        assertFalse(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("dateCol", LocalDate.of(2020, 1, 1), true, false), stats));

        // But plenty precedes 2020-09-01.
        assertTrue(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("dateCol", LocalDate.of(2020, 9, 1), true, false), stats));

        // Boundary at the minimum itself.
        assertTrue(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("dateCol", LocalDate.of(2020, 6, 1), true, false), stats));
        assertFalse(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("dateCol", LocalDate.of(2020, 6, 1), false, false), stats));
    }

    @Test
    public void dateTimeLessThanScenarios() {
        final Statistics<?> stats = dateTimeStats(
                LocalDateTime.of(2022, 1, 1, 0, 0),
                LocalDateTime.of(2022, 1, 1, 12, 0));

        // Nothing in this row group precedes its own minimum, so both of these exclude it. Both assertions were
        // previously assertTrue, pinning the blanket decline that less-than filters used to receive.
        assertFalse(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("ldtCol", LocalDateTime.of(2021, 12, 31, 23, 59), true, false), stats));
        assertFalse(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("ldtCol", LocalDateTime.of(2022, 1, 1, 0, 0), false, false), stats));

        // Inclusive of the minimum, and above it, there is something to find.
        assertTrue(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("ldtCol", LocalDateTime.of(2022, 1, 1, 0, 0), true, false), stats));
        assertTrue(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("ldtCol", LocalDateTime.of(2022, 1, 1, 6, 0), true, false), stats));
    }
}

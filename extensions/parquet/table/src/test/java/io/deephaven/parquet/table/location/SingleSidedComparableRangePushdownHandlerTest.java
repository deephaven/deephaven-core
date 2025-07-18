//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
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

    private static Statistics<?> stringStats(final String minInc, final String maxInc) {
        final PrimitiveType col = Types.required(BINARY)
                .as(LogicalTypeAnnotation.stringType())
                .named("strCol");
        return Statistics.getBuilderForReading(col)
                .withMin(minInc.getBytes(StandardCharsets.UTF_8))
                .withMax(maxInc.getBytes(StandardCharsets.UTF_8))
                .withNumNulls(0L)
                .build();
    }

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
            ColumnDefinition.ofString("strCol"),
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

    @Test
    public void stringGreaterThanScenarios() {
        final Statistics<?> stats = stringStats("alpha", "omega");

        // inside
        assertTrue(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("strCol", "beta", true, true), stats));

        // at max, inclusive
        assertTrue(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("strCol", "omega", true, true), stats));

        // at max, exclusive
        assertFalse(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("strCol", "omega", false, true), stats));

        // above max, inclusive
        assertFalse(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("strCol", "zzzz", true, true), stats));

        // below min
        assertTrue(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("strCol", "aardvark", true, true), stats));
    }

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
        final Statistics<?> stats = stringStats("a", "b");

        assertTrue(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("strCol", null, true, true), stats));
    }

    @Test
    public void lessThanFiltersSkipPushdown() {
        final Statistics<?> stats = stringStats("m", "z");

        assertTrue(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("strCol", "q", true, false), stats));
    }

    @Test
    public void dateTimeLessThanScenarios() {
        final Statistics<?> stats = dateTimeStats(
                LocalDateTime.of(2022, 1, 1, 0, 0),
                LocalDateTime.of(2022, 1, 1, 12, 0));

        // always return true for greater than filters
        assertTrue(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("ldtCol", LocalDateTime.of(2021, 12, 31, 23, 59), true, false), stats));

        // always return true for less than filters
        assertTrue(SingleSidedComparableRangePushdownHandler.maybeOverlaps(
                ssFilter("ldtCol", LocalDateTime.of(2022, 1, 1, 0, 0), false, false), stats));
    }
}

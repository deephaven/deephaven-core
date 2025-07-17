//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.select.ComparableRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
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

    private static final TableDefinition TABLE_DEFINITION = TableDefinition.of(
            ColumnDefinition.ofString("strCol"),
            ColumnDefinition.of("dateCol", Type.find(LocalDate.class)),
            ColumnDefinition.of("localDateTimeCol", Type.find(LocalDateTime.class)));

    private static ComparableRangeFilter makeComparableRangeFilter(
            final String columnName, final Comparable<?> lower, final Comparable<?> upper,
            final boolean lowerInclusive, final boolean upperInclusive) {
        final ComparableRangeFilter filter = ComparableRangeFilter.makeForTest(
                columnName, lower, upper, lowerInclusive, upperInclusive);
        filter.init(TABLE_DEFINITION);
        return filter;
    }

    private static MatchFilter makeMatchFilter(
            @NotNull final MatchFilter.MatchType matchType,
            @NotNull final String columnName,
            @NotNull final Object... values) {
        final MatchFilter filter = new MatchFilter(matchType, columnName, values);
        filter.init(TABLE_DEFINITION);
        return filter;
    }

    @Test
    public void rangeFilterScenarios() {
        final Statistics<?> statsAZ = stringStats("aaa", "zzz");

        // range wholly inside
        assertTrue(ComparablePushdownHandler.maybeOverlaps(
                makeComparableRangeFilter("strCol", "bbb", "yyy", true, true), statsAZ));

        // filter equal to statistics inclusive
        assertTrue(ComparablePushdownHandler.maybeOverlaps(
                makeComparableRangeFilter("strCol", "aaa", "zzz", true, true), statsAZ));

        // half-open overlaps
        assertFalse(ComparablePushdownHandler.maybeOverlaps(
                makeComparableRangeFilter("strCol", "aaa", "aaa", false, false), statsAZ));

        // disjoint below
        assertFalse(ComparablePushdownHandler.maybeOverlaps(
                makeComparableRangeFilter("strCol", "000", "111", true, true), statsAZ));

        // disjoint above
        assertFalse(ComparablePushdownHandler.maybeOverlaps(
                makeComparableRangeFilter("strCol", "~~~", "zz{", true, true), statsAZ));

        // swapped bounds
        assertTrue(ComparablePushdownHandler.maybeOverlaps(
                makeComparableRangeFilter("strCol", "yyy", "bbb", true, true), statsAZ));

        // null disables push‑down
        assertTrue(ComparablePushdownHandler.maybeOverlaps(
                makeComparableRangeFilter("strCol", null, "ccc", true, true), statsAZ));

        // Overlapping ('aaa', 'aaa'] with stats ['aaa', 'bbb'] should return false
        assertFalse(ComparablePushdownHandler.maybeOverlaps(
                makeComparableRangeFilter("strCol", "aaa", "aaa", false, true), stringStats("aaa", "bbb")));
    }

    @Test
    public void regularMatchFilterScenarios() {
        final Statistics<?> stats = stringStats("alpha", "omega");

        // hit
        assertTrue(ComparablePushdownHandler.maybeOverlaps(
                makeMatchFilter(MatchFilter.MatchType.Regular, "strCol", "Foo", "beta", "OMEGA"), stats));

        // all below
        assertFalse(ComparablePushdownHandler.maybeOverlaps(
                makeMatchFilter(MatchFilter.MatchType.Regular, "strCol", "000", "abc"), stats));

        // all above
        assertFalse(ComparablePushdownHandler.maybeOverlaps(
                makeMatchFilter(MatchFilter.MatchType.Regular, "strCol", "zzz", "zzz1"), stats));

        // empty list
        assertFalse(ComparablePushdownHandler.maybeOverlaps(
                makeMatchFilter(MatchFilter.MatchType.Regular, "strCol"), stats));

        // null entry
        assertTrue(ComparablePushdownHandler.maybeOverlaps(
                makeMatchFilter(MatchFilter.MatchType.Regular, "strCol", "mu", null), stats));
    }

    @Test
    public void invertedMatchFilterScenarios() {
        // stats alpha..delta ; NOT IN {beta}
        assertTrue(ComparablePushdownHandler.maybeOverlaps(
                makeMatchFilter(MatchFilter.MatchType.Inverted, "strCol", "beta"),
                stringStats("alpha", "delta")));

        // single‑point stats excluded
        assertFalse(ComparablePushdownHandler.maybeOverlaps(
                makeMatchFilter(MatchFilter.MatchType.Inverted, "strCol", "gamma"),
                stringStats("gamma", "gamma")));

        // single‑point stats, exclusion miss
        assertTrue(ComparablePushdownHandler.maybeOverlaps(
                makeMatchFilter(MatchFilter.MatchType.Inverted, "strCol", "theta"),
                stringStats("gamma", "gamma")));

        // stats span equals two exclusion points
        assertTrue(ComparablePushdownHandler.maybeOverlaps(
                makeMatchFilter(MatchFilter.MatchType.Inverted, "strCol", "bar", "baz"),
                stringStats("bar", "baz")));

        // empty exclusion list
        assertTrue(ComparablePushdownHandler.maybeOverlaps(
                makeMatchFilter(MatchFilter.MatchType.Inverted, "strCol"),
                stringStats("a", "b")));

        // null in the exclusion list
        assertTrue(ComparablePushdownHandler.maybeOverlaps(
                makeMatchFilter(MatchFilter.MatchType.Inverted, "strCol", null),
                stringStats("x", "y")));

        // exclusion list with a value that is in the stats range
        assertTrue(ComparablePushdownHandler.maybeOverlaps(
                makeMatchFilter(MatchFilter.MatchType.Inverted, "strCol", "x", "y"),
                stringStats("x", "y")));
    }

    @Test
    public void localDateFilterScenarios() {
        {
            final Statistics<?> stats2020 = dateStats(
                    LocalDate.of(2020, 1, 1),
                    LocalDate.of(2020, 12, 31));

            assertTrue(ComparablePushdownHandler.maybeOverlaps(
                    makeComparableRangeFilter("dateCol",
                            LocalDate.of(2020, 3, 1),
                            LocalDate.of(2020, 6, 1), true, true),
                    stats2020));

            assertFalse(ComparablePushdownHandler.maybeOverlaps(
                    makeComparableRangeFilter("dateCol",
                            LocalDate.of(2019, 1, 1),
                            LocalDate.of(2019, 12, 31), true, true),
                    stats2020));
        }
        {
            final Statistics<?> stats = dateStats(
                    LocalDate.of(2020, 6, 1),
                    LocalDate.of(2020, 6, 30));

            assertTrue(ComparablePushdownHandler.maybeOverlaps(
                    makeMatchFilter(MatchFilter.MatchType.Regular, "dateCol",
                            LocalDate.of(2020, 6, 15), LocalDate.of(2021, 1, 1)),
                    stats));

            assertFalse(ComparablePushdownHandler.maybeOverlaps(
                    makeMatchFilter(MatchFilter.MatchType.Regular, "dateCol",
                            LocalDate.of(2019, 12, 31), LocalDate.of(2021, 1, 1)),
                    stats));

            assertTrue(ComparablePushdownHandler.maybeOverlaps(
                    makeMatchFilter(MatchFilter.MatchType.Inverted, "dateCol",
                            LocalDate.of(2020, 6, 15)),
                    stats));

            assertFalse(ComparablePushdownHandler.maybeOverlaps(
                    makeMatchFilter(MatchFilter.MatchType.Inverted,
                            "dateCol", LocalDate.of(2020, 6, 1)),
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

            assertTrue(ComparablePushdownHandler.maybeOverlaps(
                    makeComparableRangeFilter("localDateTimeCol",
                            LocalDateTime.of(2021, 3, 10, 0, 0),
                            LocalDateTime.of(2021, 3, 20, 0, 0), true, true),
                    statsMarch));

            assertFalse(ComparablePushdownHandler.maybeOverlaps(
                    makeComparableRangeFilter("localDateTimeCol",
                            LocalDateTime.of(2021, 2, 1, 0, 0),
                            LocalDateTime.of(2021, 2, 28, 23, 59, 59), true, true),
                    statsMarch));
        }
        {
            final Statistics<?> stats = dateTimeStats(
                    LocalDateTime.of(2022, 1, 1, 0, 0),
                    LocalDateTime.of(2022, 1, 1, 12, 0));

            assertTrue(ComparablePushdownHandler.maybeOverlaps(
                    makeMatchFilter(MatchFilter.MatchType.Regular, "localDateTimeCol",
                            LocalDateTime.of(2022, 1, 1, 6, 0)),
                    stats));

            assertFalse(ComparablePushdownHandler.maybeOverlaps(
                    makeMatchFilter(MatchFilter.MatchType.Regular, "localDateTimeCol",
                            LocalDateTime.of(2021, 12, 31, 23, 59)),
                    stats));

            // single‑point stats excluded
            assertFalse(ComparablePushdownHandler.maybeOverlaps(
                    makeMatchFilter(MatchFilter.MatchType.Inverted, "localDateTimeCol",
                            LocalDateTime.of(2022, 1, 1, 0, 0)),
                    dateTimeStats(LocalDateTime.of(2022, 1, 1, 0, 0),
                            LocalDateTime.of(2022, 1, 1, 0, 0))));

            // exclusion miss
            assertTrue(ComparablePushdownHandler.maybeOverlaps(
                    makeMatchFilter(MatchFilter.MatchType.Inverted, "localDateTimeCol",
                            LocalDateTime.of(2021, 12, 31, 23, 59)),
                    stats));
        }
    }
}

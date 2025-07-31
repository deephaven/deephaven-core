//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.InstantRangeFilter;
import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.QueryConstants;
import org.apache.parquet.bytes.BytesUtils;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Instant;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class InstantPushdownHandlerTest {

    private static Statistics<?> instantStatsMillis(Instant minInc, Instant maxInc) {
        final PrimitiveType col = Types.required(INT64)
                .as(LogicalTypeAnnotation.timestampType(/* adjustedToUTC */ true,
                        LogicalTypeAnnotation.TimeUnit.MILLIS))
                .named("instCol");
        return Statistics.getBuilderForReading(col)
                .withMin(BytesUtils.longToBytes(minInc.toEpochMilli()))
                .withMax(BytesUtils.longToBytes(maxInc.toEpochMilli()))
                .withNumNulls(0L)
                .build();
    }

    @Test
    public void instantRangeFilterScenarios() {
        final Statistics<?> stats = instantStatsMillis(
                Instant.ofEpochMilli(0L), // 0 ms
                Instant.ofEpochMilli(50L)); // 50 ms

        // wholly inside
        assertTrue(InstantPushdownHandler.maybeOverlaps(
                new InstantRangeFilter("t",
                        1_000_000L, 2_000_000L, true, true), // 1–2 ms
                stats));

        // matches lower edge inclusive vs exclusive
        assertTrue(InstantPushdownHandler.maybeOverlaps(
                new InstantRangeFilter("t",
                        0L, 0L, true, true),
                stats));
        assertFalse(InstantPushdownHandler.maybeOverlaps(
                new InstantRangeFilter("t",
                        0L, 0L, false, false),
                stats));

        // disjoint before / after
        assertFalse(InstantPushdownHandler.maybeOverlaps(
                new InstantRangeFilter("t",
                        -20_000_000L, -10_000_000L, true, true), // -20 to -10 ms
                stats));
        assertFalse(InstantPushdownHandler.maybeOverlaps(
                new InstantRangeFilter("t",
                        60_000_000L, 70_000_000L, true, true),
                stats)); // 60–70 ms

        // constructor reversal still overlaps
        assertTrue(InstantPushdownHandler.maybeOverlaps(
                new InstantRangeFilter("t",
                        40_000_000L, 10_000_000L, true, true), // reversed 40 ms / 10 ms
                stats));

        // NULL bound disables push-down
        assertTrue(InstantPushdownHandler.maybeOverlaps(
                new InstantRangeFilter("t",
                        QueryConstants.NULL_LONG, 2_000_000L, true, true),
                stats));
    }

    @Test
    public void instantMatchFilterScenarios() {
        final Statistics<?> stats = instantStatsMillis(
                Instant.ofEpochMilli(1), // 1 ms
                Instant.ofEpochMilli(10)); // 10 ms

        // at least one in range
        assertTrue(InstantPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "t",
                        Instant.ofEpochMilli(2), // inside
                        Instant.ofEpochMilli(20)), // outside
                stats));

        // all values outside
        assertFalse(InstantPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "t",
                        Instant.ofEpochMilli(20), Instant.ofEpochMilli(30)),
                stats));

        // non-Instant value short-circuits to true
        assertTrue(InstantPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "t",
                        "not-an-instant"),
                stats));

        // empty list
        assertFalse(InstantPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "t"), stats));

        // list containing null
        assertTrue(InstantPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "t",
                        Instant.ofEpochMilli(2), null),
                stats));
    }

    @Test
    public void instantInvertMatchFilterScenarios() {
        // stats 0..100 ms; NOT IN {50 ms} leaves gaps
        assertTrue(InstantPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "t",
                        Instant.ofEpochMilli(50)),
                instantStatsMillis(Instant.ofEpochMilli(0L),
                        Instant.ofEpochMilli(100))));

        // single-point stats excluded
        assertFalse(InstantPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "t",
                        Instant.ofEpochMilli(25)),
                instantStatsMillis(Instant.ofEpochMilli(25),
                        Instant.ofEpochMilli(25))));

        // single-point stats, exclusion miss
        assertTrue(InstantPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "t",
                        Instant.ofEpochMilli(26L)),
                instantStatsMillis(Instant.ofEpochMilli(25L),
                        Instant.ofEpochMilli(25L))));

        // null in the exclusion list disables push-down
        assertTrue(InstantPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "t",
                        (Object) null),
                instantStatsMillis(Instant.ofEpochMilli(10L), Instant.ofEpochMilli(20L))));
    }
}

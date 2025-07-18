//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.table.location;

import io.deephaven.engine.table.impl.select.MatchFilter;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.nio.charset.StandardCharsets;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.junit.Assert.*;

@Category(OutOfBandTest.class)
public class CaseInsensitiveStringMatchPushdownHandlerTest {

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

    @Test
    public void regularMatchScenarios() {
        final Statistics<?> stats = stringStats("aAa", "zZz");

        // at least one value inside (case-mixed)
        assertTrue(CaseInsensitiveStringMatchPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "s", "FOO", "BaR", "mMm"),
                stats));

        // all values below range
        assertFalse(CaseInsensitiveStringMatchPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "s", "000", "99"),
                stats));

        // all values above range
        assertFalse(CaseInsensitiveStringMatchPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "s", "~~~", "zz{"),
                stats));

        // value just below min boundary
        assertFalse(CaseInsensitiveStringMatchPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "s", "aa`"),
                stats));

        // empty list
        assertFalse(CaseInsensitiveStringMatchPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular, "s"), stats));

        // list containing null
        assertTrue(CaseInsensitiveStringMatchPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Regular,
                        "s", "xyz", null),
                stats));
    }

    @Test
    public void invertedMatchScenarios() {
        // stats aaa..hhh ; NOT IN {ccc,DDD} leaves gaps
        assertTrue(CaseInsensitiveStringMatchPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted,
                        "s", "ccc", "DDD"),
                stringStats("aaa", "hhh")));

        // stats foo..foo; exclude foo (case-varied), so no gap
        assertFalse(CaseInsensitiveStringMatchPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted,
                        "s", "FoO"),
                stringStats("foo", "fOo")));

        // stats foo..foo ; exclude different value, so gap exists
        assertTrue(CaseInsensitiveStringMatchPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted,
                        "s", "bar"),
                stringStats("foo", "foo")));

        // stats bar..baz; NOT IN {bar,baz} still leaves an internal gap ("bar" < x < "baz")
        assertTrue(CaseInsensitiveStringMatchPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted,
                        "s", "BAR", "baz"),
                stringStats("bar", "Baz")));

        // empty exclusion list
        assertTrue(CaseInsensitiveStringMatchPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted, "s"),
                stringStats("a", "b")));

        // null in the exclusion list
        assertTrue(CaseInsensitiveStringMatchPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted,
                        "s", null),
                stringStats("x", "y")));

        // exclusion list with a value that is in the stats range
        assertTrue(CaseInsensitiveStringMatchPushdownHandler.maybeOverlaps(
                new MatchFilter(MatchFilter.MatchType.Inverted,
                        "s", "x", "y"),
                stringStats("x", "y")));
    }
}

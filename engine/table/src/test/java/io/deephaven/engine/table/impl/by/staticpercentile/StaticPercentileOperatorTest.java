//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.staticpercentile;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.TableTools;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;

import static io.deephaven.api.agg.Aggregation.AggMed;
import static io.deephaven.api.agg.Aggregation.AggPct;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;

/**
 * Compares static percentile results, which use {@link StaticPercentileOperator}, against the same aggregations of a
 * refreshing copy of the table, which use the segmented sorted multiset operator.
 */
public class StaticPercentileOperatorTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private static final String[] VALUE_COLUMNS =
            {"C", "By", "S", "I", "L", "F", "D", "B", "Scaled", "Bo", "T", "Str"};

    private static List<Aggregation> aggregations() {
        return List.of(
                AggMed(true, pairs("MedAvg")),
                AggMed(false, pairs("Med")),
                AggPct(0.1, true, pairs("P10Avg")),
                AggPct(0.25, false, pairs("P25")),
                AggPct(0.9, true, pairs("P90Avg")),
                AggPct(0.99, false, pairs("P99")));
    }

    private static String[] pairs(final String prefix) {
        final String[] pairs = new String[VALUE_COLUMNS.length];
        for (int ii = 0; ii < VALUE_COLUMNS.length; ++ii) {
            pairs[ii] = prefix + VALUE_COLUMNS[ii] + "=" + VALUE_COLUMNS[ii];
        }
        return pairs;
    }

    private static Table makeData(final int size) {
        // K == 3 is a group with only null values; Small has groups of three or four rows
        return TableTools.emptyTable(size).update(
                "K = ii % 50",
                "Small = ii % 3000",
                "Raw = (ii * 7919) % 1000",
                "C = K == 3 || ii % 19 == 0 ? NULL_CHAR : (char) ('a' + Raw % 26)",
                "By = K == 3 || ii % 23 == 0 ? NULL_BYTE : (byte) (Raw % 200 - 100)",
                "S = K == 3 || ii % 29 == 0 ? NULL_SHORT : (short) (Raw - 500)",
                "I = K == 3 || ii % 17 == 0 ? NULL_INT : (int) Raw",
                "L = K == 3 || ii % 7 == 0 ? NULL_LONG : Raw - 500",
                "F = K == 3 || ii % 31 == 0 ? NULL_FLOAT : ii % 1301 == 0 ? Float.NaN : (float) (Raw / 4.0)",
                "D = K == 3 || ii % 11 == 0 ? NULL_DOUBLE : ii % 1201 == 0 ? Double.NaN : Raw / 8.0",
                "B = K == 3 || ii % 13 == 0 ? null : java.math.BigDecimal.valueOf(Raw - 500)",
                // values that compare equal but differ in scale; the result is the first one each group received
                "Scaled = K == 3 ? null : java.math.BigDecimal.valueOf(Raw % 7).setScale((int) (ii % 3))",
                "Bo = K == 3 || ii % 37 == 0 ? null : Raw % 3 == 0",
                "T = K == 3 || ii % 41 == 0 ? null : epochNanosToInstant(Raw * 1000)",
                "Str = K == 3 || ii % 43 == 0 ? null : `s` + (Raw % 100)")
                .select();
    }

    private static void checkAgainstRefreshing(final Table data, final String... keys) {
        final QueryTable refreshing = new QueryTable(data.getRowSet().copy().toTracking(), data.getColumnSourceMap());
        refreshing.setRefreshing(true);
        final Table expected = refreshing.aggBy(aggregations(), keys);
        final Table actual = data.aggBy(aggregations(), keys);
        assertTableEquals(expected, actual);
    }

    @Test
    public void testKeyed() {
        checkAgainstRefreshing(makeData(20_011), "K");
    }

    @Test
    public void testSmallGroups() {
        checkAgainstRefreshing(makeData(10_007), "Small");
    }

    @Test
    public void testZeroKey() {
        checkAgainstRefreshing(makeData(20_011).where("K != 3"));
    }

    @Test
    public void testZeroKeyAllNull() {
        checkAgainstRefreshing(makeData(1_000).where("K == 3"));
    }

    @Test
    public void testEmpty() {
        checkAgainstRefreshing(makeData(1_000).where("false"));
        checkAgainstRefreshing(makeData(1_000).where("false"), "K");
    }
}

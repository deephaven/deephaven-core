//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by.ssmpercentile;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListener;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.testutil.generator.BigDecimalGenerator;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.DoubleGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.LongGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.junit.Rule;
import org.junit.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Random;

import static io.deephaven.api.agg.Aggregation.AggMed;
import static io.deephaven.api.agg.Aggregation.AggPct;
import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.col;
import static org.junit.Assert.assertEquals;
import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;

/**
 * Checks the refreshing percentile operator, which stages each cycle's values and applies them to its SSMs in one batch
 * per bucket, against the static percentile operator applied to a snapshot after every cycle. The tables span many
 * chunks per bucket, and columns with few distinct values make modifications that net to no change common.
 */
public class SsmChunkedPercentileOperatorTest {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private static final String[] VALUE_COLUMNS = {"Few", "I", "L", "D", "C", "B"};

    private static List<Aggregation> aggregations() {
        return List.of(
                AggMed(true, pairs("MedAvg")),
                AggMed(false, pairs("Med")),
                AggPct(0.1, true, pairs("P10Avg")),
                AggPct(0.9, false, pairs("P90")));
    }

    private static String[] pairs(final String prefix) {
        final String[] pairs = new String[VALUE_COLUMNS.length];
        for (int ii = 0; ii < VALUE_COLUMNS.length; ++ii) {
            pairs[ii] = prefix + VALUE_COLUMNS[ii] + "=" + VALUE_COLUMNS[ii];
        }
        return pairs;
    }

    /**
     * Compares an incrementally maintained aggregation with the same aggregation of a static snapshot, and checks that
     * every result row whose values changed since the previous validation was reported as added or modified.
     */
    private static class SnapshotComparison implements EvalNuggetInterface {
        private final QueryTable source;
        private final String[] keys;
        private final QueryTable incremental;
        private final WritableRowSet reported = RowSetFactory.empty();
        @ReferentialIntegrity
        private final TableUpdateListener reportedListener;
        private Table previous;

        private SnapshotComparison(final QueryTable source, final String... keys) {
            this.source = source;
            this.keys = keys;
            incremental = (QueryTable) source.aggBy(aggregations(), keys);
            reportedListener = new InstrumentedTableUpdateListener("SnapshotComparison") {
                @Override
                public void onUpdate(final TableUpdate update) {
                    reported.insert(update.added());
                    reported.insert(update.modified());
                }

                @Override
                protected void onFailureInternal(final Throwable originalException, final Entry sourceEntry) {
                    throw new AssertionError(originalException);
                }
            };
            incremental.addUpdateListener(reportedListener);
        }

        private Table sorted(final Table table) {
            return keys.length == 0 ? table : table.sort(keys);
        }

        @Override
        public void validate(final String msg) {
            final Table current = incremental.snapshot();
            final Table expected = source.snapshot().aggBy(aggregations(), keys);
            assertTableEquals(sorted(expected), sorted(current));
            if (previous != null) {
                try (final WritableRowSet unreported = current.getRowSet().minus(reported)) {
                    assertTableEquals(((QueryTable) previous).getSubTable(unreported.copy().toTracking()),
                            ((QueryTable) current).getSubTable(unreported.copy().toTracking()));
                }
            }
            previous = current;
            reported.clear();
        }

        @Override
        public void show() {
            TableTools.showWithRowSet(incremental);
        }
    }

    private static void checkIncremental(final int size, final int updateSize, final int steps, final long seed) {
        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] columnInfo = initColumnInfos(
                new String[] {"Sym", "Few", "I", "L", "D", "C", "B"},
                new SetGenerator<>("a", "b", "c"),
                new SetGenerator<>(1.5, 2.5, 3.5, 4.5),
                new IntGenerator(0, 20, 0.05),
                new LongGenerator(-1_000_000, 1_000_000, 0.05),
                new DoubleGenerator(0, 100, 0.05, 0.0005),
                new CharGenerator('a', 'f', 0.05),
                new BigDecimalGenerator(BigInteger.valueOf(-100), BigInteger.valueOf(100), 2, 0.05));
        final QueryTable table = getTable(size, random, columnInfo);
        final EvalNuggetInterface[] nuggets = {
                new SnapshotComparison(table, "Sym"),
                new SnapshotComparison(table),
        };
        for (final EvalNuggetInterface nugget : nuggets) {
            nugget.validate("initial");
        }
        for (int step = 0; step < steps; ++step) {
            RefreshingTableTestCase.simulateShiftAwareStep(updateSize, random, table, columnInfo, nuggets);
        }
    }

    @Test
    public void testManyChunksPerBucket() {
        checkIncremental(20_000, 10_000, 10, 0);
    }

    @Test
    public void testSmallUpdates() {
        checkIncremental(2_000, 20, 50, 1);
    }

    @Test
    public void testFromEmpty() {
        checkIncremental(0, 5_000, 20, 2);
    }

    /**
     * A value modified to one that compares equal but is not equal is not netted away, so the result takes the new
     * value.
     */
    @Test
    public void testModifyToComparisonEqualValue() {
        final QueryTable table = testRefreshingTable(i(0).toTracking(), col("B", new BigDecimal("1.0")));
        final Table median = table.aggBy(List.of(AggMed("B")));
        assertEquals(new BigDecimal("1.0"), median.getColumnSource("B").get(0));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(0), col("B", new BigDecimal("1.00")));
            table.notifyListeners(i(), i(), i(0));
        });
        assertEquals(new BigDecimal("1.00"), median.getColumnSource("B").get(0));
    }
}

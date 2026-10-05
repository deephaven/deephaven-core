//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.table.impl.perf.UpdatePerformanceTracker;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.generator.BigDecimalGenerator;
import io.deephaven.engine.testutil.generator.BigIntegerGenerator;
import io.deephaven.engine.testutil.generator.BooleanGenerator;
import io.deephaven.engine.testutil.generator.ByteGenerator;
import io.deephaven.engine.testutil.generator.CharGenerator;
import io.deephaven.engine.testutil.generator.DoubleGenerator;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.LongGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.generator.ShortGenerator;
import io.deephaven.engine.testutil.generator.StringGenerator;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static io.deephaven.engine.testutil.TstUtils.SHORT_TESTS;
import static io.deephaven.engine.testutil.TstUtils.getTable;
import static io.deephaven.engine.testutil.TstUtils.initColumnInfos;

/**
 * Incremental sumBy/absSumBy fuzz coverage, split from {@link QueryTableAggregationTest} so this long-running suite
 * packs into its own test-executor fork.
 */
@Category(OutOfBandTest.class)
public class QueryTableSumByIncrementalTest {

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    @Before
    public void setUp() throws Exception {
        UpdatePerformanceTracker.resetForUnitTests();
        ChunkPoolReleaseTracking.enableStrict();
    }

    @After
    public void tearDown() throws Exception {
        UpdatePerformanceTracker.resetForUnitTests();
        ChunkPoolReleaseTracking.checkAndDisable();
    }

    @Test
    public void testSumByIncremental() {
        final int[] sizes;
        if (SHORT_TESTS) {
            sizes = new int[] {100, 1_000};
        } else {
            sizes = new int[] {10, 100, 4_000, 10_000};
        }
        for (final int size : sizes) {
            for (int seed = 0; seed < 1; ++seed) {
                UpdatePerformanceTracker.resetForUnitTests();
                ChunkPoolReleaseTracking.enableStrict();
                System.out.println("Size = " + size + ", Seed = " + seed);
                testSumByIncremental(size, seed, true, true);
                testSumByIncremental(size, seed, true, false);
                testSumByIncremental(size, seed, false, true);
                testSumByIncremental(size, seed, false, false);
                UpdatePerformanceTracker.resetForUnitTests();
                ChunkPoolReleaseTracking.checkAndDisable();
            }
        }
    }

    private void testSumByIncremental(final int size, final int seed, boolean grouped, boolean lotsOfStrings) {
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            doTestSumByIncremental(size, seed, grouped, lotsOfStrings);
        }
    }

    private void doTestSumByIncremental(final int size, final int seed, boolean grouped, boolean lotsOfStrings) {
        final Random random = new Random(seed);
        final ColumnInfo<?, ?>[] columnInfo;
        final List<ColumnInfo.ColAttributes> ea = Collections.emptyList();
        final List<ColumnInfo.ColAttributes> ga = Collections.singletonList(ColumnInfo.ColAttributes.Indexed);
        final QueryTable queryTable = getTable(size, random, columnInfo = initColumnInfos(
                new String[] {"Sym", "charCol", "byteCol", "shortCol", "intCol", "longCol", "bigI", "bigD",
                        "doubleCol", "doubleNanCol", "boolCol"},
                Arrays.asList(grouped ? ga : ea, ea, ea, ea, ea, ea, ea, ea, ea, ea, ea),
                lotsOfStrings ? new StringGenerator(1000000) : new SetGenerator<>("a", "b", "c", "d"),
                new CharGenerator('a', 'z'),
                new ByteGenerator(),
                new ShortGenerator((short) -20000, (short) 20000, 0.1),
                new IntGenerator(Integer.MIN_VALUE / 2, Integer.MAX_VALUE / 2, 0.01),
                new LongGenerator(-100_000_000, 100_000_000),
                new BigIntegerGenerator(0.1),
                new BigDecimalGenerator(0.1),
                new SetGenerator<>(10.1, 20.1, 30.1, -40.1),
                new DoubleGenerator(-100000.0, 100000.0, 0.01, 0.001),
                new BooleanGenerator(0.5, 0.1)));

        if (RefreshingTableTestCase.printTableUpdates) {
            TableTools.showWithRowSet(queryTable);
        }

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> queryTable.dropColumns("Sym").sumBy()),
                EvalNugget.Sorted.from(() -> queryTable.sumBy("Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym").sumBy("Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.dropColumns("Sym").sort("intCol").sumBy("intCol"), "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym", "intCol").sumBy("Sym", "intCol"), "Sym",
                        "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym").update("x=intCol+1").sumBy("Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.sortDescending("intCol").update("x=intCol+1").dropColumns("Sym")
                        .sumBy("intCol"), "intCol"),
                EvalNugget.Sorted.from(
                        () -> queryTable.sort("Sym", "intCol").update("x=intCol+1").sumBy("Sym", "intCol"), "Sym",
                        "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1").sumBy("Sym"),
                        "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym").absSumBy("Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.dropColumns("Sym").sort("intCol").absSumBy("intCol"),
                        "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym", "intCol").absSumBy("Sym", "intCol"), "Sym",
                        "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym").update("x=intCol+1").absSumBy("Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.sortDescending("intCol").update("x=intCol+1").dropColumns("Sym")
                        .absSumBy("intCol"), "intCol"),
                EvalNugget.Sorted.from(
                        () -> queryTable.sort("Sym", "intCol").update("x=intCol+1").absSumBy("Sym", "intCol"), "Sym",
                        "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1").absSumBy("Sym"),
                        "Sym"),
        };

        for (int step = 0; step < 50; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Seed = " + seed + ", step=" + step);
            }
            RefreshingTableTestCase.simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }
}

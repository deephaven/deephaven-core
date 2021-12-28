package com.illumon.iris.db.v2;

import com.illumon.iris.db.tables.utils.TableTools;
import com.illumon.iris.db.util.liveness.LivenessScopeStack;
import com.illumon.iris.db.v2.sources.chunk.util.pools.ChunkPoolReleaseTracking;
import com.illumon.util.SafeCloseable;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import static com.illumon.iris.db.v2.TstUtils.*;

/**
 * We do a more exaustive set of sumBy tests that involve different table size, a variety of key columns, etc. as a
 * standin for the general aggregation.  They are in a separate test so that the main aggregation test can be run
 * in parallel instead of having one ten minute long test.
 */
public class QueryTableIncrementalSumByTest extends QueryTableTestBase {
    @Override
    protected void setUp() throws Exception {
        super.setUp();
        ChunkPoolReleaseTracking.enableStrict();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        ChunkPoolReleaseTracking.checkAndDisable();
    }

    public void testSumByIncremental() {
        final int[] sizes = {10, 100, 4000, 10000};
        for (final int size : sizes) {
            for (int seed = 0; seed < 1; ++seed) {
                ChunkPoolReleaseTracking.enableStrict();
                System.out.println("Size = " + size + ", Seed = " + seed);
                testSumByIncremental(size, seed, true, true);
                testSumByIncremental(size, seed, true, false);
                testSumByIncremental(size, seed, false, true);
                testSumByIncremental(size, seed, false, false);
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
        final ColumnInfo[] columnInfo;
        final List<ColumnInfo.ColAttributes> ea = Collections.emptyList();
        final List<ColumnInfo.ColAttributes> ga = Collections.singletonList(ColumnInfo.ColAttributes.Grouped);
        final QueryTable queryTable = getTable(size, random, columnInfo = initColumnInfos(new String[]{"Sym",
                        "charCol",
                        "byteCol"
                        , "shortCol", "intCol", "longCol", "bigI", "bigD", "doubleCol", "doubleNanCol", "boolCol"
                        },
                Arrays.asList(grouped ? ga : ea, ea, ea, ea, ea, ea, ea, ea, ea, ea, ea),
                lotsOfStrings ? new StringGenerator(1000000) : new SetGenerator<>("a", "b","c","d"),
                new CharGenerator('a', 'z'),
                new ByteGenerator(),
                new ShortGenerator((short)-20000, (short)20000, 0.1),
                new IntGenerator(Integer.MIN_VALUE/2, Integer.MAX_VALUE/2, 0.01),
                new LongGenerator(-100_000_000, 100_000_000),
                new BigIntegerGenerator(0.1),
                new BigDecimalGenerator(0.1),
                new SetGenerator<>(10.1, 20.1, 30.1, -40.1),
                new DoubleGenerator(-100000.0, 100000.0, 0.01, 0.001),
                new BooleanGenerator(0.5, 0.1)
        ));

        if (printTableUpdates) {
            TableTools.showWithIndex(queryTable);
        }

        final EvalNugget[] en = new EvalNugget[]{
                EvalNugget.from(() -> queryTable.dropColumns("Sym").sumBy()),
                EvalNugget.Sorted.from(() -> queryTable.sumBy("Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym").sumBy("Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.dropColumns("Sym").sort("intCol").sumBy("intCol"), "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym", "intCol").sumBy("Sym","intCol"), "Sym", "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym").update("x=intCol+1").sumBy("Sym"), "Sym"),
                EvalNugget.Sorted.from(() -> queryTable.sortDescending("intCol").update("x=intCol+1").dropColumns("Sym").sumBy("intCol"), "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1").sumBy("Sym","intCol"), "Sym", "intCol"),
                EvalNugget.Sorted.from(() -> queryTable.sort("Sym", "intCol").update("x=intCol+1").sumBy("Sym"), "Sym"),
        };

        for (int step = 0; step < 50; step++) {
            if (printTableUpdates) {
                System.out.println("Seed = " + seed + ", step=" + step);
            }
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }
}

package io.deephaven.engine.v2;

import io.deephaven.engine.tables.Table;
import io.deephaven.engine.util.liveness.LivenessScopeStack;
import io.deephaven.engine.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.util.SafeCloseable;

import java.util.Random;

import static io.deephaven.engine.v2.TstUtils.getTable;
import static io.deephaven.engine.v2.TstUtils.initColumnInfos;

public class TestTableValidator extends RefreshingTableTestCase {
    public void testValidator() {
        ChunkPoolReleaseTracking.enableStrict();

        try (final SafeCloseable sc = LivenessScopeStack.open()) {

            final Random random = new Random(0);
            final TstUtils.ColumnInfo[] columnInfo;
            final int size = 50;
            final QueryTable queryTable = getTable(size, random,
                    columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                            new TstUtils.SetGenerator<>("a", "b", "c", "d", "e"),
                            new TstUtils.IntGenerator(10, 100),
                            new TstUtils.SetGenerator<>(10.1, 20.1, 30.1)));

            final EvalNugget[] en = new EvalNugget[] {
                    new EvalNugget() {
                        public Table e() {
                            return queryTable;
                        }
                    },
            };

            for (int i = 0; i < 10; i++) {
                simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
            }
        }

        ChunkPoolReleaseTracking.checkAndDisable();
    }
}

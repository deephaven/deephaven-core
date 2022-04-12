/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.util;

import io.deephaven.engine.table.Table;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.engine.table.impl.*;
import org.junit.Assert;

import java.util.*;

import static io.deephaven.engine.table.impl.TstUtils.*;
import static io.deephaven.engine.util.TableTools.diff;

public class TestFunctionBackedTableFactory extends RefreshingTableTestCase {
    public void testIterative() {
        Random random = new Random(0);
        TstUtils.ColumnInfo columnInfo[];
        int size = 50;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new TstUtils.SetGenerator<>("a", "b", "c", "d", "e"),
                        new TstUtils.IntGenerator(10, 100),
                        new TstUtils.SetGenerator<>(10.1, 20.1, 30.1)));

        final Table functionBacked = FunctionGeneratedTableFactory.create(() -> queryTable, queryTable);

        assertTableEquals(queryTable, functionBacked, TableDiff.DiffItems.DoublesExact);

        EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new QueryTableTest.TableComparator(functionBacked, queryTable),
                // Note: disable update validation since the function backed table's prev values will always be
                // incorrect
                EvalNugget.from(() -> UpdateGraphProcessor.DEFAULT.exclusiveLock()
                        .computeLocked(() -> functionBacked.update("Mult=intCol * doubleCol"))),
        };

        for (int i = 0; i < 75; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }
}

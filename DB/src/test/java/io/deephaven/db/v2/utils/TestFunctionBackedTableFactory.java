/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.utils;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableDiff;
import io.deephaven.db.v2.*;
import org.junit.Assert;

import java.util.*;

import static io.deephaven.db.tables.utils.TableTools.diff;
import static io.deephaven.db.v2.TstUtils.getTable;
import static io.deephaven.db.v2.TstUtils.initColumnInfos;

public class TestFunctionBackedTableFactory extends LiveTableTestCase {
    public void testIterative() {
        Random random = new Random(0);
        TstUtils.ColumnInfo columnInfo[];
        int size = 50;
        final QueryTable queryTable = getTable(size, random,
            columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                new TstUtils.SetGenerator<>("a", "b", "c", "d", "e"),
                new TstUtils.IntGenerator(10, 100),
                new TstUtils.SetGenerator<>(10.1, 20.1, 30.1)));

        final Table functionBacked =
            FunctionGeneratedTableFactory.create(() -> queryTable, queryTable);

        final String diff =
            diff(functionBacked, queryTable, 10, EnumSet.of(TableDiff.DiffItems.DoublesExact));
        Assert.assertEquals("", diff);

        EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new QueryTableTest.TableComparator(functionBacked, queryTable),
                // Note: disable update validation since the function backed table's prev values
                // will always be incorrect
                EvalNugget.from(() -> LiveTableMonitor.DEFAULT.exclusiveLock()
                    .computeLocked(() -> functionBacked.update("Mult=intCol * doubleCol"))),
        };

        for (int i = 0; i < 75; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }
}

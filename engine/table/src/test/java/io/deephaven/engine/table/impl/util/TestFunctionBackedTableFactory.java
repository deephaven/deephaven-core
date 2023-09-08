/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.QueryTableTest;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableDiff;
import io.deephaven.qst.type.Type;
import io.deephaven.util.function.ThrowingRunnable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;

public class TestFunctionBackedTableFactory extends RefreshingTableTestCase {
    public void testIterative() {
        Random random = new Random(0);
        ColumnInfo<?, ?>[] columnInfo;
        int size = 50;
        final QueryTable queryTable = getTable(size, random,
                columnInfo = initColumnInfos(new String[] {"Sym", "intCol", "doubleCol"},
                        new SetGenerator<>("a", "b", "c", "d", "e"),
                        new IntGenerator(10, 100),
                        new SetGenerator<>(10.1, 20.1, 30.1)));

        final Table functionBacked = FunctionGeneratedTableFactory.create(() -> queryTable, queryTable);

        assertTableEquals(queryTable, functionBacked, TableDiff.DiffItems.DoublesExact);

        EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new QueryTableTest.TableComparator(functionBacked, queryTable),
                // Note: disable update validation since the function backed table's prev values will always be
                // incorrect
                EvalNugget.from(() -> {
                    return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                            () -> functionBacked.update("Mult=intCol * doubleCol"));
                }),
        };

        for (int i = 0; i < 75; i++) {
            simulateShiftAwareStep(size, random, queryTable, columnInfo, en);
        }
    }

    public void testNoSources() {
        // If no sources are specified, function should still run once on initializtaion.
        final Table functionBacked =
                FunctionGeneratedTableFactory.create(() -> newTable(
                        stringCol("StringCol", "MyString"),
                        intCol("IntCol", 12345)));

        assertEquals(functionBacked.size(), 1);
        assertTableEquals(newTable(
                stringCol("StringCol", "MyString"),
                intCol("IntCol", 12345)), functionBacked);
    }

    public void testMultipleSources() throws Exception {
        final AppendOnlyArrayBackedMutableTable source1 = AppendOnlyArrayBackedMutableTable.make(TableDefinition.of(
                ColumnDefinition.of("StringCol", Type.stringType())));
        final BaseArrayBackedMutableTable.ArrayBackedMutableInputTable inputTable1 = source1.makeHandler();

        final AppendOnlyArrayBackedMutableTable source2 = AppendOnlyArrayBackedMutableTable.make(TableDefinition.of(
                ColumnDefinition.of("IntCol", Type.intType())));
        final BaseArrayBackedMutableTable.ArrayBackedMutableInputTable inputTable2 = source2.makeHandler();

        final Table functionBacked =
                FunctionGeneratedTableFactory.create(() -> source1.lastBy().naturalJoin(source2, ""), source1, source2);

        assertEquals(functionBacked.size(), 0);

        handleDelayedRefresh(() -> {
            inputTable1.addAsync(newTable(stringCol("StringCol", "MyString")), false, t -> {
            });
            inputTable2.addAsync(newTable(intCol("IntCol", 12345)), false, t -> {
            });
        }, source1, source2);

        assertEquals(functionBacked.size(), 1);
        assertTableEquals(newTable(
                stringCol("StringCol", "MyString"),
                intCol("IntCol", 12345)), functionBacked);
    }

    /**
     * See {@link io.deephaven.engine.table.impl.util.TestKeyedArrayBackedMutableTable#handleDelayedRefresh}
     */
    public static void handleDelayedRefresh(final ThrowingRunnable<IOException> action,
            final BaseArrayBackedMutableTable... tables) throws Exception {
        final Thread refreshThread;
        final CountDownLatch gate = new CountDownLatch(tables.length);

        Arrays.stream(tables).forEach(t -> t.setOnPendingChange(gate::countDown));
        try {
            final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
            refreshThread = new Thread(() -> {
                // If this unexpected interruption happens, the test thread may hang in action.run()
                // indefinitely. Best to hope it's already queued the pending action and proceed with run.
                updateGraph.runWithinUnitTestCycle(() -> {
                    try {
                        gate.await();
                    } catch (InterruptedException ignored) {
                        // If this unexpected interruption happens, the test thread may hang in action.run()
                        // indefinitely. Best to hope it's already queued the pending action and proceed with run.
                    }
                    Arrays.stream(tables).forEach(BaseArrayBackedMutableTable::run);
                });
            });

            refreshThread.start();
            action.run();
        } finally {
            Arrays.stream(tables).forEach(t -> t.setOnPendingChange(null));
        }
        try {
            refreshThread.join();
        } catch (InterruptedException e) {
            throw new UncheckedDeephavenException(
                    "Interrupted unexpectedly while waiting for run cycle to complete", e);
        }
    }
}

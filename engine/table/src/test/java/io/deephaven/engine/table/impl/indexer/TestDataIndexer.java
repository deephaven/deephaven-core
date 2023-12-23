/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.indexer;

import com.google.common.collect.Sets;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;

import java.util.*;

import org.junit.experimental.categories.Category;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateTableUpdates;

@Category(OutOfBandTest.class)
public class TestDataIndexer extends RefreshingTableTestCase {

    private static ArrayList<ArrayList<String>> powerSet(Set<String> originalSet) {
        final ArrayList<ArrayList<String>> setList = new ArrayList<>();
        Set<Set<String>> powerSet = Sets.powerSet(originalSet);
        for (Set<String> set : powerSet) {
            if (set.isEmpty()) {
                continue;
            }
            setList.add(new ArrayList<>(set));
        }
        return setList;
    }

    public void testIndex() {
        testIndex(false, new Random(0), new MutableInt(50));
    }

    public void testIndexWithImmutableColumns() {
        testIndex(true, new Random(0), new MutableInt(50));
    }

    private void testIndex(final boolean immutableColumns, final Random random, final MutableInt numSteps) {
        int size = 100;

        ColumnInfo<?, ?>[] columnInfo = new ColumnInfo[3];
        if (immutableColumns) {
            columnInfo[0] = new ColumnInfo<>(new SetGenerator<>("a", "b", "c", "d", "e", "f"), "Sym",
                    ColumnInfo.ColAttributes.Immutable);
            columnInfo[1] = new ColumnInfo<>(new IntGenerator(10, 100), "intCol",
                    ColumnInfo.ColAttributes.Immutable);
        } else {
            columnInfo[0] = new ColumnInfo<>(new SetGenerator<>("a", "b", "c", "d", "e", "f"), "Sym");
            columnInfo[1] = new ColumnInfo<>(new IntGenerator(10, 100), "intCol");
        }
        columnInfo[2] = new ColumnInfo<>(new SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");

        final QueryTable queryTable = TstUtils.getTable(size, random, columnInfo);
        queryTable.setRefreshing(true);

        // Create indexes for every column combination
        final DataIndexer dataIndexer = DataIndexer.of(queryTable.getRowSet());
        for (final ArrayList<String> set : powerSet(queryTable.getColumnSourceMap().keySet())) {
            if (set.size() == 0) {
                continue;
            }
            System.out.println("Creating index for " + set);
            dataIndexer.getOrCreateDataIndex(queryTable, set.toArray(String[]::new));
        }

        addIndexValidator(queryTable, "queryTable");

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> {
                    return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                            () -> queryTable.head(0));
                }),
                EvalNugget.from(() -> {
                    return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                            () -> queryTable.head(1));
                }),
                EvalNugget.from(() -> {
                    return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                            () -> queryTable.update("intCol2 = intCol + 1"));
                }),
                EvalNugget.from(() -> {
                    return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                            () -> queryTable.update("intCol2 = intCol + 1").select());
                }),
                EvalNugget.from(() -> {
                    return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                            () -> queryTable.view("Sym", "intCol2 = intCol + 1"));
                }),
                EvalNugget.from(() -> {
                    return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                            () -> queryTable.avgBy("Sym").sort("Sym"));
                }),
                EvalNugget.from(() -> {
                    return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                            () -> queryTable.groupBy("Sym", "intCol")
                                    .sort("Sym", "intCol")
                                    .view("doubleCol=max(doubleCol)"));
                }),
                EvalNugget.from(() -> {
                    return ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                            () -> queryTable.avgBy("Sym", "doubleCol")
                                    .sort("Sym", "doubleCol")
                                    .view("intCol=min(intCol)"));
                }),
        };

        for (int ii = 0; ii < en.length; ++ii) {
            addIndexValidator(en[ii].originalValue, "en[" + ii + "]");
        }

        Table by = ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                () -> queryTable.avgBy("Sym"));
        addIndexValidator(by, "groupBy");
        Table avgBy = ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                () -> queryTable.avgBy("Sym"));
        addIndexValidator(avgBy, "avgBy");
        Table avgBy1 = ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                () -> queryTable.avgBy("Sym", "intCol"));
        addIndexValidator(avgBy1, "avgBy1");

        Table merged = Require.neqNull(ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                () -> TableTools.merge(queryTable)), "TableTools.merge(queryTable)");
        addIndexValidator(merged, "merged");
        Table updated = ExecutionContext.getContext().getUpdateGraph().exclusiveLock()
                .computeLocked(() -> merged.update("HiLo = intCol > 50 ? `Hi` : `Lo`"));
        addIndexValidator(updated, "updated");

        final int maxSteps = numSteps.intValue(); // 8;

        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Initial QueryTable: ");
            TableTools.showWithRowSet(queryTable);
        }
        for (final IndexValidator indexValidator : indexValidators) {
            indexValidator.validateIndexes();
        }

        for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(() -> {
                for (final IndexValidator indexValidator : indexValidators) {
                    indexValidator.validatePrevIndexes();
                }
                generateTableUpdates(size, random, queryTable, columnInfo);
            });
            TstUtils.validate("Table", en);
            for (final IndexValidator indexValidator : indexValidators) {
                indexValidator.validateIndexes();
            }
        }

        // we don't need them after this test is done
        indexValidators.clear();
    }

    // we don't ever need to look at the grouping validators, just make sure they don't go away
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private final ArrayList<IndexValidator> indexValidators = new ArrayList<>();

    private void addIndexValidator(Table originalValue, String context) {
        ArrayList<ArrayList<String>> columnSets = powerSet(originalValue.getColumnSourceMap().keySet());
        indexValidators.add(new IndexValidator(context, originalValue, columnSets));
    }

    public void testCombinedGrouping() {
        Random random = new Random(0);
        int size = 100;

        ColumnInfo<?, ?>[] columnInfo = new ColumnInfo[4];
        columnInfo[0] = new ColumnInfo<>(new SetGenerator<>("a", "b", "c", "d", "e", "f"), "Sym",
                ColumnInfo.ColAttributes.Immutable, ColumnInfo.ColAttributes.Grouped);
        columnInfo[1] = new ColumnInfo<>(new SetGenerator<>("q", "r", "s", "t"), "Sym2",
                ColumnInfo.ColAttributes.Immutable, ColumnInfo.ColAttributes.Grouped);
        columnInfo[2] = new ColumnInfo<>(new IntGenerator(10, 100), "intCol",
                ColumnInfo.ColAttributes.Immutable, ColumnInfo.ColAttributes.Grouped);
        columnInfo[3] = new ColumnInfo<>(new SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");

        final QueryTable nonCountingTable = TstUtils.getTable(size, random, columnInfo);

        final QueryTable countingTable = CountingTable.getCountingTable(nonCountingTable);

        final ColumnSource<?> symColumnSource = countingTable.getColumnSource("Sym");
        final ColumnSource<?> sym2ColumnSource = countingTable.getColumnSource("Sym2");
        final ColumnSource<?> intColumnSource = countingTable.getColumnSource("intCol");
        final ColumnSource<?> doubleColumnSource = countingTable.getColumnSource("doubleCol");

        final DataIndexer indexer = DataIndexer.of(countingTable.getRowSet());

        assertTrue(indexer.hasDataIndex(symColumnSource));
        assertTrue(indexer.hasDataIndex(sym2ColumnSource));
        assertTrue(indexer.hasDataIndex(intColumnSource));
        assertFalse(indexer.hasDataIndex(intColumnSource, symColumnSource));
        assertFalse(indexer.hasDataIndex(intColumnSource, symColumnSource, sym2ColumnSource));
        assertFalse(indexer.hasDataIndex(intColumnSource, symColumnSource, doubleColumnSource));

        // Add the multi-column indexes.
        indexer.getOrCreateDataIndex(countingTable, "intCol", "Sym");
        indexer.getOrCreateDataIndex(countingTable, "intCol", "Sym", "Sym2");
        indexer.getOrCreateDataIndex(countingTable, "intCol", "Sym", "doubleCol");
        indexer.getOrCreateDataIndex(countingTable, "intCol", "Sym", "Sym2", "doubleCol");

        assertTrue(indexer.hasDataIndex(intColumnSource, symColumnSource));
        assertTrue(indexer.hasDataIndex(intColumnSource, symColumnSource, sym2ColumnSource));
        assertTrue(indexer.hasDataIndex(intColumnSource, symColumnSource, doubleColumnSource));
        assertTrue(indexer.hasDataIndex(intColumnSource, symColumnSource, sym2ColumnSource, doubleColumnSource));

        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));

        IndexValidator.validateIndex(new String[] {"Sym"}, countingTable.getRowSet(), countingTable, "sym", false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        IndexValidator.validateIndex(new String[] {"intCol"}, countingTable.getRowSet(), countingTable, "intCol",
                false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        IndexValidator.validateIndex(new String[] {"intCol", "Sym"}, countingTable.getRowSet(), countingTable,
                "intCol+sym", false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) sym2ColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        IndexValidator.validateIndex(new String[] {"intCol", "Sym", "Sym2"}, countingTable.getRowSet(),
                countingTable, "intCol+sym+sym2", false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("getDouble"));
        IndexValidator.validateIndex(new String[] {"intCol", "Sym", "doubleCol"}, countingTable.getRowSet(),
                countingTable, "intCol+sym+doubleCol", false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) sym2ColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("getDouble"));
        IndexValidator.validateIndex(new String[] {"intCol", "Sym", "Sym2", "doubleCol"},
                countingTable.getRowSet(), countingTable, "intCol+sym+sym2+doubleCol", false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());
    }
}

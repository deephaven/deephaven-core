//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
import io.deephaven.util.mutable.MutableInt;
import junit.framework.TestCase;

import java.util.*;

import org.junit.experimental.categories.Category;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateTableUpdates;

@Category(OutOfBandTest.class)
public class TestDataIndexer extends RefreshingTableTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    private static List<List<String>> powerSet(Set<String> originalSet) {
        final List<List<String>> setList = new ArrayList<>();
        final Set<Set<String>> powerSet = Sets.powerSet(originalSet);
        for (final Set<String> set : powerSet) {
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

        // Create indexes for every column combination; they will be retained by our parent class's LivenessScope
        for (final List<String> set : powerSet(queryTable.getColumnSourceMap().keySet())) {
            System.out.println("Creating index for " + set);
            DataIndexer.getOrCreateDataIndex(queryTable, set.toArray(String[]::new));
        }

        addIndexValidator(queryTable, "queryTable");

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                        () -> queryTable.head(0))),
                EvalNugget.from(() -> ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                        () -> queryTable.head(1))),
                EvalNugget.from(() -> ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                        () -> queryTable.update("intCol2 = intCol + 1"))),
                EvalNugget.from(() -> ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                        () -> queryTable.update("intCol2 = intCol + 1").select())),
                EvalNugget.from(() -> ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                        () -> queryTable.view("Sym", "intCol2 = intCol + 1"))),
                EvalNugget.from(() -> ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                        () -> queryTable.avgBy("Sym").sort("Sym"))),
                EvalNugget.from(() -> ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                        () -> queryTable.groupBy("Sym", "intCol")
                                .sort("Sym", "intCol")
                                .view("doubleCol=max(doubleCol)"))),
                EvalNugget.from(() -> ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(
                        () -> queryTable.avgBy("Sym", "doubleCol")
                                .sort("Sym", "doubleCol")
                                .view("intCol=min(intCol)"))),
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

        for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
            ExecutionContext.getContext().getUpdateGraph().<ControlledUpdateGraph>cast().runWithinUnitTestCycle(() -> {
                generateTableUpdates(size, random, queryTable, columnInfo);
            });
            TstUtils.validate("Table", en);
        }
    }

    private void addIndexValidator(final Table originalValue, final String context) {
        final List<List<String>> columnSets = powerSet(originalValue.getColumnSourceMap().keySet());
        // Rely on the parent class's LivenessScope to retain the index
        new IndexValidator(context, originalValue, columnSets);
    }

    public void testCombinedGrouping() {
        Random random = new Random(0);
        int size = 100;

        ColumnInfo<?, ?>[] columnInfo = new ColumnInfo[4];
        columnInfo[0] = new ColumnInfo<>(new SetGenerator<>("a", "b", "c", "d", "e", "f"), "Sym",
                ColumnInfo.ColAttributes.Immutable, ColumnInfo.ColAttributes.Indexed);
        columnInfo[1] = new ColumnInfo<>(new SetGenerator<>("q", "r", "s", "t"), "Sym2",
                ColumnInfo.ColAttributes.Immutable, ColumnInfo.ColAttributes.Indexed);
        columnInfo[2] = new ColumnInfo<>(new IntGenerator(10, 100), "intCol",
                ColumnInfo.ColAttributes.Immutable, ColumnInfo.ColAttributes.Indexed);
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

        // Add the multi-column indexes; they will be retained by our parent class's LivenessScope
        DataIndexer.getOrCreateDataIndex(countingTable, "intCol", "Sym");
        DataIndexer.getOrCreateDataIndex(countingTable, "intCol", "Sym", "Sym2");
        DataIndexer.getOrCreateDataIndex(countingTable, "intCol", "Sym", "doubleCol");
        DataIndexer.getOrCreateDataIndex(countingTable, "intCol", "Sym", "Sym2", "doubleCol");

        assertTrue(indexer.hasDataIndex(intColumnSource, symColumnSource));
        assertTrue(indexer.hasDataIndex(intColumnSource, symColumnSource, sym2ColumnSource));
        assertTrue(indexer.hasDataIndex(intColumnSource, symColumnSource, doubleColumnSource));
        assertTrue(indexer.hasDataIndex(intColumnSource, symColumnSource, sym2ColumnSource, doubleColumnSource));

        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));

        IndexValidator.validateIndex(countingTable, new String[] {"Sym"}, false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        IndexValidator.validateIndex(countingTable, new String[] {"intCol"}, false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        IndexValidator.validateIndex(countingTable, new String[] {"intCol", "Sym"}, false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) sym2ColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        IndexValidator.validateIndex(countingTable, new String[] {"intCol", "Sym", "Sym2"}, false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("getDouble"));
        IndexValidator.validateIndex(countingTable, new String[] {"intCol", "Sym", "doubleCol"}, false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) sym2ColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("getDouble"));
        IndexValidator.validateIndex(countingTable, new String[] {"intCol", "Sym", "Sym2", "doubleCol"}, false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());
    }
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.indexer;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.engine.util.TableTools;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.TupleSource;
import io.deephaven.engine.table.impl.TupleSourceFactory;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.experimental.categories.Category;

@SuppressWarnings("ClassInitializerMayBeStatic")
@Category(OutOfBandTest.class)
public class TestRowSetIndexer extends RefreshingTableTestCase {

    private static ArrayList<ArrayList<String>> powerSet(Set<String> originalSet) {
        return powerSet(originalSet.stream().collect(Collectors.toList()));
    }

    private static <T> ArrayList<ArrayList<T>> powerSet(List<T> originalSet) {
        ArrayList<ArrayList<T>> sets = new ArrayList<>();
        if (originalSet.isEmpty()) {
            sets.add(new ArrayList<>());
            return sets;
        }
        ArrayList<T> list = new ArrayList<>(originalSet);
        T head = list.get(0);
        for (ArrayList<T> set : powerSet(list.subList(1, list.size()))) {
            ArrayList<T> newSet = new ArrayList<>(list.size());
            newSet.add(head);
            newSet.addAll(set);
            sets.add(newSet);
            sets.add(set);
        }
        Assert.eq(sets.size(), "sets.size()", 1 << originalSet.size(), "1<<originalSet.size()");
        return sets;
    }

    public void testGrouping() {
        testGrouping(false, new Random(0), new MutableInt(50));
    }

    public void testGroupingWithImmutableColumns() {
        testGrouping(true, new Random(0), new MutableInt(50));
    }

    public void testGrouping(final boolean immutableColumns, final Random random, final MutableInt numSteps) {
        int size = 100;

        TstUtils.ColumnInfo[] columnInfo = new TstUtils.ColumnInfo[3];
        if (immutableColumns) {
            // noinspection unchecked
            columnInfo[0] = new TstUtils.ColumnInfo(new TstUtils.SetGenerator<>("a", "b", "c", "d", "e", "f"), "Sym",
                    TstUtils.ColumnInfo.ColAttributes.Immutable);
            // noinspection unchecked
            columnInfo[1] = new TstUtils.ColumnInfo(new TstUtils.IntGenerator(10, 100), "intCol",
                    TstUtils.ColumnInfo.ColAttributes.Immutable);
        } else {
            // noinspection unchecked
            columnInfo[0] = new TstUtils.ColumnInfo(new TstUtils.SetGenerator<>("a", "b", "c", "d", "e", "f"), "Sym");
            // noinspection unchecked
            columnInfo[1] = new TstUtils.ColumnInfo(new TstUtils.IntGenerator(10, 100), "intCol");
        }
        // noinspection unchecked
        columnInfo[2] = new TstUtils.ColumnInfo(new TstUtils.SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");

        final QueryTable queryTable = TstUtils.getTable(size, random, columnInfo);
        addGroupingValidator(queryTable, "queryTable");

        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> queryTable.head(0));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> queryTable.head(1));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return UpdateGraphProcessor.DEFAULT.exclusiveLock()
                                .computeLocked(() -> queryTable.update("intCol2 = intCol + 1"));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return UpdateGraphProcessor.DEFAULT.exclusiveLock()
                                .computeLocked(() -> queryTable.update("intCol2 = intCol + 1").select());
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return UpdateGraphProcessor.DEFAULT.exclusiveLock()
                                .computeLocked(() -> queryTable.view("Sym", "intCol2 = intCol + 1"));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return UpdateGraphProcessor.DEFAULT.exclusiveLock()
                                .computeLocked(() -> queryTable.avgBy("Sym").sort("Sym"));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> queryTable
                                .groupBy("Sym", "intCol").sort("Sym", "intCol").view("doubleCol=max(doubleCol)"));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> queryTable
                                .avgBy("Sym", "doubleCol").sort("Sym", "doubleCol").view("intCol=min(intCol)"));
                    }
                },
        };

        for (int ii = 0; ii < en.length; ++ii) {
            addGroupingValidator(en[ii].originalValue, "en[" + ii + "]");
        }

        Table by = UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> queryTable.avgBy("Sym"));
        addGroupingValidator(by, "groupBy");
        Table avgBy = UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> queryTable.avgBy("Sym"));
        addGroupingValidator(avgBy, "avgBy");
        Table avgBy1 =
                UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> queryTable.avgBy("Sym", "intCol"));
        addGroupingValidator(avgBy1, "avgBy1");

        Table merged = Require.neqNull(
                UpdateGraphProcessor.DEFAULT.exclusiveLock().computeLocked(() -> TableTools.merge(queryTable)),
                "TableTools.merge(queryTable)");
        addGroupingValidator(merged, "merged");
        Table updated = UpdateGraphProcessor.DEFAULT.exclusiveLock()
                .computeLocked(() -> merged.update("HiLo = intCol > 50 ? `Hi` : `Lo`"));
        addGroupingValidator(updated, "updated");

        final int maxSteps = numSteps.intValue(); // 8;

        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Initial QueryTable: ");
            TableTools.showWithRowSet(queryTable);
        }
        for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
            RefreshingTableTestCase.simulateShiftAwareStep("step == " + numSteps.intValue(), size, random, queryTable,
                    columnInfo, en);
        }

        // we don't need them after this test is done
        groupingValidators.clear();
    }

    // we don't ever need to look at the grouping validators, just make sure they don't go away
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private ArrayList<GroupingValidator> groupingValidators = new ArrayList<>();

    private void addGroupingValidator(Table originalValue, String context) {
        ArrayList<ArrayList<String>> columnSets2 = powerSet(originalValue.getColumnSourceMap().keySet());
        ArrayList<String> columnNames = new ArrayList<>();
        columnNames.addAll(originalValue.getColumnSourceMap().keySet());
        columnSets2.add(columnNames);
        groupingValidators.add(new GroupingValidator(context, originalValue, columnSets2));
    }

    public void testCombinedGrouping() throws IOException {
        Random random = new Random(0);
        int size = 100;

        TstUtils.ColumnInfo[] columnInfo = new TstUtils.ColumnInfo[4];
        // noinspection unchecked
        columnInfo[0] = new TstUtils.ColumnInfo(new TstUtils.SetGenerator<>("a", "b", "c", "d", "e", "f"), "Sym",
                TstUtils.ColumnInfo.ColAttributes.Immutable, TstUtils.ColumnInfo.ColAttributes.Grouped);
        // noinspection unchecked
        columnInfo[1] = new TstUtils.ColumnInfo(new TstUtils.SetGenerator<>("q", "r", "s", "t"), "Sym2",
                TstUtils.ColumnInfo.ColAttributes.Immutable, TstUtils.ColumnInfo.ColAttributes.Grouped);
        // noinspection unchecked
        columnInfo[2] = new TstUtils.ColumnInfo(new TstUtils.IntGenerator(10, 100), "intCol",
                TstUtils.ColumnInfo.ColAttributes.Immutable, TstUtils.ColumnInfo.ColAttributes.Grouped);
        // noinspection unchecked
        columnInfo[3] = new TstUtils.ColumnInfo(new TstUtils.SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");

        final QueryTable nonCountingTable = TstUtils.getTable(size, random, columnInfo);

        final QueryTable countingTable = CountingTable.getCountingTable(nonCountingTable);

        final ColumnSource symColumnSource = countingTable.getColumnSource("Sym");
        final ColumnSource sym2ColumnSource = countingTable.getColumnSource("Sym2");
        final ColumnSource intColumnSource = countingTable.getColumnSource("intCol");
        final ColumnSource doubleColumnSource = countingTable.getColumnSource("doubleCol");

        final RowSetIndexer indexer = RowSetIndexer.of(countingTable.getRowSet());

        assertTrue(indexer.hasGrouping(symColumnSource));
        assertTrue(indexer.hasGrouping(sym2ColumnSource));
        assertTrue(indexer.hasGrouping(intColumnSource));
        assertFalse(indexer.hasGrouping(intColumnSource, symColumnSource));
        assertFalse(indexer.hasGrouping(intColumnSource, symColumnSource, sym2ColumnSource));
        assertFalse(indexer.hasGrouping(intColumnSource, symColumnSource, doubleColumnSource));

        Map<Object, RowSet> symGrouping = indexer.getGrouping(symColumnSource);
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));

        GroupingValidator.validateGrouping(new String[] {"Sym"}, countingTable.getRowSet(), countingTable, "sym",
                symGrouping);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        Map<Object, RowSet> intGrouping = indexer.getGrouping(intColumnSource);
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        GroupingValidator.validateGrouping(new String[] {"intCol"}, countingTable.getRowSet(), countingTable, "intCol",
                intGrouping);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        final TupleSource intSymTupleSource = TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource);

        Map<Object, RowSet> intSymGrouping = indexer.getGrouping(intSymTupleSource);
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        GroupingValidator.validateGrouping(new String[] {"intCol", "Sym"}, countingTable.getRowSet(), countingTable,
                "intCol+sym", intSymGrouping);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        final TupleSource intSymSym2TupleSource =
                TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource, sym2ColumnSource);
        Map<Object, RowSet> intSymSym2Grouping = indexer.getGrouping(intSymSym2TupleSource);
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) sym2ColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        GroupingValidator.validateGrouping(new String[] {"intCol", "Sym", "Sym2"}, countingTable.getRowSet(),
                countingTable, "intCol+sym+sym2", intSymSym2Grouping);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        final TupleSource intSymDoubleTupleSource =
                TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource, doubleColumnSource);
        Map<Object, RowSet> intSymDoubleGrouping = indexer.getGrouping(intSymDoubleTupleSource);
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        TestCase.assertEquals(countingTable.size(),
                ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("getDouble"));
        GroupingValidator.validateGrouping(new String[] {"intCol", "Sym", "doubleCol"}, countingTable.getRowSet(),
                countingTable, "intCol+sym+doubleCol", intSymDoubleGrouping);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        final TupleSource intSymSym2DoubleTupleSource = TupleSourceFactory.makeTupleSource(intColumnSource,
                symColumnSource, sym2ColumnSource, doubleColumnSource);
        Map<Object, RowSet> intSymSym2DoubleGrouping =
                indexer.getGrouping(intSymSym2DoubleTupleSource);
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) sym2ColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        TestCase.assertEquals(countingTable.size(),
                ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("getDouble"));
        GroupingValidator.validateGrouping(new String[] {"intCol", "Sym", "Sym2", "doubleCol"},
                countingTable.getRowSet(), countingTable, "intCol+sym+sym2+doubleCol", intSymSym2DoubleGrouping);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());
    }

    public void testRestrictedGrouping() throws IOException {
        Random random = new Random(0);
        int size = 100;

        TstUtils.ColumnInfo[] columnInfo = new TstUtils.ColumnInfo[4];
        // noinspection unchecked
        columnInfo[0] = new TstUtils.ColumnInfo(new TstUtils.SetGenerator<>("a", "b", "c", "d", "e", "f"), "Sym",
                TstUtils.ColumnInfo.ColAttributes.Immutable, TstUtils.ColumnInfo.ColAttributes.Grouped);
        // noinspection unchecked
        columnInfo[1] = new TstUtils.ColumnInfo(new TstUtils.SetGenerator<>("q", "r", "s", "t", "u", "v"), "Sym2",
                TstUtils.ColumnInfo.ColAttributes.Immutable, TstUtils.ColumnInfo.ColAttributes.Grouped);
        // noinspection unchecked
        columnInfo[2] = new TstUtils.ColumnInfo(new TstUtils.IntGenerator(10, 100), "intCol",
                TstUtils.ColumnInfo.ColAttributes.Immutable, TstUtils.ColumnInfo.ColAttributes.Grouped);
        // noinspection unchecked
        columnInfo[3] = new TstUtils.ColumnInfo(new TstUtils.SetGenerator<>(10.1, 20.1, 30.1), "doubleCol");

        final QueryTable nonCountingTable = TstUtils.getTable(size, random, columnInfo);

        final QueryTable countingTable = CountingTable.getCountingTable(nonCountingTable);

        final ColumnSource symColumnSource = countingTable.getColumnSource("Sym");
        final ColumnSource sym2ColumnSource = countingTable.getColumnSource("Sym2");
        final ColumnSource intColumnSource = countingTable.getColumnSource("intCol");
        final ColumnSource doubleColumnSource = countingTable.getColumnSource("doubleCol");

        final RowSetIndexer indexer = RowSetIndexer.of(countingTable.getRowSet());

        assertTrue(indexer.hasGrouping(symColumnSource));
        assertTrue(indexer.hasGrouping(sym2ColumnSource));
        assertTrue(indexer.hasGrouping(intColumnSource));
        assertFalse(indexer.hasGrouping(intColumnSource, symColumnSource));
        assertFalse(indexer.hasGrouping(intColumnSource, symColumnSource, sym2ColumnSource));
        assertFalse(indexer.hasGrouping(intColumnSource, symColumnSource, sym2ColumnSource,
                doubleColumnSource));
        assertFalse(indexer.hasGrouping(intColumnSource, symColumnSource, doubleColumnSource));

        final TreeSet<Object> keySet = new TreeSet<>(Arrays.asList("a", "b"));
        final Map<Object, RowSet> symGrouping = indexer.getGroupingForKeySet(keySet, symColumnSource);
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));

        GroupingValidator.validateRestrictedGrouping(new String[] {"Sym"}, countingTable.getRowSet(), countingTable,
                "sym", symGrouping, keySet);
        ((CountingTable.MethodCounter) symColumnSource).clear();
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        keySet.clear();
        keySet.addAll(Arrays.asList(10, 20, 30, 40, 50, 60, 70, 80, 90));
        final Map<Object, RowSet> intGrouping = indexer.getGroupingForKeySet(keySet, intColumnSource);
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        GroupingValidator.validateRestrictedGrouping(new String[] {"intCol"}, countingTable.getRowSet(), countingTable,
                "intCol", intGrouping, keySet);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        keySet.clear();
        final TupleSource intSymFactory = TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource);
        TstUtils.selectSubIndexSet(5, countingTable.getRowSet(), random)
                .forAllRowKeys(row -> keySet.add(intSymFactory.createTuple(row)));
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        final Map<Object, RowSet> intSymGrouping = indexer.getGroupingForKeySet(keySet,
                TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        GroupingValidator.validateRestrictedGrouping(new String[] {"intCol", "Sym"}, countingTable.getRowSet(),
                countingTable, "intCol+sym", intSymGrouping, keySet);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        keySet.clear();
        final TupleSource intSymDoubleFactory =
                TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource, doubleColumnSource);
        TstUtils.selectSubIndexSet(5, countingTable.getRowSet(), random)
                .forAllRowKeys(row -> keySet.add(intSymDoubleFactory.createTuple(row)));
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        final Map<Object, RowSet> intSymDoubleGrouping = indexer.getGroupingForKeySet(keySet,
                TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource, doubleColumnSource));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        long groupingSize = intSymDoubleGrouping.values().stream().mapToLong(RowSet::size).sum();
        TestCase.assertEquals(groupingSize, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("getDouble"));
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        GroupingValidator.validateRestrictedGrouping(new String[] {"intCol", "Sym", "doubleCol"},
                countingTable.getRowSet(), countingTable, "intCol+sym+doubleCol", intSymDoubleGrouping, keySet);

        keySet.clear();
        final TupleSource intSymSym2DoubleFactory = TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource,
                sym2ColumnSource, doubleColumnSource);
        TstUtils.selectSubIndexSet(5, countingTable.getRowSet(), random)
                .forAllRowKeys(row -> keySet.add(intSymSym2DoubleFactory.createTuple(row)));
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        final Map<Object, RowSet> intSymSym2DoubleGrouping =
                indexer.getGroupingForKeySet(keySet, TupleSourceFactory
                        .makeTupleSource(intColumnSource, symColumnSource, sym2ColumnSource, doubleColumnSource));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) sym2ColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        groupingSize = intSymSym2DoubleGrouping.values().stream().mapToLong(RowSet::size).sum();
        TestCase.assertEquals(groupingSize, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("get"));
        TestCase.assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("getDouble"));
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        GroupingValidator.validateRestrictedGrouping(new String[] {"intCol", "Sym", "Sym2", "doubleCol"},
                countingTable.getRowSet(), countingTable, "intCol+sym+sym2+doubleCol", intSymSym2DoubleGrouping,
                keySet);
    }
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.base.verify.Require;
import io.deephaven.compilertools.CompilerTools;
import io.deephaven.configuration.Configuration;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.tables.utils.TableTools;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.tuples.TupleSource;
import io.deephaven.db.v2.tuples.TupleSourceFactory;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.UpdatePerformanceTracker;
import io.deephaven.test.types.OutOfBandTest;
import org.apache.commons.lang3.mutable.MutableInt;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import org.junit.experimental.categories.Category;

import static io.deephaven.db.tables.utils.TableTools.show;
import static io.deephaven.db.v2.TstUtils.getTable;

@SuppressWarnings("ClassInitializerMayBeStatic")
@Category(OutOfBandTest.class)
public class IndexGroupingTest extends LiveTableTestCase {

    private static final boolean ENABLE_COMPILER_TOOLS_LOGGING = Configuration.getInstance()
            .getBooleanForClassWithDefault(IndexGroupingTest.class, "CompilerTools.logEnabled", false);

    private boolean oldCompilerToolsLogEnabled;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        UpdatePerformanceTracker.getInstance().enableUnitTestMode();
        oldCompilerToolsLogEnabled = CompilerTools.setLogEnabled(ENABLE_COMPILER_TOOLS_LOGGING);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        CompilerTools.setLogEnabled(oldCompilerToolsLogEnabled);
        super.tearDown();
    }

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

        final QueryTable queryTable = getTable(size, random, columnInfo);
        addGroupingValidator(queryTable, "queryTable");

        final EvalNugget[] en = new EvalNugget[] {
                new EvalNugget() {
                    public Table e() {
                        return LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> queryTable.head(0));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> queryTable.head(1));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return LiveTableMonitor.DEFAULT.exclusiveLock()
                                .computeLocked(() -> queryTable.update("intCol2 = intCol + 1"));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return LiveTableMonitor.DEFAULT.exclusiveLock()
                                .computeLocked(() -> queryTable.update("intCol2 = intCol + 1").select());
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return LiveTableMonitor.DEFAULT.exclusiveLock()
                                .computeLocked(() -> queryTable.view("Sym", "intCol2 = intCol + 1"));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return LiveTableMonitor.DEFAULT.exclusiveLock()
                                .computeLocked(() -> queryTable.avgBy("Sym").sort("Sym"));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> queryTable
                                .by("Sym", "intCol").sort("Sym", "intCol").view("doubleCol=max(doubleCol)"));
                    }
                },
                new EvalNugget() {
                    public Table e() {
                        return LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> queryTable
                                .avgBy("Sym", "doubleCol").sort("Sym", "doubleCol").view("intCol=min(intCol)"));
                    }
                },
        };

        for (int ii = 0; ii < en.length; ++ii) {
            addGroupingValidator((DynamicTable) en[ii].originalValue, "en[" + ii + "]");
        }

        Table by = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> queryTable.avgBy("Sym"));
        addGroupingValidator((DynamicTable) by, "by");
        Table avgBy = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> queryTable.avgBy("Sym"));
        addGroupingValidator((DynamicTable) avgBy, "avgBy");
        Table avgBy1 = LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> queryTable.avgBy("Sym", "intCol"));
        addGroupingValidator((DynamicTable) avgBy1, "avgBy1");

        Table merged = Require.neqNull(
                LiveTableMonitor.DEFAULT.exclusiveLock().computeLocked(() -> TableTools.merge(queryTable)),
                "TableTools.merge(queryTable)");
        addGroupingValidator((DynamicTable) merged, "merged");
        Table updated = LiveTableMonitor.DEFAULT.exclusiveLock()
                .computeLocked(() -> merged.update("HiLo = intCol > 50 ? `Hi` : `Lo`"));
        addGroupingValidator((DynamicTable) updated, "updated");

        final int maxSteps = numSteps.intValue(); // 8;

        if (LiveTableTestCase.printTableUpdates) {
            System.out.println("Initial QueryTable: ");
            TableTools.showWithIndex(queryTable);
        }
        for (numSteps.setValue(0); numSteps.intValue() < maxSteps; numSteps.increment()) {
            simulateShiftAwareStep("step == " + numSteps.intValue(), size, random, queryTable, columnInfo, en);
        }

        // we don't need them after this test is done
        groupingValidators.clear();
    }

    // we don't ever need to look at the grouping validators, just make sure they don't go away
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    private ArrayList<GroupingValidator> groupingValidators = new ArrayList<>();

    private void addGroupingValidator(DynamicTable originalValue, String context) {
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

        final QueryTable nonCountingTable = getTable(size, random, columnInfo);

        final QueryTable countingTable = CountingTable.getCountingTable(nonCountingTable);

        final ColumnSource symColumnSource = countingTable.getColumnSource("Sym");
        final ColumnSource sym2ColumnSource = countingTable.getColumnSource("Sym2");
        final ColumnSource intColumnSource = countingTable.getColumnSource("intCol");
        final ColumnSource doubleColumnSource = countingTable.getColumnSource("doubleCol");


        // noinspection unchecked
        assertTrue(countingTable.getIndex().hasGrouping(symColumnSource));
        assertTrue(countingTable.getIndex().hasGrouping(sym2ColumnSource));
        assertTrue(countingTable.getIndex().hasGrouping(intColumnSource));
        assertFalse(countingTable.getIndex().hasGrouping(intColumnSource, symColumnSource));
        assertFalse(countingTable.getIndex().hasGrouping(intColumnSource, symColumnSource, sym2ColumnSource));
        assertFalse(countingTable.getIndex().hasGrouping(intColumnSource, symColumnSource, doubleColumnSource));

        Map<Object, Index> symGrouping = countingTable.getIndex().getGrouping(symColumnSource);
        assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));

        GroupingValidator.validateGrouping(new String[] {"Sym"}, countingTable.getIndex(), countingTable, "sym",
                symGrouping);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        Map<Object, Index> intGrouping = countingTable.getIndex().getGrouping(intColumnSource);
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        GroupingValidator.validateGrouping(new String[] {"intCol"}, countingTable.getIndex(), countingTable, "intCol",
                intGrouping);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        final TupleSource intSymTupleSource = TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource);

        Map<Object, Index> intSymGrouping = countingTable.getIndex().getGrouping(intSymTupleSource);
        assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        GroupingValidator.validateGrouping(new String[] {"intCol", "Sym"}, countingTable.getIndex(), countingTable,
                "intCol+sym", intSymGrouping);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        final TupleSource intSymSym2TupleSource =
                TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource, sym2ColumnSource);
        Map<Object, Index> intSymSym2Grouping = countingTable.getIndex().getGrouping(intSymSym2TupleSource);
        assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) sym2ColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        GroupingValidator.validateGrouping(new String[] {"intCol", "Sym", "Sym2"}, countingTable.getIndex(),
                countingTable, "intCol+sym+sym2", intSymSym2Grouping);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        final TupleSource intSymDoubleTupleSource =
                TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource, doubleColumnSource);
        Map<Object, Index> intSymDoubleGrouping = countingTable.getIndex().getGrouping(intSymDoubleTupleSource);
        assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        assertEquals(countingTable.size(), ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("getDouble"));
        GroupingValidator.validateGrouping(new String[] {"intCol", "Sym", "doubleCol"}, countingTable.getIndex(),
                countingTable, "intCol+sym+doubleCol", intSymDoubleGrouping);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        final TupleSource intSymSym2DoubleTupleSource = TupleSourceFactory.makeTupleSource(intColumnSource,
                symColumnSource, sym2ColumnSource, doubleColumnSource);
        Map<Object, Index> intSymSym2DoubleGrouping = countingTable.getIndex().getGrouping(intSymSym2DoubleTupleSource);
        assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) sym2ColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        assertEquals(countingTable.size(), ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("getDouble"));
        GroupingValidator.validateGrouping(new String[] {"intCol", "Sym", "Sym2", "doubleCol"},
                countingTable.getIndex(), countingTable, "intCol+sym+sym2+doubleCol", intSymSym2DoubleGrouping);
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

        final QueryTable nonCountingTable = getTable(size, random, columnInfo);

        final QueryTable countingTable = CountingTable.getCountingTable(nonCountingTable);

        final ColumnSource symColumnSource = countingTable.getColumnSource("Sym");
        final ColumnSource sym2ColumnSource = countingTable.getColumnSource("Sym2");
        final ColumnSource intColumnSource = countingTable.getColumnSource("intCol");
        final ColumnSource doubleColumnSource = countingTable.getColumnSource("doubleCol");


        // noinspection unchecked
        assertTrue(countingTable.getIndex().hasGrouping(symColumnSource));
        assertTrue(countingTable.getIndex().hasGrouping(sym2ColumnSource));
        assertTrue(countingTable.getIndex().hasGrouping(intColumnSource));
        assertFalse(countingTable.getIndex().hasGrouping(intColumnSource, symColumnSource));
        assertFalse(countingTable.getIndex().hasGrouping(intColumnSource, symColumnSource, sym2ColumnSource));
        assertFalse(countingTable.getIndex().hasGrouping(intColumnSource, symColumnSource, sym2ColumnSource,
                doubleColumnSource));
        assertFalse(countingTable.getIndex().hasGrouping(intColumnSource, symColumnSource, doubleColumnSource));

        final TreeSet<Object> keySet = new TreeSet<>(Arrays.asList("a", "b"));
        final Map<Object, Index> symGrouping = countingTable.getIndex().getGroupingForKeySet(keySet, symColumnSource);
        assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));

        GroupingValidator.validateRestrictedGrouping(new String[] {"Sym"}, countingTable.getIndex(), countingTable,
                "sym", symGrouping, keySet);
        ((CountingTable.MethodCounter) symColumnSource).clear();
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        keySet.clear();
        keySet.addAll(Arrays.asList(10, 20, 30, 40, 50, 60, 70, 80, 90));
        final Map<Object, Index> intGrouping = countingTable.getIndex().getGroupingForKeySet(keySet, intColumnSource);
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        GroupingValidator.validateRestrictedGrouping(new String[] {"intCol"}, countingTable.getIndex(), countingTable,
                "intCol", intGrouping, keySet);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        keySet.clear();
        final TupleSource intSymFactory = TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource);
        TstUtils.selectSubIndexSet(5, countingTable.getIndex(), random)
                .forEach(row -> keySet.add(intSymFactory.createTuple(row)));
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        final Map<Object, Index> intSymGrouping = countingTable.getIndex().getGroupingForKeySet(keySet,
                TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource));
        assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        GroupingValidator.validateRestrictedGrouping(new String[] {"intCol", "Sym"}, countingTable.getIndex(),
                countingTable, "intCol+sym", intSymGrouping, keySet);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        keySet.clear();
        final TupleSource intSymDoubleFactory =
                TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource, doubleColumnSource);
        TstUtils.selectSubIndexSet(5, countingTable.getIndex(), random)
                .forEach(row -> keySet.add(intSymDoubleFactory.createTuple(row)));
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        final Map<Object, Index> intSymDoubleGrouping = countingTable.getIndex().getGroupingForKeySet(keySet,
                TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource, doubleColumnSource));
        assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        long groupingSize = intSymDoubleGrouping.values().stream().mapToLong(Index::size).sum();
        assertEquals(groupingSize, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("getDouble"));
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        GroupingValidator.validateRestrictedGrouping(new String[] {"intCol", "Sym", "doubleCol"},
                countingTable.getIndex(), countingTable, "intCol+sym+doubleCol", intSymDoubleGrouping, keySet);

        keySet.clear();
        final TupleSource intSymSym2DoubleFactory = TupleSourceFactory.makeTupleSource(intColumnSource, symColumnSource,
                sym2ColumnSource, doubleColumnSource);
        TstUtils.selectSubIndexSet(5, countingTable.getIndex(), random)
                .forEach(row -> keySet.add(intSymSym2DoubleFactory.createTuple(row)));
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        final Map<Object, Index> intSymSym2DoubleGrouping =
                countingTable.getIndex().getGroupingForKeySet(keySet, TupleSourceFactory
                        .makeTupleSource(intColumnSource, symColumnSource, sym2ColumnSource, doubleColumnSource));
        assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) sym2ColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        groupingSize = intSymSym2DoubleGrouping.values().stream().mapToLong(Index::size).sum();
        assertEquals(groupingSize, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("getDouble"));
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        GroupingValidator.validateRestrictedGrouping(new String[] {"intCol", "Sym", "Sym2", "doubleCol"},
                countingTable.getIndex(), countingTable, "intCol+sym+sym2+doubleCol", intSymSym2DoubleGrouping, keySet);
    }

}

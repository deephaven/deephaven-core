//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Selectable;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.context.QueryScope;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.indexer.DataIndexer;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.util.PrintListener;
import io.deephaven.engine.util.TableTools;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.test.types.OutOfBandTest;
import junit.framework.TestCase;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.util.*;
import java.util.stream.IntStream;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.util.TableTools.*;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

@Category(OutOfBandTest.class)
public class QueryTableWhereInTest {
    private final Logger log = LoggerFactory.getLogger(QueryTableWhereInTest.class);

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private boolean oldParallel;
    private boolean oldDisable;
    private int oldSegments;
    private long oldSize;

    @Before
    public void setUp() throws Exception {
        oldParallel = QueryTable.FORCE_PARALLEL_WHERE;
        oldDisable = QueryTable.DISABLE_PARALLEL_WHERE;
        oldSegments = QueryTable.PARALLEL_WHERE_SEGMENTS;
        oldSize = QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT;

        // we'll run this in parallel
        QueryTable.FORCE_PARALLEL_WHERE = true;
        QueryTable.DISABLE_PARALLEL_WHERE = false;
    }

    @After
    public void tearDown() throws Exception {
        QueryTable.FORCE_PARALLEL_WHERE = oldParallel;
        QueryTable.DISABLE_PARALLEL_WHERE = oldDisable;
        QueryTable.PARALLEL_WHERE_SEGMENTS = oldSegments;
        QueryTable.PARALLEL_WHERE_ROWS_PER_SEGMENT = oldSize;
    }

    @Test
    public void testWhereInDependency() {
        testWhereInDependencyInternal(false, false, true, true);
        testWhereInDependencyInternal(false, false, true, false);
        testWhereInDependencyInternal(false, false, false, true);
        testWhereInDependencyInternal(false, false, false, false);
    }

    @Test
    public void testWhereInDependencyIndexed() {
        testWhereInDependencyInternal(true, false, true, true);
        testWhereInDependencyInternal(true, false, true, false);
        testWhereInDependencyInternal(true, false, false, true);
        testWhereInDependencyInternal(true, false, false, false);

        testWhereInDependencyInternal(false, true, true, true);
        testWhereInDependencyInternal(false, true, true, false);
        testWhereInDependencyInternal(false, true, false, true);
        testWhereInDependencyInternal(false, true, false, false);

        testWhereInDependencyInternal(true, true, true, true);
        testWhereInDependencyInternal(true, true, true, false);
        testWhereInDependencyInternal(true, true, false, true);
        testWhereInDependencyInternal(true, true, false, false);
    }

    private void testWhereInDependencyInternal(
            boolean filterIndexed,
            boolean setIndexed,
            boolean sourceRefreshing,
            boolean setRefreshing) {

        final QueryTable tableToFilter = sourceRefreshing
                ? testRefreshingTable(i(10, 11, 12, 13, 14, 15).toTracking(), col("A", 1, 2, 3, 4, 5, 6),
                        col("B", 2, 4, 6, 8, 10, 12), col("C", 'a', 'b', 'c', 'd', 'e', 'f'))
                : testTable(i(10, 11, 12, 13, 14, 15).toTracking(), col("A", 1, 2, 3, 4, 5, 6),
                        col("B", 2, 4, 6, 8, 10, 12), col("C", 'a', 'b', 'c', 'd', 'e', 'f'));
        if (filterIndexed) {
            DataIndexer.getOrCreateDataIndex(tableToFilter, "A");
            DataIndexer.getOrCreateDataIndex(tableToFilter, "B");
        }

        final QueryTable setTable = setRefreshing
                ? testRefreshingTable(i(100, 101, 102).toTracking(), col("A", 1, 2, 3), col("B", 2, 4, 6))
                : testTable(i(100, 101, 102).toTracking(), col("A", 1, 2, 3), col("B", 2, 4, 6));
        final Table setTable1 = setTable.where("A > 2");
        final Table setTable2 = setTable.where("B > 6");
        if (setIndexed) {
            DataIndexer.getOrCreateDataIndex(setTable, "A");
            DataIndexer.getOrCreateDataIndex(setTable, "B");

            DataIndexer.getOrCreateDataIndex(setTable1, "A");
            DataIndexer.getOrCreateDataIndex(setTable1, "B");

            DataIndexer.getOrCreateDataIndex(setTable2, "A");
            DataIndexer.getOrCreateDataIndex(setTable2, "B");
        }

        final DynamicWhereFilter dynamicFilter1 =
                new DynamicWhereFilter((QueryTable) setTable1, true, MatchPairFactory.getExpressions("A"));
        final DynamicWhereFilter dynamicFilter2 =
                new DynamicWhereFilter((QueryTable) setTable2, true, MatchPairFactory.getExpressions("B"));

        final WhereFilter composedFilter = DisjunctiveFilter.makeDisjunctiveFilter(dynamicFilter1, dynamicFilter2);
        final Table composed = tableToFilter.where(composedFilter);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(() -> {
            TestCase.assertTrue(dynamicFilter1.satisfied(updateGraph.clock().currentStep()));
            TestCase.assertTrue(dynamicFilter2.satisfied(updateGraph.clock().currentStep()));
            TestCase.assertTrue(composed.satisfied(updateGraph.clock().currentStep()));
        });

        if (setRefreshing) {
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(setTable, i(103), col("A", 5), col("B", 8));
                setTable.notifyListeners(i(103), i(), i());
            });

            TableTools.show(composed);

            final Table expected =
                    TableTools.newTable(intCol("A", 3, 4, 5), intCol("B", 6, 8, 10), charCol("C", 'c', 'd', 'e'));

            assertTableEquals(composed, expected);
        }
    }

    @Test
    public void testWhereInOptimalIndexSelectionWithNoneAvailable() {
        final Table lhs = emptyTable(10).update("Part=ii % 2 == 0 ? `Apple` : `Pie`", "Hello=ii", "Goodbye = `A`+ii");
        DataIndexer.getOrCreateDataIndex(lhs, "Part");
        final Table rhs = emptyTable(2).update("Hello=ii", "Goodbye = `A`+ii");
        final Table result = lhs.whereIn(rhs, "Goodbye");
        assertTableEquals(lhs.head(2), result);
    }

    @Test
    public void testWhereDynamicIn() {
        final QueryTable setTable = testRefreshingTable(i(2, 4, 6, 8).toTracking(), col("X", "A", "B", "C", "B"));
        final QueryTable filteredTable = testRefreshingTable(i(1, 2, 3, 4, 5).toTracking(),
                col("X", "A", "B", "C", "D", "E"));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final Table result = updateGraph.exclusiveLock().computeLocked(
                () -> filteredTable.whereIn(setTable, "X"));
        final Table resultInverse = updateGraph.exclusiveLock().computeLocked(
                () -> filteredTable.whereNotIn(setTable, "X"));
        show(result);
        assertEquals(3, result.size());
        assertArrayEquals(new String[] {"A", "B", "C"}, ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertEquals(2, resultInverse.size());
        assertArrayEquals(new String[] {"D", "E"}, ColumnVectors.ofObject(resultInverse, "X", String.class).toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(filteredTable, i(6), col("X", "A"));
            filteredTable.notifyListeners(i(6), i(), i());
        });
        show(result);
        assertEquals(4, result.size());
        assertArrayEquals(new String[] {"A", "B", "C", "A"},
                ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertEquals(2, resultInverse.size());
        assertArrayEquals(new String[] {"D", "E"}, ColumnVectors.ofObject(resultInverse, "X", String.class).toArray());

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(setTable, i(7), col("X", "D"));
            setTable.notifyListeners(i(7), i(), i());
        });
        showWithRowSet(result);
        assertEquals(5, result.size());
        assertArrayEquals(new String[] {"A", "B", "C", "D", "A"},
                ColumnVectors.ofObject(result, "X", String.class).toArray());
        assertEquals(1, resultInverse.size());
        assertArrayEquals(new String[] {"E"}, ColumnVectors.ofObject(resultInverse, "X", String.class).toArray());

        // Real modification to set table, followed by spurious modification to set table
        IntStream.range(0, 2).forEach(ri -> {
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(setTable, i(7), col("X", "C"));
                setTable.notifyListeners(i(), i(), i(7));
            });
            showWithRowSet(result);
            assertEquals(4, result.size());
            assertArrayEquals(new String[] {"A", "B", "C", "A"},
                    ColumnVectors.ofObject(result, "X", String.class).toArray());
            assertEquals(2, resultInverse.size());
            assertArrayEquals(new String[] {"D", "E"},
                    ColumnVectors.ofObject(resultInverse, "X", String.class).toArray());
        });
    }

    @Test
    public void testWhereDynamicInIncremental() {
        testWhereDynamicIncrementalInternal(false, false, true, true);
        testWhereDynamicIncrementalInternal(false, false, true, false);
        testWhereDynamicIncrementalInternal(false, false, false, true);
        testWhereDynamicIncrementalInternal(false, false, false, false);
    }

    @Test
    public void testWhereDynamicInIncrementalIndexed() {
        testWhereDynamicIncrementalInternal(true, false, true, true);
        testWhereDynamicIncrementalInternal(true, false, true, false);
        testWhereDynamicIncrementalInternal(true, false, false, true);
        testWhereDynamicIncrementalInternal(true, false, false, false);

        testWhereDynamicIncrementalInternal(false, true, true, true);
        testWhereDynamicIncrementalInternal(false, true, true, false);
        testWhereDynamicIncrementalInternal(false, true, false, true);
        testWhereDynamicIncrementalInternal(false, true, false, false);

        testWhereDynamicIncrementalInternal(true, true, true, true);
        testWhereDynamicIncrementalInternal(true, true, true, false);
        testWhereDynamicIncrementalInternal(true, true, false, true);
        testWhereDynamicIncrementalInternal(true, true, false, false);
    }

    private static void testWhereDynamicIncrementalInternal(
            boolean filterIndexed,
            boolean setIndexed,
            boolean sourceRefreshing,
            boolean setRefreshing) {
        final ColumnInfo<?, ?>[] setInfo;
        final ColumnInfo<?, ?>[] filteredInfo;

        final int setSize = 100;
        final int filteredSize = 5000;
        final Random random = new Random(0);

        final String[] columnNames =
                new String[] {"Sym", "intCol", "doubleCol", "charCol", "byteCol", "floatCol", "longCol", "shortCol"};

        final QueryTable setTable = getTable(setRefreshing, setSize, random, setInfo = initColumnInfos(
                columnNames,
                new SetGenerator<>("aa", "bb"),
                new IntGenerator(0, 10),
                new DoubleGenerator(0, 100),
                new SetGenerator<>('a', 'b', 'c', 'd', 'e', 'f'),
                new ByteGenerator((byte) 0, (byte) 64),
                new SetGenerator<>(1.0f, 2.0f, 3.3f, null),
                new LongGenerator(0, 1000),
                new ShortGenerator((short) 500, (short) 600)));
        if (setIndexed) {
            // Add an index on every column but "doubleCol"
            for (final String columnName : columnNames) {
                if (!columnName.equals("doubleCol")) {
                    DataIndexer.getOrCreateDataIndex(setTable, columnName);
                }
            }
            // Add the multi-column index for "Sym", "intCol"
            DataIndexer.getOrCreateDataIndex(setTable, "Sym", "intCol");
        }

        final QueryTable filteredTable = getTable(sourceRefreshing, filteredSize, random,
                filteredInfo = initColumnInfos(
                        columnNames,
                        new SetGenerator<>("aa", "bb", "cc", "dd"),
                        new IntGenerator(0, 20),
                        new DoubleGenerator(0, 100),
                        new CharGenerator('a', 'z'),
                        new ByteGenerator((byte) 0, (byte) 127),
                        new SetGenerator<>(1.0f, 2.0f, 3.3f, null, 4.4f, 5.5f, 6.6f),
                        new LongGenerator(1500, 2500),
                        new ShortGenerator((short) 400, (short) 700)));

        if (filterIndexed) {
            // Add an index on every column but "doubleCol"
            for (final String columnName : columnNames) {
                if (!columnName.equals("doubleCol")) {
                    DataIndexer.getOrCreateDataIndex(filteredTable, columnName);
                }
            }
            // Add the multi-column index for "Sym", "intCol"
            DataIndexer.getOrCreateDataIndex(filteredTable, "Sym", "intCol");
        }

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "Sym")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "Sym")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "Sym", "intCol")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "intCol", "Sym")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "charCol")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "byteCol")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "shortCol")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "intCol")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "longCol")),
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "floatCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "Sym", "intCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "intCol", "Sym")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "charCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "byteCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "shortCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "intCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "longCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "floatCol")),
        };

        validate(en);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < 100; step++) {
            final boolean modSet = random.nextInt(10) < 1;
            final boolean modFiltered = random.nextBoolean();

            if (setRefreshing) {
                updateGraph.runWithinUnitTestCycle(() -> {
                    if (modSet) {
                        GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                                setSize, random, setTable, setInfo);
                    }
                });
                validate(en);
            }

            if (sourceRefreshing) {
                updateGraph.runWithinUnitTestCycle(() -> {
                    if (modFiltered) {
                        GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                                filteredSize, random, filteredTable, filteredInfo);
                    }
                });
                validate(en);
            }
        }
    }

    @Test
    public void testWhereInDynamicPartial() {
        testWhereInDynamicPartialIndexedInternal(true);
        testWhereInDynamicPartialIndexedInternal(false);
    }

    private void testWhereInDynamicPartialIndexedInternal(final boolean setRefreshing) {
        final ColumnInfo<?, ?>[] setInfo;
        final ColumnInfo<?, ?>[] filteredInfo;

        final int setSize = 100;
        final int filteredSize = 5000;
        final Random random = new Random(0);

        final String[] columnNames =
                new String[] {"Sym", "intCol", "doubleCol", "charCol", "byteCol", "floatCol", "longCol", "shortCol"};

        final QueryTable setTable = getTable(setRefreshing, setSize, random, setInfo = initColumnInfos(
                columnNames,
                new SetGenerator<>("aa", "bb"),
                new IntGenerator(0, 10),
                new DoubleGenerator(0, 100),
                new SetGenerator<>('a', 'b', 'c', 'd', 'e', 'f'),
                new ByteGenerator((byte) 0, (byte) 64),
                new SetGenerator<>(1.0f, 2.0f, 3.3f, null),
                new LongGenerator(0, 1000),
                new ShortGenerator((short) 500, (short) 600)));

        final QueryTable filteredTable = getTable(filteredSize, random, filteredInfo = initColumnInfos(
                columnNames,
                new SetGenerator<>("aa", "bb", "cc", "dd"),
                new IntGenerator(0, 20),
                new DoubleGenerator(0, 100),
                new CharGenerator('a', 'z'),
                new ByteGenerator((byte) 0, (byte) 127),
                new SetGenerator<>(1.0f, 2.0f, 3.3f, null, 4.4f, 5.5f, 6.6f),
                new LongGenerator(1500, 2500),
                new ShortGenerator((short) 400, (short) 700)));

        DataIndexer.getOrCreateDataIndex(filteredTable, "Sym");
        DataIndexer.getOrCreateDataIndex(filteredTable, "Sym", "charCol");
        DataIndexer.getOrCreateDataIndex(filteredTable, "Sym", "charCol", "longCol");
        DataIndexer.getOrCreateDataIndex(filteredTable, "Sym", "charCol", "longCol", "shortCol");

        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> filteredTable.whereIn(setTable, "Sym", "intCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "Sym", "intCol")),

                EvalNugget.from(() -> filteredTable.whereIn(setTable, "intCol", "Sym")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "intCol", "Sym")),

                EvalNugget.from(() -> filteredTable.whereIn(setTable, "Sym", "charCol", "intCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "Sym", "charCol", "intCol")),

                EvalNugget.from(() -> filteredTable.whereIn(setTable, "intCol", "charCol", "Sym")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "intCol", "charCol", "Sym")),

                EvalNugget.from(() -> filteredTable.whereIn(setTable, "Sym", "charCol", "longCol", "byteCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "Sym", "charCol", "longCol", "byteCol")),

                EvalNugget.from(() -> filteredTable.whereIn(setTable, "charCol", "Sym", "byteCol", "longCol")),
                EvalNugget.from(() -> filteredTable.whereNotIn(setTable, "charCol", "Sym", "byteCol", "longCol")),

                EvalNugget.from(
                        () -> filteredTable.whereIn(setTable, "Sym", "charCol", "longCol", "shortCol", "byteCol")),
                EvalNugget.from(
                        () -> filteredTable.whereNotIn(setTable, "Sym", "charCol", "longCol", "shortCol", "byteCol")),

                EvalNugget.from(
                        () -> filteredTable.whereIn(setTable, "charCol", "Sym", "byteCol", "longCol", "shortCol")),
                EvalNugget.from(
                        () -> filteredTable.whereNotIn(setTable, "charCol", "Sym", "byteCol", "longCol", "shortCol")),

        };

        validate(en);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (int step = 0; step < 100; step++) {
            final boolean modSet = random.nextInt(10) < 1;
            final boolean modFiltered = random.nextBoolean();

            if (setRefreshing) {
                updateGraph.runWithinUnitTestCycle(() -> {
                    if (modSet) {
                        GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                                setSize, random, setTable, setInfo);
                    }
                });
                validate(en);
            }

            updateGraph.runWithinUnitTestCycle(() -> {
                if (modFiltered) {
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                            filteredSize, random, filteredTable, filteredInfo);
                }
            });
            validate(en);
        }
    }

    @Test
    public void testWhereRefresh() {
        final Table t1 = TableTools.newTable(col("A", "b", "c", "d"));
        assertFalse(t1.isRefreshing());
        final Table t2 = ExecutionContext.getContext().getUpdateGraph().exclusiveLock()
                .computeLocked(() -> t1.where("A in `b`"));
        assertFalse(t2.isRefreshing());
        final Table t3 =
                ExecutionContext.getContext().getUpdateGraph().exclusiveLock().computeLocked(() -> t1.whereIn(t1, "A"));
        assertFalse(t3.isRefreshing());

        final Random random = new Random(0);
        final QueryTable t4 = getTable(10, random, initColumnInfos(new String[] {"B"}, new SetGenerator<>("a", "b")));
        assertTrue(t4.isRefreshing());
        final Table t5 = ExecutionContext.getContext().getUpdateGraph().exclusiveLock()
                .computeLocked(() -> t4.where("B in `b`"));
        assertTrue(t5.isRefreshing());
        final Table t6 = ExecutionContext.getContext().getUpdateGraph().exclusiveLock()
                .computeLocked(() -> t4.whereIn(t1, "B=A"));
        assertTrue(t6.isRefreshing());

        final Table t7 = ExecutionContext.getContext().getUpdateGraph().exclusiveLock()
                .computeLocked(() -> t1.whereIn(t4, "A=B"));
        assertTrue(t7.isRefreshing());
    }

    @Test
    public void testWhereInDiamond() {
        final ColumnInfo<?, ?>[] filteredInfo;

        final int size = 500;
        final Random random = new Random(0);

        final QueryTable table = getTable(size, random,
                filteredInfo = initColumnInfos(new String[] {"Sym", "intCol", "intCol2"},
                        new SetGenerator<>("aa", "bb", "bc", "cc", "dd", "ee", "ff", "gg", "hh", "ii"),
                        new IntGenerator(0, 100),
                        new IntGenerator(0, 100)));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final EvalNugget[] en = new EvalNugget[] {
                EvalNugget.from(() -> updateGraph.exclusiveLock().computeLocked(
                        () -> table.whereIn(table.where("intCol % 25 == 0"), "intCol2=intCol"))),
        };

        try {
            for (int step = 0; step < 1000; step++) {
                updateGraph.runWithinUnitTestCycle(() -> GenerateTableUpdates.generateShiftAwareTableUpdates(
                        GenerateTableUpdates.DEFAULT_PROFILE, size, random, table, filteredInfo));
                validate(en);
            }
        } catch (Exception e) {
            TestCase.fail(e.getClass().getName() + ": " + e.getMessage());
        }
    }

    @Test
    public void testWhereInDiamond2() {
        final QueryTable table = testRefreshingTable(i(1, 2, 3).toTracking(), col("x", 1, 2, 3), col("y", 2, 4, 6));
        final Table setTable = table.where("x % 2 == 0").dropColumns("y");
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final Table filteredTable = updateGraph.exclusiveLock().computeLocked(
                () -> table.whereIn(setTable, "y=x"));

        TableTools.show(filteredTable);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(table, i(4), col("x", 4), col("y", 8));
            table.notifyListeners(i(4), i(), i());
        });

        TableTools.show(setTable);
        TableTools.show(filteredTable);
    }

    private static class TestUncoalescedTable extends UncoalescedTable<TestUncoalescedTable> {
        private final Table delegate;
        private final List<Collection<? extends Selectable>> selectDistinctColumns = new ArrayList<>();

        public TestUncoalescedTable(final Table delegate) {
            super(delegate.getDefinition(), "TestUncoalescedTable");
            this.delegate = delegate;
        }

        @Override
        protected Table doCoalesce() {
            return delegate.coalesce();
        }

        @Override
        protected TestUncoalescedTable copy() {
            return this;
        }

        @Override
        public Table selectDistinct(final Collection<? extends Selectable> columns) {
            selectDistinctColumns.add(columns);
            return super.selectDistinct(columns);
        }
    }

    @Test
    public void testWhereInUncoalesced() {
        final Table table = TableTools.newTable(intCol("x", 1, 2, 3), intCol("y", 2, 4, 6));
        final Table setTable = TableTools.newTable(intCol("x", 3));

        final Table expected = table.whereIn(setTable, "x");
        final Table result = table.whereIn(new TestUncoalescedTable(setTable), "x");
        assertTableEquals(expected, result);
    }

    @Test
    public void testWhereInUncoalescedPartitioned() {
        final Table table = TableTools.newTable(intCol("x", 1, 2, 3), intCol("y", 2, 4, 6));
        final Table setTableRaw = TableTools.newTable(intCol("x", 3), intCol("y", 2));
        final TableDefinition definition =
                TableDefinition.of(setTableRaw.getDefinition().getColumn("x").withPartitioning(),
                        setTableRaw.getDefinition().getColumn("y"));
        final Table setTable = new QueryTable(definition, setTableRaw.getRowSet(), setTableRaw.getColumnSourceMap());

        final Table expected1 = table.whereIn(setTable, "x");
        final Table expected2 = table.whereIn(setTable, "y");

        final TestUncoalescedTable uncoalesced = new TestUncoalescedTable(setTable);
        final Table resultPart = table.whereIn(uncoalesced, "x");
        assertTableEquals(expected1, resultPart);
        assertEquals(1, uncoalesced.selectDistinctColumns.size());
        assertEquals(List.of(ColumnName.of("x")), uncoalesced.selectDistinctColumns.get(0));
        uncoalesced.selectDistinctColumns.clear();

        final Table resultNoPart = table.whereIn(uncoalesced, "y");
        assertTableEquals(expected2, resultNoPart);
        assertEquals(0, uncoalesced.selectDistinctColumns.size());
    }

    @Test
    public void testWhereNotInEmpty() {
        final Table x = newTable(intCol("X", 1, 2, 3));
        final Table emptyX = newTable(intCol("X"));
        final Table xWhereNotIn = x.whereNotIn(emptyX, "X");
        assertTableEquals(x, xWhereNotIn);

        final int[] emptyArray = new int[0];
        QueryScope.addParam("emptyArray", emptyArray);
        final Table expressionNot = x.where("X not in emptyArray");
        assertTableEquals(x, expressionNot);

        final Table xWhereIn = x.whereIn(emptyX, "X");
        assertTableEquals(emptyX, xWhereIn);

        final Table expressionIn = x.where("X in emptyArray");
        assertTableEquals(emptyX, expressionIn);
        QueryScope.addParam("emptyArray", null);
    }

    @Test
    public void testAddAndRemoveRefilter() {
        final QueryTable source = testRefreshingTable(i(10, 20, 30).toTracking(), stringCol("FV", "A", "B", "C"),
                intCol("Sentinel", 10, 20, 30));
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final QueryTable setTable = testRefreshingTable(i(10, 30).toTracking(), stringCol("FV", "A", "C"));

        final Table result = source.whereIn(setTable, "FV");
        final SimpleListener listener = new SimpleListener(result);
        result.addUpdateListener(listener);

        final PrintListener plResult = new PrintListener("testAddAndRemoveRefilter-result", result);
        assertTableEquals(source.where("FV in `A`, `C`"), result);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(setTable, i(20), stringCol("FV", "B"));
            TstUtils.removeRows(setTable, i(30));
            setTable.notifyListeners(i(20), i(30), i());
            TstUtils.addToTable(source, i(10), stringCol("FV", "A"), intCol("Sentinel", 40));
            source.notifyListeners(i(10), i(10), i());
        });

        assertEquals(i(10, 20), listener.update.added());
        assertEquals(i(10, 30), listener.update.removed());
        assertEquals(i(), listener.update.modified());

        assertTableEquals(source.where("FV in `A`, `B`"), result);
    }
}

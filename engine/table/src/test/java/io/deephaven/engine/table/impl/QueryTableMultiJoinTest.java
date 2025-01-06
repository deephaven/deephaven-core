//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import gnu.trove.map.hash.TIntIntHashMap;
import io.deephaven.chunk.util.pools.ChunkPoolReleaseTracking;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.*;
import io.deephaven.engine.testutil.*;
import io.deephaven.engine.testutil.generator.*;
import io.deephaven.engine.util.PrintListener;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.time.DateTimeUtils;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.type.ArrayTypeUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.util.*;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.testutil.junit4.EngineCleanup.printTableUpdates;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_INT;

@Category(OutOfBandTest.class)
public class QueryTableMultiJoinTest extends QueryTableTestBase {
    final static JoinControl DEFAULT_JOIN_CONTROL = new JoinControl();
    final static JoinControl REHASH_JOIN_CONTROL = new JoinControl() {
        @Override
        public int initialBuildSize() {
            return 1 << 4;
        }
    };

    final static JoinControl HIGH_LOAD_JOIN_CONTROL = new JoinControl() {
        @Override
        public int initialBuildSize() {
            return 1 << 4;
        }

        @Override
        public double getMaximumLoadFactor() {
            return 0.95;
        }

        @Override
        public double getTargetLoadFactor() {
            return 0.9;
        }
    };

    @Before
    public void before() throws Exception {
        ChunkPoolReleaseTracking.enableStrict();
    }

    @After
    public void after() throws Exception {
        ChunkPoolReleaseTracking.checkAndDisable();
    }

    @Test
    public void testSimple() {
        final Table t1 = TstUtils.testTable(col("Key", "A", "B", "C"), intCol("S1", 1, 2, 3));
        final Table t2 = TstUtils.testTable(col("Key", "C", "B", "D"), intCol("S2", 3, 4, 5));
        final Table t3 = TstUtils.testTable(col("Key", "F", "A", "B", "E"), intCol("S3", 6, 7, 8, 9),
                doubleCol("S3b", 9.9, 10.1, 11.2, 12.3));

        final Table result = MultiJoinFactory.of(new String[] {"Key"}, t1, t2, t3).table();
        TableTools.showWithRowSet(result);

        final Table expected = TableTools.newTable(col("Key", "A", "B", "C", "D", "F", "E"),
                intCol("S1", 1, 2, 3, NULL_INT, NULL_INT, NULL_INT),
                intCol("S2", NULL_INT, 4, 3, 5, NULL_INT, NULL_INT), intCol("S3", 7, 8, NULL_INT, NULL_INT, 6, 9),
                doubleCol("S3b", 10.1, 11.2, NULL_DOUBLE, NULL_DOUBLE, 9.9, 12.3));

        TstUtils.assertTableEquals(expected, result);
    }

    @Test
    public void testSimpleIncremental() {
        final QueryTable t1 = TstUtils.testRefreshingTable(col("Key", "A", "B", "C"), intCol("S1", 1, 2, 3));
        final QueryTable t2 = TstUtils.testRefreshingTable(col("Key", "C", "B", "D"), intCol("S2", 3, 4, 5));
        final Table t3 = TstUtils.testTable(col("Key", "F", "A", "B", "E"), intCol("S3", 6, 7, 8, 9),
                doubleCol("S3b", 9.9, 10.1, 11.2, 12.3));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final Table result =
                updateGraph.sharedLock()
                        .computeLocked(() -> MultiJoinFactory.of(new String[] {"Key"}, t1, t2, t3).table());
        System.out.println("Initial multi-join.");
        TableTools.showWithRowSet(result);
        final PrintListener printListener = new PrintListener("multiJoin", (QueryTable) result, 10);
        final TableUpdateValidator validator = TableUpdateValidator.make("multiJoin", (QueryTable) result);
        final FailureListener failureListener = new FailureListener();
        validator.getResultTable().addUpdateListener(failureListener);

        final Table expected = doIterativeMultiJoin(new String[] {"Key"}, Arrays.asList(t1, t2, t3));

        TstUtils.assertTableEquals(expected, result);

        System.out.println("Adding D, S1=4");
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet additions = RowSetFactory.fromKeys(5);
            TstUtils.addToTable(t1, additions, col("Key", "D"), intCol("S1", 4));
            t1.notifyListeners(additions, RowSetFactory.empty(), RowSetFactory.empty());
        });

        TstUtils.assertTableEquals(expected, result);

        System.out.println("Removing B, S2=4");
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet removals = RowSetFactory.fromKeys(1);
            TstUtils.removeRows(t2, removals);
            t2.notifyListeners(RowSetFactory.empty(), removals, RowSetFactory.empty());
        });
        TstUtils.assertTableEquals(expected, result);
        TableTools.showWithRowSet(result);

        System.out.println("Modifying C, S2=3 to G,10 and D, S2=5 to B, 11");
        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet modifications = RowSetFactory.fromKeys(0, 2);
            TstUtils.addToTable(t2, modifications, col("Key", "G", "B"), intCol("S2", 10, 11));
            t2.notifyListeners(RowSetFactory.empty(), RowSetFactory.empty(), modifications);
        });

        updateGraph.runWithinUnitTestCycle(() -> {
            TableTools.showWithRowSet(t1);
            TstUtils.addToTable(t1, i(3, 4), col("Key", "C", "D"), intCol("S1", 3, 4));
            TstUtils.removeRows(t1, i(2, 5));

            final TableUpdateImpl update = new TableUpdateImpl();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            update.added = RowSetFactory.empty();
            update.removed = RowSetFactory.empty();
            update.modified = RowSetFactory.empty();

            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
            shiftBuilder.shiftRange(2, 2, 1);
            shiftBuilder.shiftRange(5, 5, -1);

            update.shifted = shiftBuilder.build();

            TableTools.showWithRowSet(t1);

            t1.notifyListeners(update);
        });

        TstUtils.assertTableEquals(expected, result);

        updateGraph.runWithinUnitTestCycle(() -> {
            TstUtils.addToTable(t1, i(3), col("Key", "C"), intCol("S1", 7));

            final TableUpdateImpl update = new TableUpdateImpl();
            update.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            update.added = i(3);
            update.removed = i(3);
            update.modified = RowSetFactory.empty();

            update.shifted = RowSetShiftData.EMPTY;

            TableTools.showWithRowSet(t1);

            t1.notifyListeners(update);
        });

        TstUtils.assertTableEquals(expected, result);

        printListener.stop();
    }

    @Test
    public void testStatic() {
        for (int size = 10; size <= 100_000; size *= 10) {
            for (int seed = 0; seed < SEEDCOUNT.applyAsInt(size); ++seed) {
                System.out.println("Size = " + size + ", seed =" + seed);
                testStatic(DEFAULT_JOIN_CONTROL, size, seed, new String[] {"Key"}, new String[] {"Key2"});
                testStatic(DEFAULT_JOIN_CONTROL, size, seed, new String[] {"Key", "Key2"},
                        ArrayTypeUtils.EMPTY_STRING_ARRAY);
                testStatic(DEFAULT_JOIN_CONTROL, size, seed, new String[] {"Key", "Key2"},
                        ArrayTypeUtils.EMPTY_STRING_ARRAY);
            }
        }
    }

    @Test
    public void testStaticOverflowAndRehash() {
        for (int size = 1000; size <= 100_000; size *= 10) {
            testStatic(HIGH_LOAD_JOIN_CONTROL, size, 0, new String[] {"Key", "Key2"},
                    ArrayTypeUtils.EMPTY_STRING_ARRAY);
            testStatic(REHASH_JOIN_CONTROL, size, 0, new String[] {"Key", "Key2"},
                    ArrayTypeUtils.EMPTY_STRING_ARRAY);
        }
    }

    private void testStatic(JoinControl joinControl, int size, int seed, String[] keys, String[] drops) {
        final Random random = new Random(seed);
        final int tableCount = random.nextInt(10) + 1;

        final List<Table> inputTables = new ArrayList<>();

        for (int ii = 0; ii < tableCount; ++ii) {
            final Table table = TstUtils
                    .getTable(false, size, random,
                            TstUtils.initColumnInfos(new String[] {"Key", "Key2", "I" + ii, "D" + ii, "B" + ii},
                                    new IntGenerator(0, size), new BooleanGenerator(), new IntGenerator(),
                                    new DoubleGenerator(-1000, 1000), new BooleanGenerator(0.45, 0.1)))
                    .firstBy(keys).dropColumns(drops);
            inputTables.add(table);
        }

        if (printTableUpdates()) {
            System.out.println("Table Count: " + tableCount);
            for (int ii = 0; ii < tableCount; ++ii) {
                TableTools.showWithRowSet(inputTables.get(ii));
            }
        }

        final Table result = MultiJoinTableImpl.of(joinControl, MultiJoinInput.from(keys,
                inputTables.toArray(TableDefaults.ZERO_LENGTH_TABLE_ARRAY))).table();
        final Table expected = doIterativeMultiJoin(keys, inputTables);

        if (printTableUpdates()) {
            TableTools.showWithRowSet(result);
        }

        TstUtils.assertTableEquals(expected, result);
    }

    private static class SeedCount implements IntUnaryOperator {
        final static int seedCount = 10;
        final static TIntIntHashMap seedCountForSize = new TIntIntHashMap(1, 0.5f, -1, seedCount);

        static {
            seedCountForSize.put(100_000, 1);
            seedCountForSize.put(10_000, 1);
            seedCountForSize.put(1_000, 10);
        }

        @Override
        public int applyAsInt(int size) {
            return seedCountForSize.get(size);
        }
    }

    private static final SeedCount SEEDCOUNT = new SeedCount();

    @Test
    public void testIncremental() {
        final int seedInitial = 0;
        final int maxSteps = 20;

        for (int size = 10; size <= 10_000; size *= 10) {
            for (int seed = seedInitial; seed < seedInitial + SEEDCOUNT.applyAsInt(size); ++seed) {
                System.out.println("Size = " + size + ", seed = " + seed);
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testIncremental(DEFAULT_JOIN_CONTROL, size, seed, maxSteps, new String[] {"Key"},
                            new String[] {"Key2"});
                    testIncremental(DEFAULT_JOIN_CONTROL, size, seed, maxSteps, new String[] {"Key", "Key2"},
                            ArrayTypeUtils.EMPTY_STRING_ARRAY);
                }
            }
        }
    }

    @Test
    public void testIncrementalWithShifts() {
        final int seedInitial = 0;
        final int maxSteps = 10; // if we have more steps, we are more likely to run out of unique key space

        for (int size = 10; size <= 10_000; size *= 10) {
            for (int seed = seedInitial; seed < seedInitial + SEEDCOUNT.applyAsInt(size); ++seed) {
                System.out.println("Size = " + size + ", seed = " + seed);
                try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                    testIncrementalWithShifts(size, seed, maxSteps);
                }
            }
        }
    }

    @Test
    public void testIncrementalOverflowAndRehash() {
        final int seedInitial = 0;
        final int maxSteps = 20;

        for (int size = 1_000; size <= 100_000; size *= 10) {
            System.out.println("Size = " + size + ", seed = " + seedInitial);
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                testIncremental(HIGH_LOAD_JOIN_CONTROL, size, seedInitial, maxSteps, new String[] {"Key"},
                        new String[] {"Key2"});
                testIncremental(REHASH_JOIN_CONTROL, size, seedInitial, maxSteps, new String[] {"Key", "Key2"},
                        ArrayTypeUtils.EMPTY_STRING_ARRAY);
            }
        }
    }

    @Test
    public void testIncrementalZeroKey() {
        final int seedCount = 10;
        for (int seed = 0; seed < seedCount; ++seed) {
            System.out.println("Zero Key, seed = " + seed);
            try (final SafeCloseable ignored = LivenessScopeStack.open()) {
                testIncremental(DEFAULT_JOIN_CONTROL, 5, seed, 20, ArrayTypeUtils.EMPTY_STRING_ARRAY,
                        new String[] {"Key", "Key2"});
            }
        }
    }

    private void testIncremental(JoinControl joinControl, int size, int seed, int maxStep, String[] keys,
            String[] drops) {
        final Random random = new Random(seed);
        final int tableCount = random.nextInt(24) + 1;

        final List<QueryTable> inputTables = new ArrayList<>();
        final List<ColumnInfo<?, ?>[]> columnInfos = new ArrayList<>();
        final List<PrintListener> printListenersForRetention = new ArrayList<>();

        for (int ii = 0; ii < tableCount; ++ii) {
            final ColumnInfo<?, ?>[] columnInfo;
            final QueryTable table = TstUtils.getTable(true, random.nextInt(size), random,
                    columnInfo = TstUtils.initColumnInfos(new String[] {"Key", "Key2", "I" + ii, "D" + ii, "B" + ii},
                            new IntGenerator(0, size),
                            new SetGenerator<>(DateTimeUtils.parseInstant("2021-11-18T09:30:00 NY"),
                                    DateTimeUtils.parseInstant("2021-11-18T16:15:00 NY"),
                                    DateTimeUtils.parseInstant("2021-11-18T23:59:59.999 NY")),
                            new IntGenerator(), new DoubleGenerator(-1000, 1000), new BooleanGenerator(0.45, 0.1)));
            inputTables.add(table);
            columnInfos.add(columnInfo);
        }

        final MutableBoolean sortResult = new MutableBoolean(false);

        final List<Table> lastByInputs = new ArrayList<>();

        for (int tt = 0; tt < inputTables.size(); ++tt) {
            final Table in = inputTables.get(tt);
            final Table out;

            final double modType = random.nextDouble();
            if (keys.length == 0) {
                if (modType < 0.5) {
                    if (printTableUpdates()) {
                        System.out.println("Table " + tt + " tail(1)");
                    }
                    out = in.dropColumns(drops).tail(1);
                } else {
                    if (printTableUpdates()) {
                        System.out.println("Table " + tt + " lastBy()");
                    }
                    out = in.dropColumns(drops).lastBy(keys);
                }
            } else {
                final Table lastByKeys = in.dropColumns(drops).lastBy(keys);
                if (modType < 0.5 || lastByKeys.getDefinition().getColumnNames().size() < 2) {
                    out = lastByKeys;
                    if (printTableUpdates()) {
                        System.out.println("Table " + tt + " lastBy()");
                    }
                } else {
                    sortResult.setTrue();
                    out = lastByKeys.sort(lastByKeys.getDefinition().getColumnNames().get(1));
                    if (printTableUpdates()) {
                        System.out.println("Table " + tt + " lastBy().sort()");
                    }
                }
            }

            lastByInputs.add(out);
        }

        if (printTableUpdates()) {
            System.out.println("Table Count: " + tableCount);
            for (int ii = 0; ii < tableCount; ++ii) {
                System.out.println("Input " + ii + ":");
                TableTools.showWithRowSet(inputTables.get(ii));
            }
            for (int ii = 0; ii < tableCount; ++ii) {
                System.out.println("Last By " + ii + ":");
                TableTools.showWithRowSet(lastByInputs.get(ii));
                printListenersForRetention
                        .add(new PrintListener("Last By Result " + ii, (QueryTable) lastByInputs.get(ii), 10));
            }
        }

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final Table expected = doIterativeMultiJoin(keys, lastByInputs);
        final Table result = updateGraph.sharedLock()
                .computeLocked(() -> MultiJoinTableImpl
                        .of(joinControl,
                                MultiJoinInput.from(keys, lastByInputs.toArray(TableDefaults.ZERO_LENGTH_TABLE_ARRAY)))
                        .table());

        if (printTableUpdates()) {
            System.out.println("Initial result:");
            TableTools.showWithRowSet(result);
            printListenersForRetention.add(new PrintListener("Multi-join result", (QueryTable) result, 10));
        }

        final TableUpdateValidator validator = TableUpdateValidator.make("result validator", (QueryTable) result);
        final FailureListener listener = new FailureListener();
        validator.getResultTable().addUpdateListener(listener);

        TstUtils.assertTableEquals(expected, result);


        for (int step = 0; step < maxStep; ++step) {
            if (printTableUpdates()) {
                System.out.println("Step = " + step);
            }
            updateGraph.runWithinUnitTestCycle(() -> {
                if (random.nextBoolean()) {
                    for (int tt = 0; tt < tableCount; ++tt) {
                        if (random.nextBoolean()) {
                            GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                                    size, random, inputTables.get(tt), columnInfos.get(tt));
                        }
                    }
                } else {
                    final int tableToUpdate = random.nextInt(tableCount);
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, size,
                            random, inputTables.get(tableToUpdate), columnInfos.get(tableToUpdate));
                }
            });

            if (printTableUpdates()) {
                System.out.println("Multi-join result step=" + step + ": " + result.size());
                TableTools.showWithRowSet(result, 20);
            }

            if (sortResult.booleanValue()) {
                TstUtils.assertTableEquals(expected.sort(keys), result.sort(keys));
            } else {
                TstUtils.assertTableEquals(expected, result);
            }
        }
    }

    private void testIncrementalWithShifts(int size, int seed, int maxStep) {
        final Random random = new Random(seed);
        final int tableCount = random.nextInt(24) + 1;

        final List<QueryTable> inputTables = new ArrayList<>();
        final List<ColumnInfo<?, ?>[]> columnInfos = new ArrayList<>();

        for (int ii = 0; ii < tableCount; ++ii) {
            final ColumnInfo<?, ?>[] columnInfo;
            final QueryTable table = TstUtils.getTable(true, random.nextInt(size), random,
                    columnInfo = TstUtils.initColumnInfos(new String[] {"Key", "I" + ii},
                            new UniqueIntGenerator(0, size * 8), new IntGenerator()));
            inputTables.add(table);
            columnInfos.add(columnInfo);
        }

        if (printTableUpdates()) {
            System.out.println("Table Count: " + tableCount);
            for (int ii = 0; ii < tableCount; ++ii) {
                System.out.println("Input " + ii + ":");
                TableTools.showWithRowSet(inputTables.get(ii));
            }
        }

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final Table expected = doIterativeMultiJoin(new String[] {"Key"}, inputTables);
        final Table result = updateGraph.sharedLock().computeLocked(() -> MultiJoinFactory.of(new String[] {"Key"},
                inputTables.toArray(TableDefaults.ZERO_LENGTH_TABLE_ARRAY)).table());

        if (printTableUpdates()) {
            System.out.println("Initial result:");
            TableTools.showWithRowSet(result);
        }
        TstUtils.assertTableEquals(expected, result);

        final TableUpdateValidator validator = TableUpdateValidator.make((QueryTable) result);
        final FailureListener validatorListener = new FailureListener();
        validator.getResultTable().addUpdateListener(validatorListener);

        final Table expectSorted = expected.sort("Key");
        final Table resultSorted = result.sort("Key");

        for (int step = 0; step < maxStep; ++step) {
            if (printTableUpdates()) {
                System.out.println("Step = " + step);
            }
            updateGraph.runWithinUnitTestCycle(() -> {
                if (random.nextBoolean()) {
                    for (int tt = 0; tt < tableCount; ++tt) {
                        if (random.nextBoolean()) {
                            GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE,
                                    size, random, inputTables.get(tt), columnInfos.get(tt));
                        }
                    }
                } else {
                    final int tableToUpdate = random.nextInt(tableCount);
                    GenerateTableUpdates.generateShiftAwareTableUpdates(GenerateTableUpdates.DEFAULT_PROFILE, size,
                            random, inputTables.get(tableToUpdate), columnInfos.get(tableToUpdate));
                }
            });

            if (printTableUpdates()) {
                System.out.println("Multi-join result step=" + step + ": " + result.size());
                TableTools.showWithRowSet(result, 20);
            }

            TstUtils.assertTableEquals(expectSorted, resultSorted);
        }
    }

    @Test
    public void testZeroKeyStatic() {
        int seed = 0;
        final Random random = new Random(seed);
        final int tableCount = 5;

        final List<Table> inputTables = new ArrayList<>();

        for (int ii = 0; ii < tableCount; ++ii) {
            final Table table = TstUtils
                    .getTable(false, 10, random, TstUtils.initColumnInfos(new String[] {"I" + ii, "D" + ii, "B" + ii},
                            new IntGenerator(), new DoubleGenerator(-1000, 1000), new BooleanGenerator(0.45, 0.1)))
                    .firstBy();
            inputTables.add(table);
        }

        if (printTableUpdates()) {
            System.out.println("Table Count: " + tableCount);
            for (int ii = 0; ii < tableCount; ++ii) {
                TableTools.showWithRowSet(inputTables.get(ii));
            }
        }

        final Table result = MultiJoinFactory.of(ArrayTypeUtils.EMPTY_STRING_ARRAY,
                inputTables.toArray(TableDefaults.ZERO_LENGTH_TABLE_ARRAY)).table();
        final Table expected = doIterativeMultiJoin(ArrayTypeUtils.EMPTY_STRING_ARRAY, inputTables);

        if (printTableUpdates()) {
            TableTools.showWithRowSet(result);
        }

        TstUtils.assertTableEquals(expected, result);
    }

    @Test
    public void testDuplicateKeys() {
        final Table t1 = TableTools.newTable(intCol("C1", 1, 2), intCol("C2", 1, 1), intCol("S1", 10, 11));
        final Table t2 = TableTools.newTable(intCol("C3", 2, 2), intCol("C4", 1, 2));

        final Table joined = MultiJoinTableImpl.of(MultiJoinInput.of(t1, "Key=C1", "S1")).table();
        assertTableEquals(TableTools.newTable(intCol("Key", 1, 2), intCol("S1", 10, 11)), joined);

        try {
            MultiJoinTableImpl.of(MultiJoinInput.of(t1, "Key=C1", "S1"),
                    MultiJoinInput.of(t2, "Key=C3", "C4")).table();
            Assert.fail("expected exception");
        } catch (IllegalStateException e) {
            Assert.assertEquals("Duplicate key found for 2 in table 1.", e.getMessage());
        }

        try {
            MultiJoinTableImpl.of(MultiJoinInput.of(t1, "", "S1")).table();
            Assert.fail("expected exception");
        } catch (IllegalStateException e) {
            Assert.assertEquals("Duplicate rows for table 0 on zero-key multiJoin.", e.getMessage());
        }

        t1.setRefreshing(true);

        final QueryTable t2r = TstUtils.testRefreshingTable(intCol("C3", 2, 2), intCol("C4", 1, 2), intCol("S2", 20,
                21));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final Table joinedR = updateGraph.sharedLock().computeLocked(() -> MultiJoinTableImpl.of(
                MultiJoinInput.of(t1, "Key=C1", "S1"),
                MultiJoinInput.of(t2r, "Key=C4", "S2")).table());
        assertTableEquals(TableTools.newTable(intCol("Key", 1, 2), intCol("S1", 10, 11), intCol("S2", 20, 21)),
                joinedR);

        try {
            updateGraph.sharedLock().doLocked(() -> MultiJoinTableImpl.of(
                    MultiJoinInput.of(t1, "Key=C1", "S1"),
                    MultiJoinInput.of(t2r, "Key=C3", "C4")).table());
            Assert.fail("expected exception");
        } catch (IllegalStateException e) {
            Assert.assertEquals("Duplicate key found for 2 in table 1.", e.getMessage());
        }

        allowingError(() -> {
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(t2r, i(2), intCol("C3", 1), intCol("C4", 2), intCol("S2", 22));
                t2r.notifyListeners(i(2), i(), i());
            });
        }, (lot) -> {
            if (lot == null || lot.size() != 1) {
                return false;
            }
            final Throwable throwable = lot.get(0);
            return throwable instanceof IllegalStateException
                    && throwable.getMessage().equals("Duplicate key found for 2 in table 1.");
        });
    }

    @Test
    public void testDuplicateZeroKey() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final QueryTable t3 = TstUtils.testRefreshingTable(intCol("S1"));
        final Table j3 = updateGraph.sharedLock()
                .computeLocked(() -> MultiJoinTableImpl.of(MultiJoinInput.of(t3, null, "S1")))
                .table();
        assertTableEquals(TableTools.newTable(intCol("S1")), j3);
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(t3, i(0), intCol("S1", 1));
            t3.notifyListeners(i(0), i(), i());
        });
        assertTableEquals(TableTools.newTable(intCol("S1", 1)), j3);

        allowingError(() -> {
            updateGraph.runWithinUnitTestCycle(() -> {
                addToTable(t3, i(2), intCol("S1", 2));
                t3.notifyListeners(i(2), i(), i());
            });
        }, (lot) -> {
            if (lot == null || lot.size() != 1) {
                return false;
            }
            final Throwable throwable = lot.get(0);
            return throwable instanceof IllegalStateException
                    && throwable.getMessage().equals("Multiple rows in 0 for zero-key multiJoin.");
        });
    }

    @Test
    public void testZeroKeyTransitions() {
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final QueryTable t3 = TstUtils.testRefreshingTable(intCol("S1"));
        final Table j3 = updateGraph.sharedLock().computeLocked(() -> MultiJoinTableImpl
                .of(MultiJoinInput.of(t3, "", "S1")).table());
        final TableUpdateValidator validator = TableUpdateValidator.make("testZeroKeyTransitions", (QueryTable) j3);
        final FailureListener failureListener = new FailureListener();
        validator.getResultTable().addUpdateListener(failureListener);

        assertTableEquals(TableTools.newTable(intCol("S1")), j3);
        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(t3, i(0), intCol("S1", 1));
            t3.notifyListeners(i(0), i(), i());
        });
        assertTableEquals(TableTools.newTable(intCol("S1", 1)), j3);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(t3, i(1), intCol("S1", 1));
            removeRows(t3, i(0));
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(0, 0, 1);
            final RowSetShiftData shift = builder.build();
            final ModifiedColumnSet modifiedColumnSet = t3.newModifiedColumnSet("S1");
            t3.notifyListeners(new TableUpdateImpl(i(), i(), i(), shift, modifiedColumnSet));
        });
        assertTableEquals(TableTools.newTable(intCol("S1", 1)), j3);

        updateGraph.runWithinUnitTestCycle(() -> {
            addToTable(t3, i(2), intCol("S1", 2));
            removeRows(t3, i(1));
            final RowSetShiftData.Builder builder = new RowSetShiftData.Builder();
            builder.shiftRange(1, 1, 1);
            final RowSetShiftData shift = builder.build();
            final ModifiedColumnSet modifiedColumnSet = t3.newModifiedColumnSet("S1");
            t3.notifyListeners(new TableUpdateImpl(i(), i(), i(2), shift, modifiedColumnSet));
        });
        assertTableEquals(TableTools.newTable(intCol("S1", 2)), j3);

        updateGraph.runWithinUnitTestCycle(() -> {
            removeRows(t3, i(2));
            t3.notifyListeners(i(), i(2), i());
        });
        assertTableEquals(TableTools.newTable(intCol("S1")), j3);
    }

    @Test
    public void testColumnConflicts() {
        final Table t1 = TableTools.newTable(col("A", "a", "b"), intCol("B", 1, 2));
        final Table t2 = TableTools.newTable(col("A", "a", "b"), intCol("D", 3, 4), doubleCol("C", 3.0, 4.0));

        try {
            MultiJoinFactory.of();
            Assert.fail("expected exception");
        } catch (IllegalArgumentException iae) {
            Assert.assertEquals(iae.getMessage(), "At least one table must be included in MultiJoinTable.");
        }

        try {
            MultiJoinTableImpl.of(
                    MultiJoinInput.of(t1, "Key=B", "A"),
                    MultiJoinInput.of(t2, "KeyNotTheSame=C", "A"));
            Assert.fail("expected exception");
        } catch (IllegalArgumentException iae) {
            Assert.assertEquals(iae.getMessage(),
                    "Key column mismatch for table 1, first table has key columns=[Key], this table has [KeyNotTheSame]");
        }

        try {
            MultiJoinTableImpl.of(
                    MultiJoinInput.of(t1, "Key=B", "A"),
                    MultiJoinInput.of(t2, "Key=D", "A"));
            Assert.fail("expected exception");
        } catch (IllegalArgumentException iae) {
            Assert.assertEquals("Column A defined in table 0 and table 1", iae.getMessage());
        }

        try {
            MultiJoinTableImpl.of(
                    MultiJoinInput.of(t1, "Key=B", "A"),
                    MultiJoinInput.of(t2, "Key=D", "Key"));
            Assert.fail("expected exception");
        } catch (IllegalArgumentException iae) {
            Assert.assertEquals("Column Key defined in table key columns and table 1", iae.getMessage());
        }

        try {
            MultiJoinTableImpl.of(
                    MultiJoinInput.of(t1, "Key=B", "A"),
                    MultiJoinInput.of(t2, "Key=C", "D"));
            Assert.fail("expected exception");
        } catch (IllegalArgumentException iae) {
            Assert.assertEquals(
                    "Key column type mismatch for table 1, first table has key column types=[int], this table has [double]",
                    iae.getMessage());
        }

        try {
            MultiJoinTableImpl.of(
                    MultiJoinInput.of(t1, null, "A"),
                    MultiJoinInput.of(t2, "Key=C", "D"));
            Assert.fail("expected exception");
        } catch (IllegalArgumentException iae) {
            Assert.assertEquals(
                    "Key column mismatch for table 1, first table has key columns=[], this table has [Key]",
                    iae.getMessage());
        }

        try {
            MultiJoinTableImpl.of(
                    MultiJoinInput.of(t1, null, "A"),
                    MultiJoinInput.of(t2, null, "A"));
            Assert.fail("expected exception");
        } catch (IllegalArgumentException iae) {
            Assert.assertEquals("Column A defined in table 0 and table 1", iae.getMessage());
        }
    }

    @Test
    public void testMultiJoinInputColumnParsing() {
        final Table dummyTable = emptyTable(0);

        MultiJoinInput mji = MultiJoinInput.of(dummyTable, "Key1=A,Key2=B", "C1=C,D1=D");
        Assert.assertEquals(mji.inputTable(), dummyTable);
        Assert.assertEquals(mji.columnsToMatch()[0].left().name(), "Key1");
        Assert.assertEquals(mji.columnsToMatch()[0].right().name(), "A");
        Assert.assertEquals(mji.columnsToMatch()[1].left().name(), "Key2");
        Assert.assertEquals(mji.columnsToMatch()[1].right().name(), "B");
        Assert.assertEquals(mji.columnsToAdd()[0].newColumn().name(), "C1");
        Assert.assertEquals(mji.columnsToAdd()[0].existingColumn().name(), "C");
        Assert.assertEquals(mji.columnsToAdd()[1].newColumn().name(), "D1");
        Assert.assertEquals(mji.columnsToAdd()[1].existingColumn().name(), "D");

        // Assert whitespace and '==' is handled properly.
        mji = MultiJoinInput.of(dummyTable, "\tKey1 = A,     \tKey2  ==B ", " \tC1 =C,  D1=D");
        Assert.assertEquals(mji.inputTable(), dummyTable);
        Assert.assertEquals(mji.columnsToMatch()[0].left().name(), "Key1");
        Assert.assertEquals(mji.columnsToMatch()[0].right().name(), "A");
        Assert.assertEquals(mji.columnsToMatch()[1].left().name(), "Key2");
        Assert.assertEquals(mji.columnsToMatch()[1].right().name(), "B");
        Assert.assertEquals(mji.columnsToAdd()[0].newColumn().name(), "C1");
        Assert.assertEquals(mji.columnsToAdd()[0].existingColumn().name(), "C");
        Assert.assertEquals(mji.columnsToAdd()[1].newColumn().name(), "D1");
        Assert.assertEquals(mji.columnsToAdd()[1].existingColumn().name(), "D");

        // Assert that the factory methods are equivalent.
        final Table dummyTable2 = emptyTable(0);

        MultiJoinInput[] mjiArr = MultiJoinInput.from("Key1=A,Key2=B", dummyTable, dummyTable2);
        Assert.assertEquals(mjiArr[0].inputTable(), dummyTable);
        Assert.assertEquals(mjiArr[0].columnsToMatch()[0].left().name(), "Key1");
        Assert.assertEquals(mjiArr[0].columnsToMatch()[0].right().name(), "A");
        Assert.assertEquals(mjiArr[0].columnsToMatch()[1].left().name(), "Key2");
        Assert.assertEquals(mjiArr[0].columnsToMatch()[1].right().name(), "B");
        Assert.assertEquals(mjiArr[1].inputTable(), dummyTable2);
        Assert.assertEquals(mjiArr[1].columnsToMatch()[0].left().name(), "Key1");
        Assert.assertEquals(mjiArr[1].columnsToMatch()[0].right().name(), "A");
        Assert.assertEquals(mjiArr[1].columnsToMatch()[1].left().name(), "Key2");
        Assert.assertEquals(mjiArr[1].columnsToMatch()[1].right().name(), "B");

        mjiArr = MultiJoinInput.from(
                new String[] {"Key1=A", "Key2=B"},
                new Table[] {dummyTable, dummyTable2});
        Assert.assertEquals(mjiArr[0].inputTable(), dummyTable);
        Assert.assertEquals(mjiArr[0].columnsToMatch()[0].left().name(), "Key1");
        Assert.assertEquals(mjiArr[0].columnsToMatch()[0].right().name(), "A");
        Assert.assertEquals(mjiArr[0].columnsToMatch()[1].left().name(), "Key2");
        Assert.assertEquals(mjiArr[0].columnsToMatch()[1].right().name(), "B");
        Assert.assertEquals(mjiArr[1].inputTable(), dummyTable2);
        Assert.assertEquals(mjiArr[1].columnsToMatch()[0].left().name(), "Key1");
        Assert.assertEquals(mjiArr[1].columnsToMatch()[0].right().name(), "A");
        Assert.assertEquals(mjiArr[1].columnsToMatch()[1].left().name(), "Key2");
        Assert.assertEquals(mjiArr[1].columnsToMatch()[1].right().name(), "B");
    }

    @Test
    public void testRehashWhenEmpty() {
        final QueryTable t1 = TstUtils.testRefreshingTable(stringCol("Key"), intCol("S1"));
        final QueryTable t2 = TstUtils.testRefreshingTable(stringCol("Key"), intCol("S2"));

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();

        final Table result = updateGraph.sharedLock().computeLocked(
                () -> MultiJoinFactory.of(new String[] {"Key"}, t1, t2).table());

        updateGraph.runWithinUnitTestCycle(() -> {
            final RowSet additions = RowSetFactory.fromRange(0, 3073);
            TstUtils.addToTable(t1, additions,
                    stringCol("Key", IntStream.rangeClosed(0, 3073).mapToObj(Integer::toString).toArray(String[]::new)),
                    intCol("S1", IntStream.rangeClosed(0, 3073).map(i -> i * 2).toArray()));
            t1.notifyListeners(additions, RowSetFactory.empty(), RowSetFactory.empty());
        });
    }

    private Table doIterativeMultiJoin(String[] keyColumns, List<? extends Table> inputTables) {
        final List<Table> keyTables = inputTables.stream()
                .map(t -> keyColumns.length == 0 ? t.dropColumns(t.getDefinition().getColumnNames()).view("Dummy=1")
                        : t.view(keyColumns))
                .collect(Collectors.toList());
        final Table base = TableTools.merge(keyTables).selectDistinct(keyColumns);
        final String columnNames = String.join(",", keyColumns);

        Table result = base;
        for (Table inputTable : inputTables) {
            result = result.naturalJoin(inputTable, columnNames);
        }

        return result;
    }
}

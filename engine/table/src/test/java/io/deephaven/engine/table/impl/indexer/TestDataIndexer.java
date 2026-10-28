//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.indexer;

import com.google.common.collect.Sets;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.DataIndex;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.remote.ConstructSnapshot;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.testutil.EvalNugget;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import static io.deephaven.engine.testutil.GenerateTableUpdates.generateTableUpdates;
import static io.deephaven.engine.testutil.TstUtils.i;
import static io.deephaven.engine.util.TableTools.intCol;
import static org.junit.Assert.*;

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

    /**
     * Looking up a refreshing data index whose table has not been created does not create it.
     */
    @Test
    public void testLookupDoesNotCreateIndexTable() {
        final QueryTable source = TstUtils.testRefreshingTable(i(0, 1, 2, 3).toTracking(), intCol("Key", 0, 1, 0, 1));
        final DataIndex created = DataIndexer.getOrCreateDataIndex(source, "Key");
        assertFalse(created.tableIsCached());

        assertTrue(DataIndexer.hasDataIndex(source, "Key"));
        assertSame(created, DataIndexer.getDataIndex(source, "Key"));
        assertSame(created, DataIndexer.getOrCreateDataIndex(source, "Key"));
        assertFalse(created.tableIsCached());

        assertEquals(2, created.table().size());
    }

    /**
     * One thread computes a refreshing data index's table during an update cycle without the update graph lock. Its key
     * column cannot be read in previous space, so the computation needs the shared lock. Another thread, already
     * waiting for the exclusive lock when that happens, holds it once the cycle completes, then looks up the index and
     * uses its table. Both complete with the same table.
     */
    @Test
    public void testIndexTableComputedWhileExclusiveLockHolderUsesIndex() throws Exception {
        final QueryTable source = new QueryTable(i(0, 1, 2, 3).toTracking(),
                Collections.singletonMap("Key", new PrevRejectingIntSource()));
        source.setRefreshing(true);
        final DataIndex dataIndex = DataIndexer.getOrCreateDataIndex(source, "Key");
        assertFalse(dataIndex.tableIsCached());

        final ExecutionContext executionContext = ExecutionContext.getContext();
        final ControlledUpdateGraph updateGraph = executionContext.getUpdateGraph().cast();
        final CompletableFuture<Table> exclusiveResult = new CompletableFuture<>();
        final Thread exclusiveThread = new Thread(() -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                exclusiveResult.complete(updateGraph.exclusiveLock().computeLocked(
                        () -> DataIndexer.getDataIndex(source, "Key").table()));
            } catch (final Throwable failure) {
                exclusiveResult.completeExceptionally(failure);
            }
        }, "exclusive-index-user");
        final CompletableFuture<Table> lockFreeResult = new CompletableFuture<>();
        final Thread lockFreeThread = new Thread(() -> {
            try (final SafeCloseable ignored = executionContext.open()) {
                lockFreeResult.complete(dataIndex.table());
            } catch (final Throwable failure) {
                lockFreeResult.completeExceptionally(failure);
            }
        }, "lock-free-index-builder");
        exclusiveThread.setDaemon(true);
        lockFreeThread.setDaemon(true);

        updateGraph.startCycleForUnitTests(false);
        try {
            exclusiveThread.start();
            awaitWaitingForUpdateGraphLock(exclusiveThread, exclusiveResult);
            lockFreeThread.start();
            awaitWaitingForUpdateGraphLock(lockFreeThread, lockFreeResult);
        } finally {
            updateGraph.markSourcesRefreshedForUnitTests();
            updateGraph.completeCycleForUnitTests();
        }

        final Table exclusiveTable = exclusiveResult.get(30, TimeUnit.SECONDS);
        final Table lockFreeTable = lockFreeResult.get(30, TimeUnit.SECONDS);
        assertSame(exclusiveTable, lockFreeTable);
        assertEquals(2, exclusiveTable.size());
    }

    /**
     * An int column, {@code rowKey % 2}, that rejects snapshots reading its previous values.
     */
    private static class PrevRejectingIntSource extends AbstractColumnSource<Integer>
            implements MutableColumnSourceGetDefaults.ForInt {

        private PrevRejectingIntSource() {
            super(int.class);
        }

        @Override
        public int getInt(final long rowKey) {
            return (int) (rowKey % 2);
        }

        @Override
        public int getPrevInt(final long rowKey) {
            throw new ConstructSnapshot.NoSnapshotAllowedException();
        }
    }

    /**
     * Wait until {@code thread} is parked waiting for the update graph lock. If {@code thread} completes first, its
     * result or failure is reported instead.
     */
    private static void awaitWaitingForUpdateGraphLock(
            final Thread thread,
            final CompletableFuture<Table> result) throws Exception {
        final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
        final long deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (true) {
            if (result.isDone()) {
                throw new AssertionError(thread.getName() + " completed without waiting for the update graph lock: "
                        + result.get());
            }
            final ThreadInfo threadInfo = threadMXBean.getThreadInfo(thread.getId());
            if (threadInfo != null && threadInfo.getLockInfo() != null
                    && threadInfo.getLockInfo().getClassName().startsWith(ReentrantReadWriteLock.class.getName())) {
                return;
            }
            if (System.nanoTime() > deadlineNanos) {
                throw new AssertionError(thread.getName() + " is not waiting for the update graph lock: " + threadInfo);
            }
            Thread.sleep(10);
        }
    }

    @Test
    public void testIndex() {
        testIndex(false, new Random(0), new MutableInt(50));
    }

    @Test
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

        final int maxSteps = numSteps.get(); // 8;

        if (RefreshingTableTestCase.printTableUpdates) {
            System.out.println("Initial QueryTable: ");
            TableTools.showWithRowSet(queryTable);
        }

        for (numSteps.set(0); numSteps.get() < maxSteps; numSteps.increment()) {
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

    @Test
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

        assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));

        IndexValidator.validateIndex(countingTable, new String[] {"Sym"}, false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        IndexValidator.validateIndex(countingTable, new String[] {"intCol"}, false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        IndexValidator.validateIndex(countingTable, new String[] {"intCol", "Sym"}, false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) sym2ColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        IndexValidator.validateIndex(countingTable, new String[] {"intCol", "Sym", "Sym2"}, false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("getDouble"));
        IndexValidator.validateIndex(countingTable, new String[] {"intCol", "Sym", "doubleCol"}, false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());

        assertEquals(0, ((CountingTable.MethodCounter) symColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) sym2ColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) intColumnSource).getMethodCount("getInt"));
        assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("get"));
        assertEquals(0, ((CountingTable.MethodCounter) doubleColumnSource).getMethodCount("getDouble"));
        IndexValidator.validateIndex(countingTable, new String[] {"intCol", "Sym", "Sym2", "doubleCol"}, false);
        countingTable.getColumnSources().forEach(x -> ((CountingTable.MethodCounter) x).clear());
    }
}

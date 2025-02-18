//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.csv.CsvTools;
import io.deephaven.csv.util.CsvReaderException;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.HierarchicalTable.SnapshotState;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import io.deephaven.engine.table.impl.sources.ByteAsBooleanColumnSource;
import io.deephaven.engine.table.impl.sources.LongAsInstantColumnSource;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.engine.testutil.ColumnInfo;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.EvalNuggetInterface;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.generator.IntGenerator;
import io.deephaven.engine.testutil.generator.SetGenerator;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.util.TableTools;
import io.deephaven.test.types.OutOfBandTest;

import junit.framework.TestCase;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.ByteArrayInputStream;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.deephaven.api.agg.Aggregation.*;
import static io.deephaven.engine.table.impl.sources.ReinterpretUtils.byteToBooleanSource;
import static io.deephaven.engine.table.impl.sources.ReinterpretUtils.longToInstantSource;
import static io.deephaven.engine.table.impl.sources.ReinterpretUtils.maybeConvertToPrimitiveChunkType;
import static io.deephaven.engine.testutil.TstUtils.*;
import static io.deephaven.engine.testutil.testcase.RefreshingTableTestCase.simulateShiftAwareStep;
import static io.deephaven.engine.util.TableTools.*;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests for {@link HierarchicalTable#snapshot(SnapshotState, Table, ColumnName, BitSet, RowSequence, WritableChunk[])
 * hierarchical table snapshots}.
 */
@Category(OutOfBandTest.class)
public class TestHierarchicalTableSnapshots {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void testRollupSnapshotSatisfaction() throws ExecutionException, InterruptedException, TimeoutException {
        // noinspection resource
        final QueryTable source = testRefreshingTable(
                RowSetFactory.fromKeys(10).toTracking(),
                intCol("A", 1),
                intCol("B", 2),
                intCol("C", 3));
        final RollupTable rollupTable = source.rollup(List.of(AggMax("MaxC=C")), "A", "B");

        final SnapshotState snapshotState = rollupTable.makeSnapshotState();

        final Table expandAllKeys = newTable(
                intCol(rollupTable.getRowDepthColumn().name(), 0),
                intCol("A", NULL_INT),
                intCol("B", NULL_INT),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));

        final Table initialSnapshot = snapshotToTable(rollupTable, snapshotState,
                expandAllKeys, ColumnName.of("Action"), null, RowSetFactory.flat(4) /* Ask for 1 extra. */);
        final Table initialExpected = newTable(
                intCol(rollupTable.getRowDepthColumn().name(), 1, 2, 3),
                booleanCol(rollupTable.getRowExpandedColumn().name(), true, true, null),
                intCol("A", NULL_INT, 1, 1),
                intCol("B", NULL_INT, NULL_INT, 2),
                intCol("MaxC", 3, 3, 3));
        assertTableEquals(initialExpected, initialSnapshot);
        freeSnapshotTableChunks(initialSnapshot);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutorService concurrentExecutor = Executors.newSingleThreadScheduledExecutor();

        updateGraph.startCycleForUnitTests();
        addToTable(source, RowSetFactory.fromKeys(20), intCol("A", 1), intCol("B", 2), intCol("C", 4));
        source.notifyListeners(new TableUpdateImpl(RowSetFactory.fromKeys(20),
                RowSetFactory.empty(), RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        final Future<Table> snapshotFuture = concurrentExecutor.submit(() -> snapshotToTable(
                rollupTable, snapshotState, expandAllKeys, ColumnName.of("Action"), null, RowSetFactory.flat(4)));
        // We need to deliver 3 notifications to ensure that the rollup's 3 aggregation layers are satisfied.
        // The future cannot complete before that happens.
        for (int ni = 0; ni < 3; ++ni) {
            assertThat(snapshotFuture.isDone()).isFalse();
            updateGraph.flushOneNotificationForUnitTests();
        }
        /* @formatter off:
         * The snapshot thread will be racing to see that the rollup root is satisfied.
         * There are 2 scenarios:
         * 1. Snapshot sees that the input is satisfied or waits for it successfully before the cycle is complete, thus
         *    allowing it to complete concurrently.
         * 2. Snapshot fails to wait for structural satisfaction before the cycle finishes, and has to re-try, which
         *    is guaranteed to succeed against the idle cycle.
         * We cannot control which of these outcomes happens without instrumenting the snapshot to let us gate progress
         * through its structural satisfaction checks.
         * @formatter:on */
        updateGraph.completeCycleForUnitTests();

        final Table updatedSnapshot = snapshotFuture.get(30, TimeUnit.SECONDS);
        final Table updatedExpected = newTable(
                intCol(rollupTable.getRowDepthColumn().name(), 1, 2, 3),
                booleanCol(rollupTable.getRowExpandedColumn().name(), true, true, null),
                intCol("A", NULL_INT, 1, 1),
                intCol("B", NULL_INT, NULL_INT, 2),
                intCol("MaxC", 4, 4, 4));
        assertTableEquals(updatedExpected, updatedSnapshot);
        freeSnapshotTableChunks(updatedSnapshot);

        concurrentExecutor.shutdown();
    }

    @Test
    public void testTreeSnapshotSatisfaction() throws ExecutionException, InterruptedException, TimeoutException {
        // noinspection resource
        final QueryTable source = testRefreshingTable(
                RowSetFactory.fromKeys(10, 11, 12).toTracking(),
                intCol("CID", 0, 1, 2),
                intCol("PID", NULL_INT, 0, 1),
                intCol("Other", 50, 60, 70));
        final TreeTable treeTable = source.tree("CID", "PID");

        final SnapshotState snapshotState = treeTable.makeSnapshotState();

        final Table expandAllKeys = newTable(
                intCol(treeTable.getRowDepthColumn().name(), 0),
                intCol("CID", NULL_INT),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));

        final Table initialSnapshot = snapshotToTable(treeTable, snapshotState,
                expandAllKeys, ColumnName.of("Action"), null, RowSetFactory.flat(4));
        final Table initialExpected = newTable(
                intCol(treeTable.getRowDepthColumn().name(), 1, 2, 3),
                booleanCol(treeTable.getRowExpandedColumn().name(), true, true, null),
                intCol("CID", 0, 1, 2),
                intCol("PID", NULL_INT, 0, 1),
                intCol("Other", 50, 60, 70));
        assertTableEquals(initialExpected, initialSnapshot);
        freeSnapshotTableChunks(initialSnapshot);

        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        final ExecutorService concurrentExecutor = Executors.newSingleThreadScheduledExecutor();

        updateGraph.startCycleForUnitTests();
        addToTable(source, RowSetFactory.fromKeys(20), intCol("CID", 3), intCol("PID", 2), intCol("Other", 800));
        source.notifyListeners(new TableUpdateImpl(RowSetFactory.fromKeys(20),
                RowSetFactory.empty(), RowSetFactory.empty(), RowSetShiftData.EMPTY, ModifiedColumnSet.EMPTY));
        final Future<Table> snapshotFuture = concurrentExecutor.submit(() -> snapshotToTable(
                treeTable, snapshotState, expandAllKeys, ColumnName.of("Action"), null, RowSetFactory.flat(5)));
        // We need to deliver 2 notifications to ensure that the tree's partition and lookup aggregations are satisfied.
        // The future cannot complete before that happens.
        for (int ni = 0; ni < 2; ++ni) {
            assertThat(snapshotFuture.isDone()).isFalse();
            updateGraph.flushOneNotificationForUnitTests();
        }
        /* @formatter off:
         * The snapshot thread will be racing to see that the tree and sourceRowLookup are satisfied.
         * There are 2 scenarios:
         * 1. Snapshot sees that any combination of the two inputs are satisfied, and waits for any unsatisfied input(s)
         *    successfully before the cycle is complete, thus allowing it to complete concurrently.
         * 2. Snapshot fails to wait for structural satisfaction before the cycle finishes, and has to re-try, which
         *    is guaranteed to succeed against the idle cycle.
         * We cannot control which of these outcomes happens without instrumenting the snapshot to let us gate progress
         * through its structural satisfaction checks.
         * @formatter:on */
        updateGraph.completeCycleForUnitTests();

        final Table updatedSnapshot = snapshotFuture.get(30, TimeUnit.SECONDS);
        final Table updatedExpected = newTable(
                intCol(treeTable.getRowDepthColumn().name(), 1, 2, 3, 4),
                booleanCol(treeTable.getRowExpandedColumn().name(), true, true, true, null),
                intCol("CID", 0, 1, 2, 3),
                intCol("PID", NULL_INT, 0, 1, 2),
                intCol("Other", 50, 60, 70, 800));
        assertTableEquals(updatedExpected, updatedSnapshot);
        freeSnapshotTableChunks(updatedSnapshot);

        concurrentExecutor.shutdown();
    }

    @Test
    public void testSortedExpandAll() throws CsvReaderException {
        final String data = "A,B,C,N\n" +
                "Apple,One,Alpha,1\n" +
                "Apple,One,Alpha,2\n" +
                "Apple,One,Bravo,3\n" +
                "Apple,One,Bravo,4\n" +
                "Apple,One,Bravo,5\n" +
                "Apple,One,Bravo,6\n" +
                "Banana,Two,Alpha,7\n" +
                "Banana,Two,Alpha,8\n" +
                "Banana,Two,Bravo,3\n" +
                "Banana,Two,Bravo,4\n" +
                "Banana,Three,Bravo,1\n" +
                "Banana,Three,Bravo,1\n";

        final Table source = CsvTools.readCsv(new ByteArrayInputStream(data.getBytes()));

        TableTools.show(source);
        final RollupTable rollupTable = source.rollup(List.of(Aggregation.of(AggSpec.sum(), "N")), "A", "B", "C");
        final RollupTable sortedRollup = rollupTable.withNodeOperations(
                rollupTable.makeNodeOperationsRecorder(RollupTable.NodeType.Aggregated).sortDescending("N"));

        final String[] arrayWithNull = new String[1];
        final Table keyTable = newTable(
                intCol(rollupTable.getRowDepthColumn().name(), 0),
                stringCol("A", arrayWithNull),
                stringCol("B", arrayWithNull),
                stringCol("C", arrayWithNull),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));

        final SnapshotState ss = rollupTable.makeSnapshotState();
        final Table snapshot =
                snapshotToTable(rollupTable, ss, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        TableTools.showWithRowSet(snapshot);

        final SnapshotState ssSort = sortedRollup.makeSnapshotState();

        final Table snapshotSort =
                snapshotToTable(sortedRollup, ssSort, keyTable, ColumnName.of("Action"), null, RowSetFactory.flat(30));
        TableTools.showWithRowSet(snapshotSort);

        // first we know that the size of the tables must be the same
        TestCase.assertEquals(snapshot.size(), snapshotSort.size());
        // and the first row must be the same, because it is the parent
        assertTableEquals(snapshot.head(1), snapshotSort.head(1));
        // then we have six rows of banana, and that should be identical
        assertTableEquals(snapshot.slice(5, 11), snapshotSort.slice(1, 7));
        // then we need to check on the apple rows, but those are not actually identical because of sorting
        Table appleExpected = snapshot.where("A=`Apple`").sortDescending("N");
        assertTableEquals(appleExpected, snapshotSort.slice(7, 11));

        freeSnapshotTableChunks(snapshot);
        freeSnapshotTableChunks(snapshotSort);
    }

    // region Test Rollup Tables

    // This is the list of supported aggregations for rollup. These are all using `intCol` as the column to aggregate
    // because the re-aggregation logic is effectively the same for all column types.
    private final Collection<Aggregation> aggs = List.of(
            AggAbsSum("absSum=intCol"),
            AggAvg("avg=intCol"),
            AggCount("count"),
            AggCountWhere("countWhere", "intCol > 50"),
            AggCountDistinct("countDistinct=intCol"),
            AggDistinct("distinct=intCol"),
            AggFirst("first=intCol"),
            AggLast("last=intCol"),
            AggMax("max=intCol"),
            AggMin("min=intCol"),
            AggSortedFirst("Sym", "firstSorted=intCol"),
            AggSortedLast("Sym", "lastSorted=intCol"),
            AggStd("std=intCol"),
            AggSum("sum=intCol"),
            AggUnique("unique=intCol"),
            AggVar("var=intCol"),
            AggWAvg("intCol", "wavg=intCol"),
            AggWSum("intCol", "wsum=intCol"));

    // Companion list of columns to compare between rollup root and the zero-key equivalent
    private final String[] columnsToCompare = new String[] {
            "absSum",
            "avg",
            "count",
            "countWhere",
            "countDistinct",
            "distinct",
            "first",
            "last",
            "max",
            "min",
            "firstSorted",
            "lastSorted",
            "std",
            "sum",
            "unique",
            "var",
            "wavg",
            "wsum"
    };

    /**
     * Perform a large table test, comparing the rollup table root to the zero-key equivalent table, incorporating all
     * supported aggregations.
     */
    @Test
    public void testRollupVsZeroKeyStatic() {
        final Random random = new Random(0);
        // Create the test table
        final ColumnInfo[] columnInfo = initColumnInfos(
                new String[] {"Sym", "intCol"},
                new SetGenerator<>("a", "b", "c", "d"),
                new IntGenerator(10, 1_000));

        final Table testTable = getTable(false, 100_000, random, columnInfo);

        final RollupTable rollupTable = testTable.rollup(aggs, false, "Sym");
        final Table rootTable = rollupTable.getRoot();

        final Table actual = rootTable.select(columnsToCompare);
        final Table expected = testTable.aggBy(aggs);

        // Compare the zero-key equivalent table to the rollup table root
        TstUtils.assertTableEquals(actual, expected);
    }

    /**
     * Perform a large table test, comparing the rollup table root to the zero-key equivalent table, incorporating all
     * supported aggregations.
     */
    @Test
    public void testRollupVsZeroKeyIncremental() {
        for (int size = 10; size <= 1000; size *= 10) {
            testRollupIncrementalInternal("size-" + size, size);
        }
    }

    private void testRollupIncrementalInternal(final String ctxt, final int size) {
        final Random random = new Random(0);

        final ColumnInfo[] columnInfo = initColumnInfos(
                new String[] {"Sym", "intCol"},
                new SetGenerator<>("a", "b", "c", "d"),
                new IntGenerator(10, 1_000));

        final QueryTable testTable = getTable(true, 100_000, random, columnInfo);

        EvalNuggetInterface[] en = new EvalNuggetInterface[] {
                new QueryTableTest.TableComparator(
                        testTable.rollup(aggs, false, "Sym")
                                .getRoot().select(columnsToCompare),
                        testTable.aggBy(aggs))
        };

        final int steps = 100;
        for (int step = 0; step < steps; step++) {
            if (RefreshingTableTestCase.printTableUpdates) {
                System.out.println("Step = " + step);
            }
            simulateShiftAwareStep(ctxt + " step == " + step, size, random, testTable, columnInfo, en);
        }
    }

    @Test
    public void testRollupMultipleOps() throws CsvReaderException {
        final String data = "A,B,C,N\n" +
                "Apple,One,Alpha,1\n" +
                "Apple,One,Alpha,2\n" +
                "Apple,One,Bravo,3\n" +
                "Apple,One,Bravo,4\n" +
                "Apple,One,Bravo,5\n" +
                "Apple,One,Bravo,6\n" +
                "Banana,Two,Alpha,7\n" +
                "Banana,Two,Alpha,8\n" +
                "Banana,Two,Bravo,3\n" +
                "Banana,Two,Bravo,4\n" +
                "Banana,Three,Bravo,1\n" +
                "Banana,Three,Bravo,1\n";
        final Table source = CsvTools.readCsv(new ByteArrayInputStream(data.getBytes()));

        // Make a simple rollup
        final Collection<Aggregation> aggs = List.of(
                AggCount("count"),
                AggSum("sumN=N"));

        final String[] arrayWithNull = new String[1];

        final RollupTable rollupTable = source.rollup(aggs, false, "A", "B", "C");

        // format, update multiple times, then sort by the final updateView column
        final RollupTable customRollup = rollupTable.withNodeOperations(
                rollupTable.makeNodeOperationsRecorder(RollupTable.NodeType.Aggregated)
                        .formatColumns("sumN=`#00FF00`")
                        .updateView("sumNPlus1 = sumN + 1")
                        .formatColumns("sumNPlus1=`#FF0000`")
                        .updateView("sumNPlus2 = sumNPlus1 + 1")
                        .sort("sumNPlus2"));

        final Table customKeyTable = newTable(
                intCol(customRollup.getRowDepthColumn().name(), 0),
                stringCol("A", arrayWithNull),
                stringCol("B", arrayWithNull),
                stringCol("C", arrayWithNull),
                byteCol("Action", HierarchicalTable.KEY_TABLE_ACTION_EXPAND_ALL));

        final SnapshotState ssCustom = customRollup.makeSnapshotState();
        final Table customSnapshot =
                snapshotToTable(customRollup, ssCustom, customKeyTable, ColumnName.of("Action"), null,
                        RowSetFactory.flat(30));
        TableTools.showWithRowSet(customSnapshot);

        final Table expected = newTable(
                stringCol("A", null, "Apple", "Apple", "Apple", "Apple", "Banana", "Banana", "Banana", "Banana",
                        "Banana", "Banana"),
                stringCol("B", null, null, "One", "One", "One", null, "Three", "Three", "Two", "Two", "Two"),
                stringCol("C", null, null, null, "Alpha", "Bravo", null, null, "Bravo", null, "Bravo", "Alpha"),
                longCol("count", 12, 6, 6, 2, 4, 6, 2, 2, 4, 2, 2),
                longCol("sumN", 45, 21, 21, 3, 18, 24, 2, 2, 22, 7, 15),
                longCol("sumNPlus1", 46, 22, 22, 4, 19, 25, 3, 3, 23, 8, 16),
                longCol("sumNPlus2", 47, 23, 23, 5, 20, 26, 4, 4, 24, 9, 17));

        // Truncate the table and compare to expected.
        assertTableEquals(expected, customSnapshot.view("A", "B", "C", "count", "sumN", "sumNPlus1", "sumNPlus2"));

        freeSnapshotTableChunks(customSnapshot);
    }

    // endregion Test Rollup Tables

    @SuppressWarnings("SameParameterValue")
    private static Table snapshotToTable(
            @NotNull final HierarchicalTable<?> hierarchicalTable,
            @NotNull final SnapshotState snapshotState,
            @NotNull final Table keyTable,
            @Nullable final ColumnName keyTableActionColumn,
            @Nullable final BitSet columns,
            @NotNull final RowSequence rows) {
        final ColumnDefinition<?>[] availableColumns =
                hierarchicalTable.getAvailableColumnDefinitions().toArray(ColumnDefinition[]::new);
        final ColumnDefinition<?>[] includedColumns = columns == null
                ? availableColumns
                : columns.stream().mapToObj(ci -> availableColumns[ci]).toArray(ColumnDefinition[]::new);

        assertThat(rows.isContiguous()).isTrue();
        final int rowsSize = rows.intSize();
        // noinspection rawtypes
        final WritableChunk[] chunks = Arrays.stream(includedColumns)
                .map(cd -> maybeConvertToPrimitiveChunkType(cd.getDataType()))
                .map(ct -> ct.makeWritableChunk(rowsSize))
                .toArray(WritableChunk[]::new);

        // noinspection unchecked
        final long expandedSize =
                hierarchicalTable.snapshot(snapshotState, keyTable, keyTableActionColumn, columns, rows, chunks);
        final int snapshotSize = chunks.length == 0 ? 0 : chunks[0].size();
        final long expectedSnapshotSize = rows.isEmpty()
                ? 0
                : Math.min(rows.lastRowKey() + 1, expandedSize) - rows.firstRowKey();
        assertThat(snapshotSize).isEqualTo(expectedSnapshotSize);

        final LinkedHashMap<String, ColumnSource<?>> sources = new LinkedHashMap<>(includedColumns.length);
        for (int ci = 0; ci < includedColumns.length; ++ci) {
            final ColumnDefinition<?> columnDefinition = includedColumns[ci];
            // noinspection unchecked
            final WritableChunk<? extends Values> chunk = chunks[ci];
            final ChunkColumnSource<?> chunkColumnSource = ChunkColumnSource.make(
                    chunk.getChunkType(), columnDefinition.getDataType(), columnDefinition.getComponentType());
            chunkColumnSource.addChunk(chunk);
            final ColumnSource<?> source;
            if (columnDefinition.getDataType() == Boolean.class && chunkColumnSource.getType() == byte.class) {
                // noinspection unchecked
                source = byteToBooleanSource((ColumnSource<Byte>) chunkColumnSource);
            } else if (columnDefinition.getDataType() == Instant.class && chunkColumnSource.getType() == long.class) {
                // noinspection unchecked
                source = longToInstantSource((ColumnSource<Long>) chunkColumnSource);
            } else {
                source = chunkColumnSource;
            }
            sources.put(columnDefinition.getName(), source);
        }

        // noinspection resource
        return new QueryTable(
                TableDefinition.of(includedColumns),
                RowSetFactory.flat(snapshotSize).toTracking(),
                sources);
    }

    private static void freeSnapshotTableChunks(@NotNull final Table snapshotTable) {
        snapshotTable.getColumnSources().forEach(cs -> {
            if (cs instanceof ByteAsBooleanColumnSource) {
                ((ChunkColumnSource<?>) cs.reinterpret(byte.class)).clear();
            } else if (cs instanceof LongAsInstantColumnSource) {
                ((ChunkColumnSource<?>) cs.reinterpret(long.class)).clear();
            } else {
                ((ChunkColumnSource<?>) cs).clear();
            }
        });
    }
}

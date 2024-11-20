//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.hierarchical.HierarchicalTable;
import io.deephaven.engine.table.hierarchical.HierarchicalTable.SnapshotState;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.impl.sources.ByteAsBooleanColumnSource;
import io.deephaven.engine.table.impl.sources.LongAsInstantColumnSource;
import io.deephaven.engine.table.impl.sources.chunkcolumnsource.ChunkColumnSource;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.test.types.OutOfBandTest;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Instant;
import java.util.Arrays;
import java.util.BitSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static io.deephaven.api.agg.Aggregation.AggMax;
import static io.deephaven.engine.table.impl.sources.ReinterpretUtils.byteToBooleanSource;
import static io.deephaven.engine.table.impl.sources.ReinterpretUtils.longToInstantSource;
import static io.deephaven.engine.table.impl.sources.ReinterpretUtils.maybeConvertToPrimitiveChunkType;
import static io.deephaven.engine.testutil.TstUtils.addToTable;
import static io.deephaven.engine.testutil.TstUtils.assertTableEquals;
import static io.deephaven.engine.testutil.TstUtils.testRefreshingTable;
import static io.deephaven.engine.util.TableTools.booleanCol;
import static io.deephaven.engine.util.TableTools.byteCol;
import static io.deephaven.engine.util.TableTools.intCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link HierarchicalTable#snapshot(SnapshotState, Table, ColumnName, BitSet, RowSequence, WritableChunk[])
 * hierarchical table snapshots}.
 */
@Category(OutOfBandTest.class)
public class TestHierarchicalTableSnapshots {

    @Rule
    public final EngineCleanup framework = new EngineCleanup();

    @Test
    public void testRollupSnapshotSatisfaction() throws ExecutionException, InterruptedException {
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
                expandAllKeys, ColumnName.of("Action"), null, RowSetFactory.flat(4));
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
            assertFalse(snapshotFuture.isDone());
            updateGraph.flushOneNotificationForUnitTests();
        }
        // We may need to deliver one additional notification, in case the concurrent snapshot is waiting for a
        // WaitNotification to fire. We should not assert that the future isn't done, however, since a race might
        // prevent that notification from being needed.
        updateGraph.flushOneNotificationForUnitTests();

        final Table updatedSnapshot = snapshotFuture.get();
        updateGraph.completeCycleForUnitTests();
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

        assertTrue(rows.isContiguous());
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
        assertEquals(expectedSnapshotSize, snapshotSize);

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

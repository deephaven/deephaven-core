//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.table;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.RowSetShiftData;
import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.InstrumentedTableUpdateListenerAdapter;
import io.deephaven.engine.table.impl.sources.LongArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.TstUtils;
import io.deephaven.engine.testutil.testcase.RefreshingTableTestCase;
import io.deephaven.engine.updategraph.UpdateSourceCombiner;
import io.deephaven.extensions.barrage.util.BarrageUtil;
import io.deephaven.util.QueryConstants;
import io.deephaven.util.annotations.ReferentialIntegrity;
import org.junit.Test;

import java.time.Instant;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

import static io.deephaven.engine.util.TableTools.instantCol;
import static io.deephaven.engine.util.TableTools.longCol;
import static io.deephaven.engine.util.TableTools.newTable;
import static io.deephaven.engine.util.TableTools.stringCol;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * A {@link BarrageTable} subscribed to some of its table's columns stores, and reports changes to, only those columns,
 * whatever a server sends it for the others. Each test subscribes to {@code A} alone, and sends data for every column,
 * as a server does that writes other subscriptions' columns to every subscriber.
 */
public class BarrageTableUnsubscribedColumnsTest extends RefreshingTableTestCase {

    private static final TableDefinition DEFINITION = TableDefinition.of(
            ColumnDefinition.ofLong("A"),
            ColumnDefinition.ofLong("B"),
            ColumnDefinition.ofString("C"),
            ColumnDefinition.ofTime("D"));
    private static final String[] UNSUBSCRIBED_COLUMNS = {"B", "C", "D"};
    private static final int NUM_ROWS = 4;

    private UpdateSourceCombiner updateSourceCombiner;
    @ReferentialIntegrity
    private InstrumentedTableUpdateListenerAdapter listener;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        updateSourceCombiner = new UpdateSourceCombiner(ExecutionContext.getContext().getUpdateGraph());
    }

    @Override
    public void tearDown() throws Exception {
        listener = null;
        updateSourceCombiner = null;
        super.tearDown();
    }

    /**
     * Modified rows of a column the table is not subscribed to are not stored, and the update the table publishes
     * neither names the column as modified nor counts the rows modified only in it.
     */
    @Test
    public void testRedirectedTableIgnoresModifiedRowsOfUnsubscribedColumns() {
        final BarrageTable table = makeTable(Map.of());
        deliver(table, snapshot(NUM_ROWS));

        final List<BitSet> modifiedColumns = new ArrayList<>();
        final List<RowSet> modifiedRows = new ArrayList<>();
        listener = new InstrumentedTableUpdateListenerAdapter("modified columns", table, true) {
            @Override
            public void onUpdate(final TableUpdate upstream) {
                modifiedColumns.add(upstream.modifiedColumnSet().extractAsBitSet());
                modifiedRows.add(upstream.modified().copy());
            }
        };
        table.addUpdateListener(listener);

        deliver(table, modifyEveryColumn());

        assertEquals(1, modifiedColumns.size());
        final BitSet expectedColumns = new BitSet();
        expectedColumns.set(0);
        assertEquals(expectedColumns, modifiedColumns.get(0));
        try (final RowSet expectedRows = RowSetFactory.fromKeys(1)) {
            assertEquals(expectedRows, modifiedRows.get(0));
        }
        modifiedRows.forEach(RowSet::close);

        TstUtils.assertTableEquals(newTable(
                longCol("A", 10, 21, 12, 13),
                longCol("B", QueryConstants.NULL_LONG, QueryConstants.NULL_LONG, QueryConstants.NULL_LONG,
                        QueryConstants.NULL_LONG),
                stringCol("C", null, null, null, null),
                instantCol("D", null, null, null, null)), table);
        assertUnsubscribedColumnsReadNull(table);
        assertUnsubscribedColumnsUnallocated(table);
    }

    /** Added rows of a column a blink table is not subscribed to are not stored, nor is storage allocated for them. */
    @Test
    public void testBlinkTableAllocatesNoStorageForUnsubscribedColumns() {
        final BarrageTable table = makeTable(Map.of(Table.BLINK_TABLE_ATTRIBUTE, true));
        // a blink table's subscription is acknowledged with an empty snapshot
        deliver(table, snapshot(0));
        deliver(table, addRows());

        TstUtils.assertTableEquals(newTable(
                longCol("A", 10, 11, 12, 13),
                longCol("B", QueryConstants.NULL_LONG, QueryConstants.NULL_LONG, QueryConstants.NULL_LONG,
                        QueryConstants.NULL_LONG),
                stringCol("C", null, null, null, null),
                instantCol("D", null, null, null, null)), table);
        assertUnsubscribedColumnsReadNull(table);
        assertUnsubscribedColumnsUnallocated(table);
    }

    private BarrageTable makeTable(final Map<String, Object> attributes) {
        final BarrageUtil.ConvertedArrowSchema schema = BarrageUtil.convertArrowSchema(BarrageUtil.makeSchema(
                BarrageUtil.DEFAULT_SNAPSHOT_OPTIONS, DEFINITION, attributes, false));
        final BarrageTable table = BarrageTable.make(null, updateSourceCombiner,
                ExecutionContext.getContext().getUpdateGraph(), null, schema, true, null);
        table.addSourceToRegistrar();
        return table;
    }

    private void deliver(final BarrageTable table, final BarrageMessage message) {
        // the table retains the message until it applies it
        table.handleBarrageMessage(message);
        message.close();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        updateGraph.runWithinUnitTestCycle(updateSourceCombiner::run);
    }

    private static void assertUnsubscribedColumnsReadNull(final Table table) {
        for (final String name : UNSUBSCRIBED_COLUMNS) {
            final ColumnSource<?> source = table.getColumnSource(name);
            table.getRowSet().forAllRowKeys(key -> {
                assertNull(name + " at " + key, source.get(key));
                assertNull(name + " previous at " + key, source.getPrev(key));
            });
        }
    }

    private static void assertUnsubscribedColumnsUnallocated(final BarrageTable table) {
        assertTrue("A is allocated", capacity(table.destSources[0]) > 0);
        for (int ci = 1; ci < DEFINITION.numColumns(); ++ci) {
            assertEquals(DEFINITION.getColumns().get(ci).getName() + " capacity", 0, capacity(table.destSources[ci]));
        }
    }

    private static long capacity(final WritableColumnSource<?> source) {
        if (source instanceof LongArraySource) {
            return ((LongArraySource) source).getCapacity();
        }
        if (source instanceof ObjectArraySource) {
            return ((ObjectArraySource<?>) source).getCapacity();
        }
        throw new AssertionError("unexpected source " + source.getClass());
    }

    /**
     * A snapshot of {@code numRows} rows acknowledging a full subscription to {@code A}, holding data for every column.
     */
    private static BarrageMessage snapshot(final int numRows) {
        final BarrageMessage message = new BarrageMessage();
        message.isSnapshot = true;
        message.snapshotColumns = new BitSet();
        message.snapshotColumns.set(0);
        message.firstSeq = message.lastSeq = 0;
        message.tableSize = numRows;
        message.rowsAdded = RowSetFactory.flat(numRows);
        message.rowsIncluded = RowSetFactory.flat(numRows);
        message.rowsRemoved = RowSetFactory.empty();
        message.shifted = RowSetShiftData.EMPTY;
        message.addColumnData = numRows == 0 ? emptyAddColumns() : fullAddColumns();
        message.modColumnData = emptyModColumns();
        return message;
    }

    /** An update that adds {@code NUM_ROWS} rows, holding data for every column. */
    private static BarrageMessage addRows() {
        final BarrageMessage message = new BarrageMessage();
        message.firstSeq = message.lastSeq = 1;
        message.tableSize = NUM_ROWS;
        message.rowsAdded = RowSetFactory.flat(NUM_ROWS);
        message.rowsIncluded = RowSetFactory.flat(NUM_ROWS);
        message.rowsRemoved = RowSetFactory.empty();
        message.shifted = RowSetShiftData.EMPTY;
        message.addColumnData = fullAddColumns();
        message.modColumnData = emptyModColumns();
        return message;
    }

    /** An update that modifies rows of every column, and of {@code A} only row 1. */
    private static BarrageMessage modifyEveryColumn() {
        final BarrageMessage message = new BarrageMessage();
        message.firstSeq = message.lastSeq = 1;
        message.tableSize = NUM_ROWS;
        message.rowsAdded = RowSetFactory.empty();
        message.rowsIncluded = RowSetFactory.empty();
        message.rowsRemoved = RowSetFactory.empty();
        message.shifted = RowSetShiftData.EMPTY;
        message.addColumnData = emptyAddColumns();
        message.modColumnData = new BarrageMessage.ModColumnData[] {
                modColumn(long.class, RowSetFactory.fromKeys(1), longs(21)),
                modColumn(long.class, RowSetFactory.fromKeys(2, 3), longs(32, 33)),
                modColumn(String.class, RowSetFactory.fromKeys(0, 3), strings("c0", "c3")),
                modColumn(Instant.class, RowSetFactory.fromKeys(0), longs(40)),
        };
        return message;
    }

    private static BarrageMessage.AddColumnData[] fullAddColumns() {
        return new BarrageMessage.AddColumnData[] {
                addColumn(long.class, longs(10, 11, 12, 13)),
                addColumn(long.class, longs(20, 21, 22, 23)),
                addColumn(String.class, strings("c0", "c1", "c2", "c3")),
                addColumn(Instant.class, longs(30, 31, 32, 33)),
        };
    }

    private static BarrageMessage.AddColumnData[] emptyAddColumns() {
        final BarrageMessage.AddColumnData[] columns = new BarrageMessage.AddColumnData[DEFINITION.numColumns()];
        for (int ci = 0; ci < columns.length; ++ci) {
            columns[ci] = addColumn(DEFINITION.getColumns().get(ci).getDataType(), null);
        }
        return columns;
    }

    private static BarrageMessage.ModColumnData[] emptyModColumns() {
        final BarrageMessage.ModColumnData[] columns = new BarrageMessage.ModColumnData[DEFINITION.numColumns()];
        for (int ci = 0; ci < columns.length; ++ci) {
            columns[ci] = modColumn(DEFINITION.getColumns().get(ci).getDataType(), RowSetFactory.empty(), null);
        }
        return columns;
    }

    private static BarrageMessage.AddColumnData addColumn(final Class<?> type, final Chunk<Values> data) {
        final BarrageMessage.AddColumnData column = new BarrageMessage.AddColumnData();
        column.type = type;
        column.chunkType = chunkType(type);
        column.data = new ArrayList<>();
        if (data != null) {
            column.data.add(data);
        }
        return column;
    }

    private static BarrageMessage.ModColumnData modColumn(
            final Class<?> type, final RowSet rowsModified, final Chunk<Values> data) {
        final BarrageMessage.ModColumnData column = new BarrageMessage.ModColumnData();
        column.type = type;
        column.chunkType = chunkType(type);
        column.rowsModified = rowsModified;
        column.data = new ArrayList<>();
        if (data != null) {
            column.data.add(data);
        }
        return column;
    }

    /** Instants travel as their epoch nanoseconds. */
    private static ChunkType chunkType(final Class<?> type) {
        return type == String.class ? ChunkType.Object : ChunkType.Long;
    }

    private static Chunk<Values> longs(final long... values) {
        final WritableLongChunk<Values> chunk = WritableLongChunk.makeWritableChunk(values.length);
        chunk.copyFromTypedArray(values, 0, 0, values.length);
        return chunk;
    }

    private static Chunk<Values> strings(final String... values) {
        final WritableObjectChunk<String, Values> chunk = WritableObjectChunk.makeWritableChunk(values.length);
        chunk.copyFromTypedArray(values, 0, 0, values.length);
        return chunk;
    }
}

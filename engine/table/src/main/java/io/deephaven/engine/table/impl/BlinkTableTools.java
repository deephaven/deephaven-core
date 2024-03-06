//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.engine.updategraph.UpdateGraph;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Tools for manipulating blink tables.
 *
 * @see Table#BLINK_TABLE_ATTRIBUTE
 */
public class BlinkTableTools {
    public static final Object DEFAULT_MEMO_KEY = new Object() {
        @Override
        public String toString() {
            return "DEFAULT_MEMOIZATION_KEY";
        }
    };

    /**
     * Convert a Blink Table to an in-memory append only table.
     * <p>
     * Note, this table will grow without bound as new blink table rows are encountered. The result is memoized under
     * {@link #DEFAULT_MEMO_KEY}.
     *
     * @param blinkTable The input blink table
     * @return An append-only in-memory table representing all data encountered in the blink table across all cycles
     */
    public static Table blinkToAppendOnly(final Table blinkTable) {
        // Setting the size limit as maximum allowed value
        return blinkToAppendOnly(blinkTable, Long.MAX_VALUE, DEFAULT_MEMO_KEY);
    }

    /**
     * Convert a Blink Table to an in-memory append only table.
     * <p>
     * Note, this table will grow without bound as new blink table rows are encountered.
     *
     * @param blinkTable The input blink table
     * @param memoKey saves a weak reference to the result of the given operation under the given memoization key (null
     *        to disable memoization)
     * @return An append-only in-memory table representing all data encountered in the blink table across all cycles
     */
    public static Table blinkToAppendOnly(@NotNull final Table blinkTable, @Nullable final Object memoKey) {
        // Setting the size limit as maximum allowed value
        return blinkToAppendOnly(blinkTable, Long.MAX_VALUE, memoKey);
    }

    /**
     * Convert a Blink Table to an in-memory append only table with a limit on maximum size. Any updates beyond that
     * limit won't be appended to the table.
     * <p>
     * The result is memoized under {@link #DEFAULT_MEMO_KEY}.
     *
     * @param blinkTable The input blink table
     * @param sizeLimit The maximum number of rows in the append-only table
     * @return An append-only in-memory table representing all data encountered in the blink table across all cycles
     *         till maximum row count
     */
    public static Table blinkToAppendOnly(@NotNull final Table blinkTable, long sizeLimit) {
        return blinkToAppendOnly(blinkTable, sizeLimit, DEFAULT_MEMO_KEY);
    }

    /**
     * Convert a Blink Table to an in-memory append only table with a limit on maximum size. Any updates beyond that
     * limit won't be appended to the table.
     *
     * @param blinkTable The input blink table
     * @param sizeLimit The maximum number of rows in the append-only table
     * @param memoKey saves a weak reference to the result of the given operation under the given size limit and
     *        memoization key (null to disable memoization)
     * @return An append-only in-memory table representing all data encountered in the blink table across all cycles
     *         till maximum row count
     */
    public static Table blinkToAppendOnly(
            final Table blinkTable,
            final long sizeLimit,
            @Nullable final Object memoKey) {
        if (sizeLimit < 0) {
            throw new IllegalArgumentException("Size limit cannot be negative, limit=" + sizeLimit);
        }
        if (!isBlink(blinkTable)) {
            throw new IllegalArgumentException("Input is not a blink table!");
        }
        final UpdateGraph updateGraph = blinkTable.getUpdateGraph();
        try (final SafeCloseable ignored = ExecutionContext.getContext().withUpdateGraph(updateGraph).open()) {
            final QueryTable coalesced = (QueryTable) (blinkTable.coalesce());
            return coalesced.getResult(new BlinkToAppendOnlyOperation(coalesced, sizeLimit, memoKey));
        }
    }

    /**
     * Returns true if {@code table} is a blink table.
     *
     * @param table The table to check for blink behavior
     * @return Whether {@code table} is a blink table
     * @see Table#BLINK_TABLE_ATTRIBUTE
     */
    public static boolean isBlink(@NotNull final Table table) {
        if (!table.isRefreshing()) {
            return false;
        }
        return Boolean.TRUE.equals(table.getAttribute(Table.BLINK_TABLE_ATTRIBUTE));
    }

    /**
     * Returns true if {@code attributes} indicate a blink table.
     *
     * @param attributes The map to check for blink table attributes
     * @return Whether {@code attributes} indicate a blink table
     * @see Table#BLINK_TABLE_ATTRIBUTE
     */
    public static boolean hasBlink(@NotNull final Map<String, Object> attributes) {
        return Boolean.TRUE.equals(attributes.get(Table.BLINK_TABLE_ATTRIBUTE));
    }

    private static class BlinkToAppendOnlyOperation implements QueryTable.MemoizableOperation<QueryTable> {
        private final QueryTable parent;
        private final long sizeLimit;
        private final Object memoKey;
        private final ColumnSource<?>[] sourceColumns;
        private final WritableColumnSource<?>[] destColumns;

        private QueryTable resultTable;
        private BaseTable.ListenerImpl resultListener;

        private BlinkToAppendOnlyOperation(
                @NotNull final QueryTable parent,
                final long sizeLimit,
                @Nullable final Object memoKey) {
            this.parent = parent;
            this.sizeLimit = sizeLimit;
            this.memoKey = memoKey;

            this.sourceColumns = new ColumnSource<?>[parent.numColumns()];
            this.destColumns = new WritableColumnSource<?>[parent.numColumns()];
        }

        @Override
        public String getDescription() {
            final String sizeLimitStr = sizeLimit == Long.MAX_VALUE ? "unbounded" : Long.toString(sizeLimit);
            final String memoKeyStr = memoKey == null ? "none" : memoKey.toString();
            return String.format("BlinkTableTools.blinkToAppendOnly(%s, %s)", sizeLimitStr, memoKeyStr);
        }

        @Override
        public String getLogPrefix() {
            return "BlinkTableTools.blinkToAppendOnly";
        }

        @Override
        public MemoizedOperationKey getMemoizedOperationKey() {
            return memoKey == null ? null : MemoizedOperationKey.blinkToAppendOnly(sizeLimit, memoKey);
        }

        @Override
        public Result<QueryTable> initialize(boolean usePrev, long beforeClock) {
            final Map<String, ? extends ColumnSource<?>> parentColumns = parent.getColumnSourceMap();
            final Map<String, WritableColumnSource<?>> resultColumns = new LinkedHashMap<>(parentColumns.size());

            // note that we do not need to enable prev tracking for an add-only table
            int colIdx = 0;
            for (Map.Entry<String, ? extends ColumnSource<?>> sourceEntry : parentColumns
                    .entrySet()) {
                final ColumnSource<?> sourceColumn = sourceEntry.getValue();
                final WritableColumnSource<?> newColumn = ArrayBackedColumnSource.getMemoryColumnSource(
                        0, sourceColumn.getType(), sourceColumn.getComponentType());
                resultColumns.put(sourceEntry.getKey(), newColumn);

                // read and write primitives whenever possible
                sourceColumns[colIdx] = ReinterpretUtils.maybeConvertToPrimitive(sourceColumn);
                destColumns[colIdx++] = ReinterpretUtils.maybeConvertToWritablePrimitive(newColumn);
            }

            final RowSet rowSetToUse = usePrev ? parent.getRowSet().prev() : parent.getRowSet();
            final RowSet initialRowSet = rowSetToUse.isEmpty()
                    ? RowSetFactory.empty()
                    : appendRows(0, rowSetToUse, usePrev);
            resultTable = new QueryTable(initialRowSet.writableCast().toTracking(), resultColumns);
            resultTable.setRefreshing(true);
            resultTable.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
            resultTable.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);
            resultTable.setFlat();

            if (resultTable.size() < sizeLimit) {
                resultListener = new BaseTable.ListenerImpl(getDescription(), parent, resultTable) {
                    @Override
                    public void onUpdate(TableUpdate upstream) {
                        BlinkToAppendOnlyOperation.this.onUpdate(upstream);
                    }
                };
            }

            return new Result<>(resultTable, resultListener);
        }

        private void onUpdate(final TableUpdate upstream) {
            if (upstream.modified().isNonempty() || upstream.shifted().nonempty()) {
                throw new IllegalStateException("Blink tables should not modify or shift!");
            }
            if (upstream.added().isEmpty()) {
                return;
            }

            final TableUpdateImpl downstream = new TableUpdateImpl();
            downstream.added = appendRows(resultTable.size(), upstream.added(), false);
            Assert.eqTrue(downstream.added.isNonempty(), "downstream.added.isNonempty()");
            resultTable.getRowSet().writableCast().insertRange(
                    downstream.added.firstRowKey(), downstream.added.lastRowKey());
            downstream.modified = RowSetFactory.empty();
            downstream.removed = RowSetFactory.empty();
            downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
            downstream.shifted = RowSetShiftData.EMPTY;
            resultTable.notifyListeners(downstream);

            if (resultTable.size() == sizeLimit) {
                // No more rows can be appended, so remove the listener and remove all references
                resultListener.forceReferenceCountToZero();
                resultListener = null;
            }
        }

        private RowSet appendRows(final long currentSize, final RowSet newRows, final boolean usePrev) {
            long newRowsSize = newRows.size();
            Assert.gtZero(newRowsSize, "newRowsSize");
            RowSet rowsToAdd = null;
            if (currentSize > sizeLimit - newRowsSize) {
                newRowsSize = (sizeLimit - currentSize);
                rowsToAdd = newRows.subSetByPositionRange(0, newRowsSize);
            }
            final long totalSize = currentSize + newRowsSize;
            final RowSet newRange = RowSetFactory.fromRange(currentSize, totalSize - 1);

            try (final SafeCloseable ignored = rowsToAdd) {
                if (rowsToAdd == null) {
                    rowsToAdd = newRows;
                }
                ChunkUtils.copyData(sourceColumns, rowsToAdd, destColumns, newRange, usePrev);
            }
            Assert.leq(totalSize, "totalSize", sizeLimit, "sizeLimit");

            return newRange;
        }
    }
}

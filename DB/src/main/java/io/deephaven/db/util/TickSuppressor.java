/*
 * Copyright (c) 2020 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.util;

import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.live.LiveTableMonitor;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.hashing.ChunkEquals;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.OrderedKeys;
import io.deephaven.util.SafeCloseableArray;
import org.apache.commons.lang3.mutable.MutableInt;

/**
 * Tools for reducing the number of ticks generated by a table.
 */
public class TickSuppressor {
    private TickSuppressor() {} // static use only

    /**
     * For shift aware listeners, the modified column set cannot mark particular rows and columns dirty; only all of the
     * columns in the modified rows.  However, rows can be both removed and added and those rows do not affect the
     * modified column set.
     *
     * <p>If you have a table that has a small number of modified rows with many modified columns;
     * and join on a right-hand side that modifies many rows, but few columns; downstream operations must treat all rows
     * and columns in the cross product as modified.</p>
     *
     * <p>This utility function will convert all modified rows to added and removed rows, such that downstream operations
     * can modify rows without additionally marking the columns of this table dirty.</p>
     *
     * @param input an input table
     * @return an output table that will produce no modified rows, but rather adds and removes instead
     */
    public static Table convertModificationsToAddsAndRemoves(Table input) {
        if (!input.isLive()) {
            return input;
        }

        LiveTableMonitor.DEFAULT.checkInitiateTableOperation();

        final QueryTable resultTable = new QueryTable(input.getDefinition(), input.getIndex(), input.getColumnSourceMap());
        ((BaseTable)input).copyAttributes(resultTable, BaseTable.CopyAttributeOperation.Filter);

        final BaseTable.ShiftAwareListenerImpl listener = new BaseTable.ShiftAwareListenerImpl("convertModificationsToAddsAndRemoves", (DynamicTable) input, resultTable) {
            @Override
            public void onUpdate(Update upstream) {
                final Update downstream = upstream.copy();
                downstream.added = upstream.added.union(upstream.modified);
                downstream.removed = upstream.removed.union(upstream.getModifiedPreShift());
                downstream.modified = Index.FACTORY.getEmptyIndex();
                downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                resultTable.notifyListeners(downstream);
            }
        };
        ((DynamicTable)input).listenForUpdates(listener);
        return resultTable;
    }

    /**
     * Removes spurious modifications from an update.
     *
     * <p>The Deephaven query engine guarantees that any row or column that has been modified, must be marked modified in
     * an update.  However, for efficiency, it does not guarantee that only rows with changed data are marked as
     * modified.  There are cases where a query writer would like to remove spurious modifications.  For example if
     * a downstream listener is sending network messages eliminating additional messages may be worthwhile.</p>
     *
     * <p>This function produces a new query table with the same contents as the original query table.  For each
     * modified row and column, if a row has not actually been modified or a column has no modifications; then remove
     * the modification from the downstream update.</p>
     *
     * @param input an input table
     *
     * @return an output table where the set of modified rows and columns is restricted to cells where current and
     * previous values are not identical
     */
    public static Table removeSpuriousModifications(Table input) {
        if (!input.isLive()) {
            return input;
        }

        LiveTableMonitor.DEFAULT.checkInitiateTableOperation();

        final QueryTable resultTable = new QueryTable(input.getDefinition(), input.getIndex(), input.getColumnSourceMap());
        ((BaseTable)input).copyAttributes(resultTable, BaseTable.CopyAttributeOperation.Filter);

        final String[] columnNames = input.getDefinition().getColumnNamesArray();
        final ModifiedColumnSet [] inputModifiedColumnSets = new ModifiedColumnSet[columnNames.length];
        final ModifiedColumnSet [] outputModifiedColumnSets = new ModifiedColumnSet[columnNames.length];
        final ColumnSource [] inputSources = new ColumnSource[columnNames.length];
        final ChunkEquals [] equalityKernel = new ChunkEquals[columnNames.length];
        for (int cc = 0; cc < outputModifiedColumnSets.length; ++cc) {
            inputModifiedColumnSets[cc] = ((DynamicTable)input).newModifiedColumnSet(columnNames[cc]);
            outputModifiedColumnSets[cc] = resultTable.newModifiedColumnSet(columnNames[cc]);
            inputSources[cc] = input.getColumnSource(columnNames[cc]);
            equalityKernel[cc] = ChunkEquals.makeEqual(inputSources[cc].getChunkType());
        }


        final BaseTable.ShiftAwareListenerImpl listener = new BaseTable.ShiftAwareListenerImpl("removeSpuriousModifications", (DynamicTable) input, resultTable) {
            final ModifiedColumnSet.Transformer identityTransformer = ((DynamicTable)input).newModifiedColumnSetIdentityTransformer(resultTable);
            @Override
            public void onUpdate(Update upstream) {
                final Update downstream = upstream.copy();
                downstream.modifiedColumnSet = resultTable.getModifiedColumnSetForUpdates();

                if (downstream.modified.isEmpty()) {
                    identityTransformer.clearAndTransform(upstream.modifiedColumnSet, downstream.modifiedColumnSet);
                    resultTable.notifyListeners(downstream);
                    return;
                }

                final int columnCount = resultTable.getColumnSourceMap().size();
                final int chunkSize = (int)Math.min(1 << 16, downstream.modified.size());

                final ChunkSource.GetContext [] getContextArray = new ChunkSource.GetContext[columnCount];
                final ChunkSource.GetContext [] prevContextArray = new ChunkSource.GetContext[columnCount];
                final WritableBooleanChunk [] changedCellsArray = new WritableBooleanChunk[columnCount];
                final boolean [] changedColumns = new boolean[columnCount];

                final Index.SequentialBuilder builder = Index.CURRENT_FACTORY.getSequentialBuilder();

                try (final SafeCloseableArray<ChunkSource.GetContext> ignored = new SafeCloseableArray<>(getContextArray);
                     final SafeCloseableArray<ChunkSource.GetContext> ignored2 = new SafeCloseableArray<>(prevContextArray);
                     final SafeCloseableArray<WritableBooleanChunk> ignored3 = new SafeCloseableArray<>(changedCellsArray);
                     final SharedContext currentSharedContext = SharedContext.makeSharedContext();
                     final SharedContext prevSharedContext = SharedContext.makeSharedContext();
                     final OrderedKeys.Iterator preOkIt = upstream.getModifiedPreShift().getOrderedKeysIterator();
                     final OrderedKeys.Iterator postOkIt = upstream.modified.getOrderedKeysIterator()) {
                    int changedColumnCount = 0;
                    for (int cc = 0; cc < columnCount; cc++) {
                        if (upstream.modifiedColumnSet.containsAny(inputModifiedColumnSets[cc])) {
                            getContextArray[cc] = inputSources[cc].makeGetContext(chunkSize, currentSharedContext);
                            prevContextArray[cc] = inputSources[cc].makeGetContext(chunkSize, prevSharedContext);
                            changedCellsArray[cc] = WritableBooleanChunk.makeWritableChunk(chunkSize);
                            changedColumnCount++;
                        }
                    }
                    final int [] changedColumnIndices = new int[changedColumnCount];
                    int cp = 0;
                    for (int cc = 0; cc < columnCount; cc++) {
                        if (upstream.modifiedColumnSet.containsAny(inputModifiedColumnSets[cc])) {
                            changedColumnIndices[cp++] = cc;
                        }
                    }

                    while (postOkIt.hasMore()) {
                        try (final OrderedKeys postChunkOk = postOkIt.getNextOrderedKeysWithLength(chunkSize);
                        final OrderedKeys preChunkOk = preOkIt.getNextOrderedKeysWithLength(chunkSize)) {
                            currentSharedContext.reset();
                            prevSharedContext.reset();

                            for (final int cc : changedColumnIndices) {
                                //noinspection unchecked
                                final Chunk<Attributes.Values> currentValues = inputSources[cc].getChunk(getContextArray[cc], postChunkOk);
                                //noinspection unchecked
                                final Chunk<Attributes.Values> prevValues = inputSources[cc].getPrevChunk(prevContextArray[cc], preChunkOk);

                                // now we need to compare them
                                equalityKernel[cc].notEqual(currentValues, prevValues, changedCellsArray[cc]);
                            }

                            final MutableInt pos = new MutableInt(0);
                            postChunkOk.forAllLongs((idx) -> {
                                boolean idxChanged = false;
                                for (final int cc : changedColumnIndices) {
                                    if (changedCellsArray[cc].get(pos.intValue())) {
                                        idxChanged = changedColumns[cc] = true;
                                    }
                                }
                                if (idxChanged) {
                                    builder.appendKey(idx);
                                }
                                pos.increment();
                            });
                        }
                    }
                }

                downstream.modified = builder.getIndex();

                downstream.modifiedColumnSet.clear();
                if (downstream.modified.nonempty()) {
                    for (int cc = 0; cc < changedColumns.length; ++cc) {
                        if (changedColumns[cc]) {
                            downstream.modifiedColumnSet.setAll(outputModifiedColumnSets[cc]);
                        }
                    }
                }

                resultTable.notifyListeners(downstream);
            }
        };
        ((DynamicTable)input).listenForUpdates(listener);

        return resultTable;
    }
}

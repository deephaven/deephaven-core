/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.sources.RedirectedColumnSource;
import io.deephaven.engine.table.impl.util.*;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

public class FlattenOperation implements QueryTable.MemoizableOperation<QueryTable> {

    @Override
    public String getDescription() {
        return "flatten()";
    }

    @Override
    public String getLogPrefix() {
        return "flatten";
    }

    @Override
    public MemoizedOperationKey getMemoizedOperationKey() {
        return MemoizedOperationKey.flatten();
    }

    @Override
    public Result<QueryTable> initialize(boolean usePrev, long beforeClock) {
        final TrackingRowSet rowSet = parent.getRowSet();
        final Map<String, ColumnSource<?>> resultColumns = new LinkedHashMap<>();
        final RowRedirection rowRedirection = new WrappedRowSetRowRedirection(rowSet);

        final long size = usePrev ? rowSet.sizePrev() : rowSet.size();

        for (Map.Entry<String, ColumnSource<?>> entry : parent.getColumnSourceMap().entrySet()) {
            resultColumns.put(entry.getKey(), RedirectedColumnSource.maybeRedirect(rowRedirection, entry.getValue()));
        }

        resultTable = new QueryTable(RowSetFactory.flat(size).toTracking(), resultColumns);
        resultTable.setFlat();
        parent.copyAttributes(resultTable, BaseTable.CopyAttributeOperation.Flatten);

        TableUpdateListener resultListener = null;
        if (parent.isRefreshing()) {
            resultListener = new BaseTable.ListenerImpl(getDescription(), parent, resultTable) {
                @Override
                public void onUpdate(TableUpdate upstream) {
                    FlattenOperation.this.onUpdate(upstream);
                }
            };
        }

        prevSize = size;
        mcsTransformer = parent.newModifiedColumnSetIdentityTransformer(resultTable);
        return new Result<>(resultTable, resultListener);
    }

    private final QueryTable parent;

    private long prevSize;
    private QueryTable resultTable;
    private ModifiedColumnSet.Transformer mcsTransformer;

    FlattenOperation(final QueryTable parent) {
        this.parent = parent;
    }

    private void onUpdate(final TableUpdate upstream) {
        // Note: we can safely ignore shifted since shifts do not change data AND shifts are not allowed to reorder.
        final TrackingRowSet rowSet = parent.getRowSet();
        final long newSize = rowSet.size();

        final TableUpdateImpl downstream = new TableUpdateImpl();
        downstream.modifiedColumnSet = resultTable.getModifiedColumnSetForUpdates();
        mcsTransformer.clearAndTransform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);

        // Check to see if we can simply invert and pass-down.
        downstream.modified = rowSet.invert(upstream.modified());
        if (upstream.added().isEmpty() && upstream.removed().isEmpty()) {
            downstream.added = RowSetFactory.empty();
            downstream.removed = RowSetFactory.empty();
            downstream.shifted = RowSetShiftData.EMPTY;
            resultTable.notifyListeners(downstream);
            return;
        }

        downstream.added = rowSet.invert(upstream.added());
        try (final RowSet prevRowSet = rowSet.copyPrev()) {
            downstream.removed = prevRowSet.invert(upstream.removed());
        }
        final RowSetShiftData.Builder outShifted = new RowSetShiftData.Builder();

        // Helper to ensure that we can prime iterators and still detect the end.
        final Consumer<MutableObject<RowSet.RangeIterator>> updateIt = (it) -> {
            if (it.getValue().hasNext()) {
                it.getValue().next();
            } else {
                it.setValue(null);
            }
        };

        // Create our range iterators and prime them.
        final MutableObject<RowSet.RangeIterator> rmIt = new MutableObject<>(downstream.removed().rangeIterator());
        final MutableObject<RowSet.RangeIterator> addIt = new MutableObject<>(downstream.added().rangeIterator());
        updateIt.accept(rmIt);
        updateIt.accept(addIt);

        // Iterate through these ranges to generate shift instructions.
        long currDelta = 0; // converts from prev key-space to new key-space
        long currMarker = 0; // everything less than this marker is accounted for

        while (rmIt.getValue() != null || addIt.getValue() != null) {
            final long nextRm = rmIt.getValue() == null ? RowSequence.NULL_ROW_KEY
                    : rmIt.getValue().currentRangeStart();
            final long nextAdd = addIt.getValue() == null ? RowSequence.NULL_ROW_KEY
                    : addIt.getValue().currentRangeStart() - currDelta;

            if (nextRm == nextAdd) { // note neither can be null in this case
                final long dtRm = rmIt.getValue().currentRangeEnd() - rmIt.getValue().currentRangeStart() + 1;
                final long dtAdd = addIt.getValue().currentRangeEnd() - addIt.getValue().currentRangeStart() + 1;

                // shift only if these don't cancel each other out
                if (dtRm != dtAdd) {
                    outShifted.shiftRange(currMarker, nextAdd - 1, currDelta);
                    currDelta += dtAdd - dtRm;
                    currMarker = rmIt.getValue().currentRangeEnd() + 1;
                }

                updateIt.accept(rmIt);
                updateIt.accept(addIt);
            } else if (nextAdd == RowSequence.NULL_ROW_KEY
                    || (nextRm != RowSequence.NULL_ROW_KEY && nextRm < nextAdd)) {
                // rmIt cannot be null
                final long dtRm = rmIt.getValue().currentRangeEnd() - rmIt.getValue().currentRangeStart() + 1;

                outShifted.shiftRange(currMarker, nextRm - 1, currDelta);
                currDelta -= dtRm;
                currMarker = rmIt.getValue().currentRangeEnd() + 1;
                updateIt.accept(rmIt);
            } else {
                // addIt cannot be null
                final long dtAdd = addIt.getValue().currentRangeEnd() - addIt.getValue().currentRangeStart() + 1;

                outShifted.shiftRange(currMarker, nextAdd - 1, currDelta);
                currDelta += dtAdd;
                currMarker = nextAdd;
                updateIt.accept(addIt);
            }
        }

        // finishing shift if remaining chunk is non-empty (it might be empty if we removed the end)
        if (currMarker < prevSize) {
            outShifted.shiftRange(currMarker, prevSize - 1, currDelta);
        }

        if (newSize < prevSize) {
            resultTable.getRowSet().writableCast().removeRange(newSize, prevSize - 1);
        } else if (newSize > prevSize) {
            resultTable.getRowSet().writableCast().insertRange(prevSize, newSize - 1);
        }

        downstream.shifted = outShifted.build();
        prevSize = newSize;
        resultTable.notifyListeners(downstream);
    }
}

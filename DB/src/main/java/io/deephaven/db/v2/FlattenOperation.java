/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ReadOnlyRedirectedColumnSource;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.IndexShiftData;
import io.deephaven.db.v2.utils.RedirectionIndex;
import io.deephaven.db.v2.utils.WrappedIndexRedirectionIndexImpl;
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
        final Index index = parent.getIndex();
        final Map<String, ColumnSource<?>> resultColumns = new LinkedHashMap<>();
        final RedirectionIndex redirectionIndex = new WrappedIndexRedirectionIndexImpl(index);

        final long size = usePrev ? index.sizePrev() : index.size();

        for (Map.Entry<String, ColumnSource<?>> entry : parent.getColumnSourceMap().entrySet()) {
            resultColumns.put(entry.getKey(), new ReadOnlyRedirectedColumnSource<>(redirectionIndex, entry.getValue()));
        }

        resultTable = new QueryTable(Index.FACTORY.getFlatIndex(size), resultColumns);
        resultTable.setFlat();
        parent.copyAttributes(resultTable, BaseTable.CopyAttributeOperation.Flatten);

        ShiftAwareListener resultListener = null;
        if (parent.isRefreshing()) {
            resultListener = new BaseTable.ShiftAwareListenerImpl(getDescription(), parent, resultTable) {
                @Override
                public void onUpdate(Update upstream) {
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

    private void onUpdate(final ShiftAwareListener.Update upstream) {
        // Note: we can safely ignore shifted since shifts do not change data AND shifts are not allowed to reorder.
        final Index index = parent.getIndex();
        final long newSize = index.size();

        final ShiftAwareListener.Update downstream = new ShiftAwareListener.Update();
        downstream.modifiedColumnSet = resultTable.modifiedColumnSet;
        mcsTransformer.clearAndTransform(upstream.modifiedColumnSet, downstream.modifiedColumnSet);

        // Check to see if we can simply invert and pass-down.
        downstream.modified = index.invert(upstream.modified);
        if (upstream.added.empty() && upstream.removed.empty()) {
            downstream.added = Index.FACTORY.getEmptyIndex();
            downstream.removed = Index.FACTORY.getEmptyIndex();
            downstream.shifted = IndexShiftData.EMPTY;
            resultTable.notifyListeners(downstream);
            return;
        }

        downstream.added = index.invert(upstream.added);
        try (final Index prevIndex = index.getPrevIndex()) {
            downstream.removed = prevIndex.invert(upstream.removed);
        }
        final IndexShiftData.Builder outShifted = new IndexShiftData.Builder();

        // Helper to ensure that we can prime iterators and still detect the end.
        final Consumer<MutableObject<Index.RangeIterator>> updateIt = (it) -> {
            if (it.getValue().hasNext()) {
                it.getValue().next();
            } else {
                it.setValue(null);
            }
        };

        // Create our range iterators and prime them.
        final MutableObject<Index.RangeIterator> rmIt = new MutableObject<>(downstream.removed.rangeIterator());
        final MutableObject<Index.RangeIterator> addIt = new MutableObject<>(downstream.added.rangeIterator());
        updateIt.accept(rmIt);
        updateIt.accept(addIt);

        // Iterate through these ranges to generate shift instructions.
        long currDelta = 0; // converts from prev key-space to new key-space
        long currMarker = 0; // everything less than this marker is accounted for

        while (rmIt.getValue() != null || addIt.getValue() != null) {
            final long nextRm = rmIt.getValue() == null ? Index.NULL_KEY
                    : rmIt.getValue().currentRangeStart();
            final long nextAdd = addIt.getValue() == null ? Index.NULL_KEY
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
            } else if (nextAdd == Index.NULL_KEY || (nextRm != Index.NULL_KEY && nextRm < nextAdd)) {
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
            resultTable.getIndex().removeRange(newSize, prevSize - 1);
        } else if (newSize > prevSize) {
            resultTable.getIndex().insertRange(prevSize, newSize - 1);
        }

        downstream.shifted = outShifted.build();
        prevSize = newSize;
        resultTable.notifyListeners(downstream);
    }
}

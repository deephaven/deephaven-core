/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.db.v2.utils.Index;

public class SliceLikeOperation implements QueryTable.Operation<QueryTable> {

    public static SliceLikeOperation slice(final QueryTable parent, final long firstPositionInclusive,
            final long lastPositionExclusive, final String op) {

        if (firstPositionInclusive < 0 && lastPositionExclusive > 0) {
            throw new IllegalArgumentException("Can not slice with a negative first position (" + firstPositionInclusive
                    + ") and positive last position (" + lastPositionExclusive + ")");
        }
        // note: first >= 0 && last < 0 is allowed, otherwise first must be less than last
        if ((firstPositionInclusive < 0 || lastPositionExclusive >= 0)
                && lastPositionExclusive < firstPositionInclusive) {
            throw new IllegalArgumentException("Can not slice with a first position (" + firstPositionInclusive
                    + ") after last position (" + lastPositionExclusive + ")");
        }

        return new SliceLikeOperation(op, op + "(" + firstPositionInclusive + ", " + lastPositionExclusive + ")",
                parent, firstPositionInclusive, lastPositionExclusive, firstPositionInclusive == 0);
    }

    public static SliceLikeOperation headPct(final QueryTable parent, final double percent) {
        return new SliceLikeOperation("headPct", "headPct(" + percent + ")", parent,
                0, 0, true) {
            @Override
            protected long getLastPositionExclusive() {
                return (long) Math.ceil(percent * parent.size());
            }
        };
    }

    public static SliceLikeOperation tailPct(final QueryTable parent, final double percent) {
        return new SliceLikeOperation("tailPct", "tailPct(" + percent + ")", parent,
                0, 0, false) {
            @Override
            protected long getFirstPositionInclusive() {
                return -(long) Math.ceil(percent * parent.size());
            }
        };
    }

    private final String operation;
    private final String description;
    private final QueryTable parent;
    private final long _firstPositionInclusive; // use the accessor
    private final long _lastPositionExclusive; // use the accessor
    private final boolean isFlat;
    private QueryTable resultTable;

    private SliceLikeOperation(final String operation, final String description, final QueryTable parent,
            final long firstPositionInclusive, final long lastPositionExclusive,
            final boolean mayBeFlat) {
        this.operation = operation;
        this.description = description;
        this.parent = parent;
        this._firstPositionInclusive = firstPositionInclusive;
        this._lastPositionExclusive = lastPositionExclusive;
        this.isFlat = parent.isFlat() && mayBeFlat;
    }

    @Override
    public String getDescription() {
        return description;
    }

    @Override
    public String getLogPrefix() {
        return operation;
    }

    protected long getFirstPositionInclusive() {
        return _firstPositionInclusive;
    }

    protected long getLastPositionExclusive() {
        return _lastPositionExclusive;
    }

    @Override
    public Result initialize(boolean usePrev, long beforeClock) {
        final Index parentIndex = parent.getIndex();
        final Index resultIndex = computeSliceIndex(usePrev ? parentIndex.getPrevIndex() : parentIndex);

        // result table must be a sub-table so we can pass ModifiedColumnSet to listeners when possible
        resultTable = parent.getSubTable(resultIndex);
        if (isFlat) {
            resultTable.setFlat();
        }

        ShiftAwareListener resultListener = null;
        if (parent.isRefreshing()) {
            resultListener = new BaseTable.ShiftAwareListenerImpl(getDescription(), parent, resultTable) {
                @Override
                public void onUpdate(Update upstream) {
                    SliceLikeOperation.this.onUpdate(upstream);
                }
            };
        }

        return new Result(resultTable, resultListener);
    }

    private void onUpdate(final ShiftAwareListener.Update upstream) {
        final Index index = resultTable.getIndex();
        final Index sliceIndex = computeSliceIndex(parent.getIndex());

        final ShiftAwareListener.Update downstream = new ShiftAwareListener.Update();
        downstream.removed = upstream.removed.intersect(index);
        index.remove(downstream.removed);

        downstream.shifted = upstream.shifted.intersect(index);
        downstream.shifted.apply(index);

        // Must calculate in post-shift space what indices were removed by the slice operation.
        final Index opRemoved = index.minus(sliceIndex);
        index.remove(opRemoved);
        downstream.shifted.unapply(opRemoved);
        downstream.removed.insert(opRemoved);

        // Must intersect against modified set before adding the new rows to result index.
        downstream.modified = upstream.modified.intersect(index);

        downstream.added = sliceIndex.minus(index);
        index.insert(downstream.added);

        // propagate an empty MCS if modified is empty
        downstream.modifiedColumnSet = upstream.modifiedColumnSet;
        if (downstream.modified.empty()) {
            downstream.modifiedColumnSet = resultTable.modifiedColumnSet;
            downstream.modifiedColumnSet.clear();
        }

        resultTable.notifyListeners(downstream);
    }

    private Index computeSliceIndex(Index useIndex) {
        final long size = parent.size();
        long startSlice = getFirstPositionInclusive();
        long endSlice = getLastPositionExclusive();

        if (startSlice < 0) {
            if (endSlice == 0) { // special code for tail!
                endSlice = size;
            }
            startSlice = Math.max(0, startSlice + size);
        }
        if (endSlice < 0) {
            endSlice = Math.max(0, endSlice + size);
        }
        // allow [firstPos,-lastPos] by being tolerant of overlap
        endSlice = Math.max(startSlice, endSlice);

        return useIndex.subindexByPos(startSlice, endSlice);
    }
}

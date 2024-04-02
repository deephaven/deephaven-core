//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;

public class SliceLikeOperation implements QueryTable.Operation<QueryTable> {

    public static SliceLikeOperation slice(final QueryTable parent, final long firstPositionInclusive,
            final long lastPositionExclusive, final String op) {

        // note: first >= 0 && last < 0 is allowed, otherwise first must be less than last
        if ((firstPositionInclusive < 0 || lastPositionExclusive >= 0)
                && lastPositionExclusive < firstPositionInclusive) {
            throw new IllegalArgumentException("Can not slice with a first position (" + firstPositionInclusive
                    + ") after last position (" + lastPositionExclusive + ")");
        }

        return new SliceLikeOperation(op, op + "(" + firstPositionInclusive + ", " + lastPositionExclusive + ")",
                parent, firstPositionInclusive, lastPositionExclusive, firstPositionInclusive == 0);
    }

    public static SliceLikeOperation slicePct(final QueryTable parent, final double startPercentInclusive,
            final double endPercentExclusive) {

        if (startPercentInclusive < 0 || startPercentInclusive > 1
                || endPercentExclusive < 0 || endPercentExclusive > 1) {
            throw new IllegalArgumentException(
                    "Cannot slice with start percentage (" + startPercentInclusive + ") and end percentage ("
                            + endPercentExclusive + "), percentages must be between [0, 1]");
        }
        return new SliceLikeOperation("slicePct",
                "slicePct(" + startPercentInclusive + ", " + endPercentExclusive + ")",
                parent, 0, 0, startPercentInclusive == 0) {
            @Override
            protected long getFirstPositionInclusive() {
                return (long) Math.floor(startPercentInclusive * parent.size());
            }

            @Override
            protected long getLastPositionExclusive() {
                return (long) Math.floor(endPercentExclusive * parent.size());
            }
        };
    }

    public static SliceLikeOperation headPct(final QueryTable parent, final double percent) {
        return new SliceLikeOperation("headPct", "headPct(" + percent + ")", parent,
                0, 0, true) {
            @Override
            protected long getLastPositionExclusive() {
                // Already verified percent is between [0,1] here
                return (long) Math.ceil(percent * parent.size());
            }
        };
    }

    public static SliceLikeOperation tailPct(final QueryTable parent, final double percent) {
        return new SliceLikeOperation("tailPct", "tailPct(" + percent + ")", parent,
                0, 0, false) {
            @Override
            protected long getFirstPositionInclusive() {
                // Already verified percent is between [0,1] here
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
    public Result<QueryTable> initialize(boolean usePrev, long beforeClock) {
        final TrackingRowSet parentRowSet = parent.getRowSet();
        final TrackingRowSet resultRowSet =
                computeSliceRowSet(usePrev ? parentRowSet.prev() : parentRowSet).toTracking();
        // result table must be a sub-table so we can pass ModifiedColumnSet to listeners when possible
        resultTable = parent.getSubTable(resultRowSet);
        if (isFlat) {
            resultTable.setFlat();
        }

        if (operation.equals("headPct")) {
            // With headPct, resultTable has a floating tail. So if parent is ADD_ONLY, new rows might be added and old
            // rows removed from resultTable as the parent grows. But if parent is APPEND_ONLY, new rows can only be
            // appended to resultTable
            if (parent.isAppendOnly()) {
                resultTable.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
                resultTable.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);
            }
        } else if (!operation.equals("tailPct") && getFirstPositionInclusive() >= 0) {
            // With tailPct or when first position is negative, even if the parent is APPEND_ONLY, new rows can be added
            // and old rows removed from resultTable as the parent grows. Therefore, we cannot propagate the property.
            // Otherwise, we can propagate APPEND_ONLY and conditionally propagate ADD_ONLY.
            if (parent.isAddOnly() && getLastPositionExclusive() <= 0) {
                // The tail cannot be fixed, else we might need to remove rows
                resultTable.setAttribute(Table.ADD_ONLY_TABLE_ATTRIBUTE, true);
            }
            if (parent.isAppendOnly()) {
                resultTable.setAttribute(Table.APPEND_ONLY_TABLE_ATTRIBUTE, true);
            }
        }

        TableUpdateListener resultListener = null;
        if (parent.isRefreshing()) {
            resultListener = new BaseTable.ListenerImpl(getDescription(), parent, resultTable) {
                @Override
                public void onUpdate(TableUpdate upstream) {
                    SliceLikeOperation.this.onUpdate(upstream);
                }
            };
        }

        return new Result<>(resultTable, resultListener);
    }

    private void onUpdate(final TableUpdate upstream) {
        final TrackingWritableRowSet rowSet = resultTable.getRowSet().writableCast();
        final RowSet sliceRowSet = computeSliceRowSet(parent.getRowSet());

        final TableUpdateImpl downstream = new TableUpdateImpl();
        downstream.removed = upstream.removed().intersect(rowSet);
        rowSet.remove(downstream.removed());

        downstream.shifted = upstream.shifted().intersect(rowSet);
        downstream.shifted().apply(rowSet);

        // Must calculate in post-shift space what indices were removed by the slice operation.
        final WritableRowSet opRemoved = rowSet.minus(sliceRowSet);
        rowSet.remove(opRemoved);
        downstream.shifted().unapply(opRemoved);
        downstream.removed().writableCast().insert(opRemoved);

        // Must intersect against modified set before adding the new rows to result rowSet.
        downstream.modified = upstream.modified().intersect(rowSet);

        downstream.added = sliceRowSet.minus(rowSet);
        rowSet.insert(downstream.added());

        // propagate an empty MCS if modified is empty
        downstream.modifiedColumnSet = upstream.modifiedColumnSet();
        if (downstream.modified().isEmpty()) {
            downstream.modifiedColumnSet = resultTable.getModifiedColumnSetForUpdates();
            downstream.modifiedColumnSet.clear();
        }

        resultTable.notifyListeners(downstream);
    }

    private WritableRowSet computeSliceRowSet(RowSet useRowSet) {
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

        return useRowSet.subSetByPositionRange(startSlice, endSlice);
    }
}

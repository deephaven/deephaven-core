/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.updategraph.LogicalClock;
import io.deephaven.engine.v2.sources.ReversedColumnSource;
import io.deephaven.engine.v2.sources.UnionRedirection;

import java.util.LinkedHashMap;
import java.util.Map;

public class ReverseOperation implements QueryTable.MemoizableOperation<QueryTable> {

    private final QueryTable parent;
    private QueryTable resultTable;
    private ModifiedColumnSet.Transformer mcsTransformer;

    // minimum pivot is rowSet container size -- this guarantees that we only generate container shifts
    private static final long MINIMUM_PIVOT = UnionRedirection.CHUNK_MULTIPLE;
    // since we are using highest one bit, this should be a power of two
    private static final int PIVOT_GROWTH_FACTOR = 4;

    private long pivotPoint;
    private long prevPivot;
    private long lastPivotChange;
    private long resultSize;

    public ReverseOperation(QueryTable parent) {
        this.parent = parent;
    }

    @Override
    public String getDescription() {
        return "reverse()";
    }

    @Override
    public String getLogPrefix() {
        return "reverse";
    }

    @Override
    public MemoizedOperationKey getMemoizedOperationKey() {
        return MemoizedOperationKey.reverse();
    }

    @Override
    public SwapListener newSwapListener(QueryTable queryTable) {
        return new SwapListener(queryTable) {
            @Override
            public synchronized boolean end(long clockCycle) {
                final boolean success = super.end(clockCycle);
                if (success) {
                    QueryTable.startTrackingPrev(resultTable.getColumnSources());
                }
                return success;
            }
        };
    }

    @Override
    public Result<QueryTable> initialize(boolean usePrev, long beforeClock) {
        final RowSet rowSetToReverse = usePrev ? parent.getRowSet().getPrevRowSet() : parent.getRowSet();
        prevPivot = pivotPoint = computePivot(rowSetToReverse.lastRowKey());
        lastPivotChange = usePrev ? beforeClock - 1 : beforeClock;

        final Map<String, ColumnSource<?>> resultMap = new LinkedHashMap<>();
        for (Map.Entry<String, ColumnSource<?>> entry : parent.getColumnSourceMap().entrySet()) {
            resultMap.put(entry.getKey(), new ReversedColumnSource<>(entry.getValue(), this));
        }

        final TrackingMutableRowSet rowSet = transform(rowSetToReverse).toTracking();
        resultSize = rowSet.size();
        Assert.eq(resultSize, "resultSize", rowSetToReverse.size(), "rowSetToReverse.size()");

        resultTable = new QueryTable(parent.getDefinition(), rowSet, resultMap);
        mcsTransformer = parent.newModifiedColumnSetIdentityTransformer(resultTable);
        parent.copyAttributes(resultTable, BaseTable.CopyAttributeOperation.Reverse);

        if (!parent.isRefreshing()) {
            return new Result<>(resultTable);
        }

        final Listener listener =
                new BaseTable.ListenerImpl(getDescription(), parent, resultTable) {
                    @Override
                    public void onUpdate(final Update upstream) {
                        ReverseOperation.this.onUpdate(upstream);
                    }
                };

        return new Result<>(resultTable, listener);
    }

    private void onUpdate(final Listener.Update upstream) {
        final MutableRowSet rowSet = resultTable.getRowSet().mutableCast();
        final RowSet parentRowSet = parent.getRowSet();
        Assert.eq(resultSize, "resultSize", rowSet.size(), "rowSet.size()");

        if (parentRowSet.size() != (rowSet.size() + upstream.added.size() - upstream.removed.size())) {
            QueryTable.log.error()
                    .append("Size Mismatch: Result rowSet: ")
                    .append(rowSet).append(" size=").append(rowSet.size())
                    .append(", Original rowSet: ")
                    .append(parentRowSet).append(" size=").append(parentRowSet.size())
                    .append(", Added: ").append(upstream.added).append(" size=").append(upstream.added.size())
                    .append(", Removed: ").append(upstream.removed).append(" size=").append(upstream.removed.size())
                    .endl();
            throw new IllegalStateException();
        }

        final Listener.Update downstream = new Listener.Update();

        // removed is in pre-shift keyspace
        downstream.removed = transform(upstream.removed);
        rowSet.remove(downstream.removed);

        // transform shifted and apply to our rowSet
        final long newShift =
                (parentRowSet.lastRowKey() > pivotPoint) ? computePivot(parentRowSet.lastRowKey()) - pivotPoint : 0;
        if (upstream.shifted.nonempty() || newShift > 0) {
            long watermarkKey = 0;
            final RowSetShiftData.Builder oShiftedBuilder = new RowSetShiftData.Builder();

            // Bounds seem weird because we might need to shift all keys outside of shifts too.
            for (int idx = upstream.shifted.size(); idx >= 0; --idx) {
                final long nextShiftEnd;
                final long nextShiftStart;
                final long nextShiftDelta;
                if (idx <= 0) {
                    nextShiftStart = nextShiftEnd = pivotPoint + 1;
                    nextShiftDelta = 0;
                } else {
                    // Note: begin/end flip responsibilities in the transformation
                    nextShiftDelta = -upstream.shifted.getShiftDelta(idx - 1);
                    final long minStart = Math.max(-nextShiftDelta - newShift, 0);
                    nextShiftStart = Math.max(minStart, transform(upstream.shifted.getEndRange(idx - 1)));
                    nextShiftEnd = transform(upstream.shifted.getBeginRange(idx - 1));
                    if (nextShiftEnd < nextShiftStart) {
                        continue;
                    }
                }

                // insert range prior to here; note shift ends are inclusive so we need the -1 for endRange
                long innerEnd = nextShiftStart - 1 + (nextShiftDelta < 0 ? nextShiftDelta : 0);
                oShiftedBuilder.shiftRange(watermarkKey, innerEnd, newShift);

                if (idx <= 0) {
                    continue;
                }

                // insert this range
                oShiftedBuilder.shiftRange(nextShiftStart, nextShiftEnd, newShift + nextShiftDelta);
                watermarkKey = nextShiftEnd + 1 + (nextShiftDelta > 0 ? nextShiftDelta : 0);
            }

            downstream.shifted = oShiftedBuilder.build();
            RowSetShiftUtils.apply(downstream.shifted, rowSet);

            // Update pivot logic.
            lastPivotChange = LogicalClock.DEFAULT.currentStep();
            prevPivot = pivotPoint;
            pivotPoint += newShift;
        } else {
            downstream.shifted = RowSetShiftData.EMPTY;
        }

        // added/modified are in post-shift keyspace
        downstream.added = transform(upstream.added);
        rowSet.insert(downstream.added);
        downstream.modified = transform(upstream.modified);

        Assert.eq(downstream.added.size(), "update.added.size()", upstream.added.size(), "upstream.added.size()");
        Assert.eq(downstream.removed.size(), "update.removed.size()", upstream.removed.size(),
                "upstream.removed.size()");
        Assert.eq(downstream.modified.size(), "update.modified.size()", upstream.modified.size(),
                "upstream.modified.size()");

        downstream.modifiedColumnSet = resultTable.modifiedColumnSet;
        downstream.modifiedColumnSet.clear();
        if (downstream.modified.isNonempty()) {
            mcsTransformer.transform(upstream.modifiedColumnSet, downstream.modifiedColumnSet);
        }

        if (rowSet.size() != parentRowSet.size()) {
            QueryTable.log.error()
                    .append("Size Mismatch: Result rowSet: ").append(rowSet)
                    .append("Original rowSet: ").append(parentRowSet)
                    .append("Upstream update: ").append(upstream)
                    .append("Downstream update: ").append(downstream)
                    .endl();
            Assert.neq(rowSet.size(), "rowSet.size()", parentRowSet.size(), "parent.build().size()");
        }

        resultTable.notifyListeners(downstream);
        resultSize = rowSet.size();
    }

    private long computePivot(long maxInnerIndex) {
        final long highestOneBit = Long.highestOneBit(maxInnerIndex);
        if (highestOneBit > (Long.MAX_VALUE / PIVOT_GROWTH_FACTOR)) {
            return Long.MAX_VALUE;
        } else {
            // make it big enough that we should be able to accommodate what we are adding now, plus a bit more
            return Math.max(highestOneBit * PIVOT_GROWTH_FACTOR - 1, MINIMUM_PIVOT);
        }
    }

    private long getPivotPrev() {
        if ((prevPivot != pivotPoint) && (LogicalClock.DEFAULT.currentStep() != lastPivotChange)) {
            prevPivot = pivotPoint;
        }
        return prevPivot;
    }

    /**
     * Transform an outer (reversed) rowSet to the inner (unreversed) rowSet, or vice versa.
     *
     * @param rowSetToTransform the outer rowSet
     * @return the corresponding inner rowSet
     */
    public MutableRowSet transform(final RowSet rowSetToTransform) {
        return transform(rowSetToTransform, false);
    }

    /**
     * Transform an outer (reversed) rowSet to the inner (unreversed) rowSet as of the previous cycle, or vice versa.
     *
     * @param outerRowSet the outer rowSet
     * @return the corresponding inner rowSet
     */
    public MutableRowSet transformPrev(final RowSet outerRowSet) {
        return transform(outerRowSet, true);
    }

    private MutableRowSet transform(final RowSet outerRowSet, final boolean usePrev) {
        final long pivot = usePrev ? getPivotPrev() : pivotPoint;
        final RowSetBuilderRandom reversedBuilder = RowSetFactory.builderRandom();

        for (final RowSet.RangeIterator rangeIterator = outerRowSet.rangeIterator(); rangeIterator.hasNext();) {
            rangeIterator.next();
            final long startValue = rangeIterator.currentRangeStart();
            final long endValue = rangeIterator.currentRangeEnd();
            final long transformedStart = (startValue < 0) ? startValue : pivot - startValue;
            final long transformedEnd = (endValue < 0) ? endValue : pivot - endValue;
            Assert.geqZero(transformedStart, "transformedStart");
            Assert.geqZero(transformedEnd, "transformedEnd");
            Assert.leq(transformedEnd, "transformedEnd", transformedStart, "transformedStart");
            reversedBuilder.addRange(transformedEnd, transformedStart);
        }

        return reversedBuilder.build();
    }

    /**
     * Transform an outer (reversed) rowSet to the inner (unreversed) rowSet, or vice versa.
     *
     * @param outerIndex the outer rowSet
     * @return the corresponding inner rowSet
     */
    public long transform(long outerIndex) {
        return (outerIndex < 0) ? outerIndex : pivotPoint - outerIndex;
    }

    /**
     * Transform an outer (reversed) rowSet to the inner (unreversed) rowSet as of the previous cycle, or vice versa.
     *
     * @param outerIndex the outer rowSet
     * @return the corresponding inner rowSet
     */
    public long transformPrev(long outerIndex) {
        return (outerIndex < 0) ? outerIndex : getPivotPrev() - outerIndex;
    }
}

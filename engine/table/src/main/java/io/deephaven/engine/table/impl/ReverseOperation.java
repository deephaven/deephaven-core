//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.impl.rsp.RspArray;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.TableUpdate;
import io.deephaven.engine.table.TableUpdateListener;
import io.deephaven.engine.table.impl.sources.ReversedColumnSource;
import io.deephaven.util.annotations.VisibleForTesting;
import org.jetbrains.annotations.NotNull;

import java.util.LinkedHashMap;
import java.util.Map;

public class ReverseOperation implements QueryTable.MemoizableOperation<QueryTable> {

    private final QueryTable parent;
    private QueryTable resultTable;
    private ModifiedColumnSet.Transformer mcsTransformer;

    /**
     * Minimum pivot is RSP container size. This guarantees that we only generate shifts that are a multiple of
     * container size, which is important if we're using an RSP-backed OrderedLongSet to implement our RowSet.
     */
    @VisibleForTesting
    static final long MINIMUM_PIVOT = RspArray.BLOCK_SIZE;
    /**
     * Maximum pivot is the maximum possible row key.
     */
    private static final long MAXIMUM_PIVOT = Long.MAX_VALUE;
    /**
     * Since we are using highest one bit of maximum parent row key, this should be a power of two greater than one.
     */
    private static final int PIVOT_GROWTH_FACTOR = 4;

    private long pivotPoint;
    private long prevPivotPoint;
    private long lastPivotPointChange;

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
    public OperationSnapshotControl newSnapshotControl(QueryTable queryTable) {
        return new OperationSnapshotControl(queryTable) {
            @Override
            public synchronized boolean snapshotCompletedConsistently(
                    final long afterClockValue,
                    final boolean usedPreviousValues) {
                final boolean success = super.snapshotCompletedConsistently(afterClockValue, usedPreviousValues);
                if (success) {
                    QueryTable.startTrackingPrev(resultTable.getColumnSources());
                }
                return success;
            }
        };
    }

    @Override
    public Result<QueryTable> initialize(boolean usePrev, long beforeClock) {
        final RowSet rowSetToReverse = usePrev ? parent.getRowSet().prev() : parent.getRowSet();
        prevPivotPoint = pivotPoint = computePivot(rowSetToReverse.lastRowKey());
        lastPivotPointChange = usePrev ? beforeClock - 1 : beforeClock;

        final Map<String, ColumnSource<?>> resultColumnSources = new LinkedHashMap<>();
        for (Map.Entry<String, ColumnSource<?>> entry : parent.getColumnSourceMap().entrySet()) {
            resultColumnSources.put(entry.getKey(), new ReversedColumnSource<>(entry.getValue(), this));
        }

        final TrackingWritableRowSet resultRowSet = transform(rowSetToReverse).toTracking();
        Assert.eq(resultRowSet.size(), "resultRowSet.size()", rowSetToReverse.size(), "rowSetToReverse.size()");

        resultTable = new QueryTable(parent.getDefinition(), resultRowSet, resultColumnSources);
        mcsTransformer = parent.newModifiedColumnSetIdentityTransformer(resultTable);
        parent.copyAttributes(resultTable, BaseTable.CopyAttributeOperation.Reverse);

        if (!parent.isRefreshing()) {
            return new Result<>(resultTable);
        }

        final TableUpdateListener listener =
                new BaseTable.ListenerImpl(getDescription(), parent, resultTable) {
                    @Override
                    public void onUpdate(final TableUpdate upstream) {
                        ReverseOperation.this.onUpdate(upstream);
                    }
                };

        return new Result<>(resultTable, listener);
    }

    private void onUpdate(final TableUpdate upstream) {
        final WritableRowSet resultRowSet = resultTable.getRowSet().writableCast();
        final TrackingRowSet parentRowSet = parent.getRowSet();
        if (resultRowSet.size() != parentRowSet.sizePrev()) {
            QueryTable.log.error()
                    .append("Result Size Mismatch: Pre-Update Result RowSet: ")
                    .append(resultRowSet).append(" size=").append(resultRowSet.size())
                    .append(", Parent RowSet: ")
                    .append(parentRowSet).append(" size=").append(parentRowSet.size())
                    .append(", Parent Previous RowSet: ")
                    .append(parentRowSet.prev()).append(" size=").append(parentRowSet.sizePrev())
                    .append(", Added: ").append(upstream.added()).append(" size=").append(upstream.added().size())
                    .append(", Removed: ").append(upstream.removed()).append(" size=").append(upstream.removed().size())
                    .endl();
            Assert.eq(resultRowSet.size(), "resultRowSet.size()", parentRowSet.sizePrev(), "parentRowSet.sizePrev()");
        }

        if (parentRowSet.size() != (resultRowSet.size() + upstream.added().size() - upstream.removed().size())) {
            QueryTable.log.error()
                    .append("Parent Size Mismatch: Pre-Update Result RowSet: ")
                    .append(resultRowSet).append(" size=").append(resultRowSet.size())
                    .append(", Parent RowSet: ")
                    .append(parentRowSet).append(" size=").append(parentRowSet.size())
                    .append(", Added: ").append(upstream.added()).append(" size=").append(upstream.added().size())
                    .append(", Removed: ").append(upstream.removed()).append(" size=").append(upstream.removed().size())
                    .endl();
            Assert.eq(parentRowSet.size(), "parentRowSet.size()",
                    resultRowSet.size() + upstream.added().size() - upstream.removed().size(),
                    "resultRowSet.size() + upstream.added().size() - upstream.removed().size()");
        }

        final TableUpdateImpl downstream = new TableUpdateImpl();

        // removed is in pre-shift keyspace
        downstream.removed = transform(upstream.removed());
        resultRowSet.remove(downstream.removed());

        // transform shifted and apply to our RowSet
        final long newShift =
                (parentRowSet.lastRowKey() > pivotPoint) ? computePivot(parentRowSet.lastRowKey()) - pivotPoint : 0;
        if (upstream.shifted().nonempty() || newShift > 0) {
            // Only compute downstream shifts if there are retained rows to shift
            if (resultRowSet.isEmpty()) {
                downstream.shifted = RowSetShiftData.EMPTY;
            } else {
                long watermarkKey = 0;
                final RowSetShiftData.Builder oShiftedBuilder = new RowSetShiftData.Builder();

                // Bounds seem weird because we might need to shift all keys outside of shifts too.
                for (int idx = upstream.shifted().size(); idx >= 0; --idx) {
                    final long nextShiftEnd;
                    final long nextShiftStart;
                    final long nextShiftDelta;
                    if (idx == 0) {
                        nextShiftStart = nextShiftEnd = pivotPoint + 1;
                        nextShiftDelta = 0;
                    } else {
                        // Note: begin/end flip responsibilities in the transformation
                        nextShiftDelta = -upstream.shifted().getShiftDelta(idx - 1);
                        final long minStart = Math.max(-nextShiftDelta - newShift, 0);
                        nextShiftStart = Math.max(minStart, transform(upstream.shifted().getEndRange(idx - 1)));
                        nextShiftEnd = transform(upstream.shifted().getBeginRange(idx - 1));
                        if (nextShiftEnd < nextShiftStart) {
                            continue;
                        }
                    }

                    // insert range prior to here; note shift ends are inclusive so we need the -1 for endRange
                    long innerEnd = nextShiftStart - 1 + (nextShiftDelta < 0 ? nextShiftDelta : 0);
                    oShiftedBuilder.shiftRange(watermarkKey, innerEnd, newShift);

                    if (idx == 0) {
                        continue;
                    }

                    // insert this range
                    oShiftedBuilder.shiftRange(nextShiftStart, nextShiftEnd, newShift + nextShiftDelta);
                    watermarkKey = nextShiftEnd + 1 + (nextShiftDelta > 0 ? nextShiftDelta : 0);
                }

                downstream.shifted = oShiftedBuilder.build();
                downstream.shifted().apply(resultRowSet);
            }

            // Update pivot logic.
            lastPivotPointChange = parent.getUpdateGraph().clock().currentStep();
            prevPivotPoint = pivotPoint;
            pivotPoint += newShift;
        } else {
            downstream.shifted = RowSetShiftData.EMPTY;
        }

        // added/modified are in post-shift keyspace
        downstream.added = transform(upstream.added());
        resultRowSet.insert(downstream.added());
        downstream.modified = transform(upstream.modified());

        Assert.eq(downstream.added().size(), "update.added.size()", upstream.added().size(), "upstream.added.size()");
        Assert.eq(downstream.removed().size(), "update.removed.size()", upstream.removed().size(),
                "upstream.removed.size()");
        Assert.eq(downstream.modified().size(), "update.modified.size()", upstream.modified().size(),
                "upstream.modified.size()");

        downstream.modifiedColumnSet = resultTable.getModifiedColumnSetForUpdates();
        downstream.modifiedColumnSet.clear();
        if (downstream.modified().isNonempty()) {
            mcsTransformer.transform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
        }

        if (resultRowSet.size() != parentRowSet.size()) {
            QueryTable.log.error()
                    .append("Size Mismatch: Result RowSet: ").append(resultRowSet)
                    .append("Parent RowSet: ").append(parentRowSet)
                    .append("Upstream Update: ").append(upstream)
                    .append("Downstream Update: ").append(downstream)
                    .endl();
            Assert.eq(resultRowSet.size(), "resultRowSet.size()", parentRowSet.size(), "parentRowSet.size()");
        }

        resultTable.notifyListeners(downstream);
    }

    private static long computePivot(final long parentLastRowKey) {
        final long highestOneBit = Long.highestOneBit(parentLastRowKey);
        if (highestOneBit > (MAXIMUM_PIVOT / PIVOT_GROWTH_FACTOR)) {
            return MAXIMUM_PIVOT;
        } else {
            // Make it big enough that we should be able to accommodate what we are adding now, plus a bit more
            return Math.max(highestOneBit * PIVOT_GROWTH_FACTOR - 1, MINIMUM_PIVOT);
        }
    }

    private long getPrevPivotPoint() {
        if ((prevPivotPoint != pivotPoint)) {
            if (parent.getUpdateGraph().clock().currentStep() != lastPivotPointChange) {
                prevPivotPoint = pivotPoint;
            }
        }
        return prevPivotPoint;
    }

    /**
     * Transform an outer (reversed) RowSet to the inner (un-reversed) RowSet, or vice versa.
     *
     * @param outerRowSet The outer (reversed) RowSet
     * @return The corresponding inner RowSet
     */
    public WritableRowSet transform(@NotNull final RowSet outerRowSet) {
        return transform(outerRowSet, false);
    }

    /**
     * Transform an outer (reversed) RowSet to the inner (un-reversed) RowSet as of the previous cycle, or vice versa.
     *
     * @param outerRowSet The outer (reversed) RowSet
     * @return The corresponding inner RowSet
     */
    public WritableRowSet transformPrev(@NotNull final RowSet outerRowSet) {
        return transform(outerRowSet, true);
    }

    private WritableRowSet transform(@NotNull final RowSet outerRowSet, final boolean usePrev) {
        final long pivot = usePrev ? getPrevPivotPoint() : pivotPoint;
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
     * Transform an outer (reversed) row key to the inner (un-reversed) row key, or vice versa.
     *
     * @param outerRowKey The outer (reversed) row key
     * @return The corresponding inner row key
     */
    public long transform(final long outerRowKey) {
        return (outerRowKey < 0) ? outerRowKey : pivotPoint - outerRowKey;
    }

    /**
     * Transform an outer (reversed) row key to the inner (un-reversed) row key as of the previous cycle, or vice versa.
     *
     * @param outerRowKey The outer (reversed) row key
     * @return The corresponding inner row key
     */
    public long transformPrev(final long outerRowKey) {
        return (outerRowKey < 0) ? outerRowKey : getPrevPivotPoint() - outerRowKey;
    }
}

package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.exceptions.OutOfKeySpaceException;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.impl.join.JoinListenerRecorder;
import io.deephaven.engine.table.impl.sources.BitMaskingColumnSource;
import io.deephaven.engine.table.impl.sources.BitShiftingColumnSource;
import io.deephaven.engine.table.impl.sources.CrossJoinRightColumnSource;
import io.deephaven.util.SafeCloseableList;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.LongConsumer;

/**
 * Implementation for chunk-oriented aggregation operations, including {@link Table#join}.
 */
public class CrossJoinHelper {
    // Note: This would be >= 16 to get efficient performance from WritableRowSet#insert and
    // WritableRowSet#shiftInPlace. However, it is
    // very costly for joins of many small groups for the default to be so high.
    public static final int DEFAULT_NUM_RIGHT_BITS_TO_RESERVE = Configuration.getInstance()
            .getIntegerForClassWithDefault(CrossJoinHelper.class, "numRightBitsToReserve", 10);

    /**
     * Static-use only.
     */
    private CrossJoinHelper() {}

    static Table join(final QueryTable leftTable, final QueryTable rightTable, final MatchPair[] columnsToMatch,
            final MatchPair[] columnsToAdd, final int numReserveRightBits) {
        return join(leftTable, rightTable, columnsToMatch, columnsToAdd, numReserveRightBits, new JoinControl());
    }

    static Table join(final QueryTable leftTable, final QueryTable rightTable, final MatchPair[] columnsToMatch,
            final MatchPair[] columnsToAdd, final int numReserveRightBits, final JoinControl control) {
        final Table result =
                internalJoin(leftTable, rightTable, columnsToMatch, columnsToAdd, numReserveRightBits, control);
        leftTable.maybeCopyColumnDescriptions(result, rightTable, columnsToMatch, columnsToAdd);
        return result;
    }

    private static Table internalJoin(final QueryTable leftTable, final QueryTable rightTable,
            final MatchPair[] columnsToMatch, final MatchPair[] columnsToAdd, int numRightBitsToReserve,
            final JoinControl control) {
        QueryTable.checkInitiateOperation(leftTable);
        QueryTable.checkInitiateOperation(rightTable);

        try (final BucketingContext bucketingContext =
                new BucketingContext("join", leftTable, rightTable, columnsToMatch, columnsToAdd, control)) {
            // TODO: if we have a single column of unique values, and the range is small, we can use a simplified table
            // if (!rightTable.isRefreshing()
            // && control.useUniqueTable(uniqueValues, maximumUniqueValue, minumumUniqueValue)){ (etc)
            if (bucketingContext.keyColumnCount == 0) {
                if (!leftTable.isRefreshing() && !rightTable.isRefreshing()) {
                    numRightBitsToReserve = 1; // tight computation of this is efficient and appropriate
                }
                return zeroKeyColumnsJoin(leftTable, rightTable, columnsToAdd, numRightBitsToReserve,
                        bucketingContext.listenerDescription);
            }

            final ModifiedColumnSet rightKeyColumns =
                    rightTable.newModifiedColumnSet(MatchPair.getRightColumns(columnsToMatch));
            final ModifiedColumnSet leftKeyColumns =
                    leftTable.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToMatch));

            if (!rightTable.isRefreshing()) {
                // TODO: use grouping
                if (!leftTable.isRefreshing()) {
                    final StaticChunkedCrossJoinStateManager jsm = new StaticChunkedCrossJoinStateManager(
                            bucketingContext.leftSources, control.initialBuildSize(), control, leftTable);
                    jsm.setMaximumLoadFactor(control.getMaximumLoadFactor());
                    jsm.setTargetLoadFactor(control.getTargetLoadFactor());

                    // We can only build from right, because the left hand side does not permit us to nicely rehash as
                    // we only have the row redirection when building left and no way to reverse the lookup.
                    final TrackingWritableRowSet resultRowSet = jsm.buildFromRight(leftTable,
                            bucketingContext.leftSources, rightTable, bucketingContext.rightSources)
                            .toTracking();

                    return makeResult(leftTable, rightTable, columnsToAdd, jsm, resultRowSet,
                            cs -> new CrossJoinRightColumnSource<>(jsm, cs, rightTable.isRefreshing()));
                }

                final LeftOnlyIncrementalChunkedCrossJoinStateManager jsm =
                        new LeftOnlyIncrementalChunkedCrossJoinStateManager(
                                bucketingContext.leftSources, control.initialBuildSize(), leftTable,
                                numRightBitsToReserve);
                jsm.setMaximumLoadFactor(control.getMaximumLoadFactor());
                jsm.setTargetLoadFactor(control.getTargetLoadFactor());

                final TrackingWritableRowSet resultRowSet =
                        jsm.buildLeftTicking(leftTable, rightTable, bucketingContext.rightSources).toTracking();
                final QueryTable resultTable = makeResult(leftTable, rightTable, columnsToAdd, jsm, resultRowSet,
                        cs -> new CrossJoinRightColumnSource<>(jsm, cs, rightTable.isRefreshing()));

                jsm.startTrackingPrevValues();
                final ModifiedColumnSet.Transformer leftTransformer = leftTable.newModifiedColumnSetTransformer(
                        resultTable,
                        leftTable.getColumnSourceMap().keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));

                leftTable.listenForUpdates(new BaseTable.ListenerImpl(bucketingContext.listenerDescription,
                        leftTable, resultTable) {
                    @Override
                    public void onUpdate(final TableUpdate upstream) {
                        jsm.validateKeySpaceSize();

                        final TableUpdateImpl downstream = new TableUpdateImpl();
                        downstream.added = RowSetFactory.empty();

                        final RowSetBuilderRandom rmBuilder = RowSetFactory.builderRandom();
                        jsm.removeLeft(upstream.removed(), (stateSlot, leftKey) -> {
                            final RowSet rightRowSet = jsm.getRightRowSet(stateSlot);
                            if (rightRowSet.isNonempty()) {
                                final long prevLeftOffset = leftKey << jsm.getPrevNumShiftBits();
                                final long lastRightIndex = prevLeftOffset + rightRowSet.size() - 1;
                                rmBuilder.addRange(prevLeftOffset, lastRightIndex);
                            }
                        });
                        downstream.removed = rmBuilder.build();
                        resultRowSet.remove(downstream.removed());

                        try (final WritableRowSet prevLeftRowSet = leftTable.getRowSet().copyPrev()) {
                            prevLeftRowSet.remove(upstream.removed());
                            jsm.applyLeftShift(prevLeftRowSet, upstream.shifted());
                            downstream.shifted = expandLeftOnlyShift(upstream.shifted(), jsm);
                            downstream.shifted().apply(resultRowSet);
                        }

                        if (upstream.modifiedColumnSet().containsAny(leftKeyColumns)) {
                            // the jsm helper sets downstream.modified and appends to
                            // downstream.added/downstream.removed
                            jsm.processLeftModifies(upstream, downstream, resultRowSet);
                            if (downstream.modified().isEmpty()) {
                                downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                            } else {
                                downstream.modifiedColumnSet = resultTable.modifiedColumnSet;
                                leftTransformer.transform(upstream.modifiedColumnSet(), resultTable.modifiedColumnSet);
                            }
                        } else if (upstream.modified().isNonempty()) {
                            final RowSetBuilderSequential modBuilder = RowSetFactory.builderSequential();
                            upstream.modified().forAllRowKeys(ll -> {
                                final RowSet rightRowSet = jsm.getRightRowSetFromLeftIndex(ll);
                                if (rightRowSet.isNonempty()) {
                                    final long currResultOffset = ll << jsm.getNumShiftBits();
                                    modBuilder.appendRange(currResultOffset, currResultOffset + rightRowSet.size() - 1);
                                }
                            });
                            downstream.modified = modBuilder.build();
                            downstream.modifiedColumnSet = resultTable.modifiedColumnSet;
                            leftTransformer.transform(upstream.modifiedColumnSet(), resultTable.modifiedColumnSet);
                        } else {
                            downstream.modified = RowSetFactory.empty();
                            downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                        }

                        final RowSetBuilderRandom addBuilder = RowSetFactory.builderRandom();
                        jsm.addLeft(upstream.added(), (stateSlot, leftKey) -> {
                            final RowSet rightRowSet = jsm.getRightRowSet(stateSlot);
                            if (rightRowSet.isNonempty()) {
                                final long regionStart = leftKey << jsm.getNumShiftBits();
                                addBuilder.addRange(regionStart, regionStart + rightRowSet.size() - 1);
                            }
                        });
                        try (final RowSet added = addBuilder.build()) {
                            downstream.added().writableCast().insert(added);
                            resultRowSet.insert(added);
                        }

                        resultTable.notifyListeners(downstream);
                    }
                });
                return resultTable;
            }

            final RightIncrementalChunkedCrossJoinStateManager jsm = new RightIncrementalChunkedCrossJoinStateManager(
                    bucketingContext.leftSources, control.initialBuildSize(), bucketingContext.rightSources, leftTable,
                    numRightBitsToReserve);
            jsm.setMaximumLoadFactor(control.getMaximumLoadFactor());
            jsm.setTargetLoadFactor(control.getTargetLoadFactor());

            final TrackingWritableRowSet resultRowSet = jsm.build(leftTable, rightTable).toTracking();

            final QueryTable resultTable = makeResult(leftTable, rightTable, columnsToAdd, jsm, resultRowSet,
                    cs -> new CrossJoinRightColumnSource<>(jsm, cs, rightTable.isRefreshing()));

            final ModifiedColumnSet.Transformer rightTransformer =
                    rightTable.newModifiedColumnSetTransformer(resultTable, columnsToAdd);

            if (leftTable.isRefreshing()) {
                // LeftIndexToSlot needs prev value tracking
                jsm.startTrackingPrevValues();

                final ModifiedColumnSet.Transformer leftTransformer = leftTable.newModifiedColumnSetTransformer(
                        resultTable,
                        leftTable.getColumnSourceMap().keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));

                final JoinListenerRecorder leftRecorder =
                        new JoinListenerRecorder(true, bucketingContext.listenerDescription, leftTable, resultTable);
                final JoinListenerRecorder rightRecorder =
                        new JoinListenerRecorder(false, bucketingContext.listenerDescription, rightTable, resultTable);

                // The approach for both-sides-ticking is to:
                // - Aggregate all right side changes, queued to apply at the right time while processing left update.
                // - Handle left removes.
                // - Handle right removes (including right modified removes).
                // - Handle left shifts
                // - Handle left modifies.
                // - Handle right modifies and adds (including right modified adds and all downstream shift data).
                // - Handle left adds.
                // - Generate downstream MCS.
                // - Propagate and Profit.
                final MergedListener mergedListener = new MergedListener(Arrays.asList(leftRecorder, rightRecorder),
                        Collections.emptyList(), bucketingContext.listenerDescription, resultTable) {
                    private final CrossJoinModifiedSlotTracker tracker = new CrossJoinModifiedSlotTracker(jsm);

                    @Override
                    protected void process() {
                        final TableUpdate upstreamLeft = leftRecorder.getUpdate();
                        final TableUpdate upstreamRight = rightRecorder.getUpdate();
                        final boolean leftChanged = upstreamLeft != null;
                        final boolean rightChanged = upstreamRight != null;

                        final TableUpdateImpl downstream = new TableUpdateImpl();

                        // If there are any right changes, let's probe and aggregate them now.
                        if (rightChanged) {
                            tracker.rightShifted = upstreamRight.shifted();

                            if (upstreamRight.removed().isNonempty()) {
                                jsm.rightRemove(upstreamRight.removed(), tracker);
                            }
                            if (upstreamRight.shifted().nonempty()) {
                                try (final RowSet prevIndex = rightTable.getRowSet().copyPrev()) {
                                    jsm.rightShift(prevIndex, upstreamRight.shifted(), tracker);
                                }
                            }
                            if (upstreamRight.added().isNonempty()) {
                                jsm.rightAdd(upstreamRight.added(), tracker);
                            }
                            if (upstreamRight.modified().isNonempty()) {
                                jsm.rightModified(upstreamRight,
                                        upstreamRight.modifiedColumnSet().containsAny(rightKeyColumns), tracker);
                            }

                            // space needed for right RowSet might have changed, let's verify we have enough keyspace
                            jsm.validateKeySpaceSize();

                            // We must finalize all known slots, so that left accumulation does not mix with right
                            // accumulation.
                            if (upstreamRight.shifted().nonempty()) {
                                try (final RowSet prevIndex = rightTable.getRowSet().copyPrev()) {
                                    jsm.shiftRightIndexToSlot(prevIndex, upstreamRight.shifted());
                                }
                            }
                            tracker.finalizeRightProcessing();
                        }

                        final int prevRightBits = jsm.getPrevNumShiftBits();
                        final int currRightBits = jsm.getNumShiftBits();
                        final boolean allRowsShift = prevRightBits != currRightBits;

                        final boolean leftModifiedMightReslot =
                                leftChanged && upstreamLeft.modifiedColumnSet().containsAny(leftKeyColumns);

                        // Let us gather all removes from the left. This includes aggregating the results of left
                        // modified.
                        if (leftChanged) {
                            if (upstreamLeft.removed().isNonempty()) {
                                jsm.leftRemoved(upstreamLeft.removed(), tracker);
                            } else {
                                tracker.leftRemoved = RowSetFactory.empty();
                            }

                            if (upstreamLeft.modified().isNonempty()) {
                                // translates the left modified as rms/mods/adds and accumulates into
                                // tracker.{leftRemoved,leftModified,leftAdded}
                                jsm.leftModified(upstreamLeft, leftModifiedMightReslot, tracker);
                            } else {
                                tracker.leftModified = RowSetFactory.empty();
                            }

                            downstream.removed = tracker.leftRemoved;
                            downstream.modified = tracker.leftModified;
                            resultRowSet.remove(downstream.removed());
                        } else {
                            downstream.removed = RowSetFactory.empty();
                            downstream.modified = RowSetFactory.empty();
                        }

                        if (rightChanged) {
                            // With left removes (and modified-removes) applied (yet adds and modified-adds pending),
                            // we can now easily calculate which rows are removed due to right removes.

                            try (final WritableRowSet leftIndexToVisitForRightRm = RowSetFactory.empty()) {
                                tracker.forAllModifiedSlots(slotState -> {
                                    if (slotState.leftRowSet.size() > 0 && slotState.rightRemoved.isNonempty()) {
                                        leftIndexToVisitForRightRm.insert(slotState.leftRowSet);
                                    }
                                });

                                try (final WritableRowSet toRemove = RowSetFactory.empty()) {
                                    // This could use a sequential builder, however, since we are always appending
                                    // non-overlapping containers, inserting into a RowSet is actually rather
                                    // efficient.
                                    leftIndexToVisitForRightRm.forAllRowKeys(ii -> {
                                        final long prevOffset = ii << prevRightBits;
                                        final CrossJoinModifiedSlotTracker.SlotState state = tracker
                                                .getFinalSlotState(jsm.getTrackerCookie(jsm.getSlotFromLeftIndex(ii)));
                                        toRemove.insertWithShift(prevOffset, state.rightRemoved);
                                    });
                                    downstream.removed().writableCast().insert(toRemove);
                                }
                            }
                        }

                        // apply left shifts to tracker (so our mods/adds are in post-shift space)
                        if (leftChanged && upstreamLeft.shifted().nonempty()) {
                            tracker.leftShifted = upstreamLeft.shifted();
                            try (final WritableRowSet prevLeftMinusRemovals = leftTable.getRowSet().copyPrev()) {
                                prevLeftMinusRemovals.remove(upstreamLeft.removed());
                                jsm.leftShift(prevLeftMinusRemovals, upstreamLeft.shifted(), tracker);
                            }
                        }

                        // note rows to shift might have no shifts but still need result RowSet updated
                        final RowSet rowsToShift;
                        final boolean mustCloseRowsToShift;

                        if (rightChanged) {
                            // process right mods / adds (in post-shift space)
                            final RowSetBuilderRandom addsToVisit = RowSetFactory.builderRandom();
                            final RowSetBuilderRandom modsToVisit = RowSetFactory.builderRandom();
                            tracker.forAllModifiedSlots(slotState -> {
                                if (slotState.leftRowSet.size() == 0) {
                                    return;
                                }
                                if (slotState.rightAdded.isNonempty()) {
                                    addsToVisit.addRowSet(slotState.leftRowSet);
                                }
                                if (slotState.rightModified.isNonempty()) {
                                    modsToVisit.addRowSet(slotState.leftRowSet);
                                }
                            });

                            try (final RowSet leftIndexesToVisitForAdds = addsToVisit.build();
                                    final RowSet leftIndexesToVisitForMods = modsToVisit.build();
                                    final WritableRowSet modified = RowSetFactory.empty()) {
                                downstream.added = RowSetFactory.empty();

                                leftIndexesToVisitForAdds.forAllRowKeys(ii -> {
                                    final long currOffset = ii << currRightBits;
                                    final CrossJoinModifiedSlotTracker.SlotState state = tracker
                                            .getFinalSlotState(jsm.getTrackerCookie(jsm.getSlotFromLeftIndex(ii)));
                                    downstream.added().writableCast().insertWithShift(currOffset, state.rightAdded);
                                });

                                leftIndexesToVisitForMods.forAllRowKeys(ii -> {
                                    final long currOffset = ii << currRightBits;
                                    final CrossJoinModifiedSlotTracker.SlotState state = tracker
                                            .getFinalSlotState(jsm.getTrackerCookie(jsm.getSlotFromLeftIndex(ii)));
                                    modified.insertWithShift(currOffset, state.rightModified);
                                });
                                downstream.modified().writableCast().insert(modified);

                                mustCloseRowsToShift = leftChanged || !allRowsShift;
                                if (allRowsShift) {
                                    rowsToShift = leftChanged ? leftTable.getRowSet().minus(upstreamLeft.added())
                                            : leftTable.getRowSet();
                                } else {
                                    rowsToShift = leftIndexesToVisitForAdds.copy();
                                }
                            }

                            if (!allRowsShift) {
                                // removals might generate shifts, so let's add those to our RowSet
                                final RowSetBuilderRandom rmsToVisit = RowSetFactory.builderRandom();
                                tracker.forAllModifiedSlots(slotState -> {
                                    if (slotState.leftRowSet.size() > 0 && slotState.rightRemoved.isNonempty()) {
                                        rmsToVisit.addRowSet(slotState.leftRowSet);
                                    }
                                });
                                try (final RowSet leftIndexesToVisitForRm = rmsToVisit.build()) {
                                    rowsToShift.writableCast().insert(leftIndexesToVisitForRm);
                                }
                            }
                        } else {
                            mustCloseRowsToShift = false;
                            rowsToShift = RowSetFactory.empty();
                        }

                        // Generate shift data; build up result RowSet changes for all but added left
                        final long prevCardinality = 1L << prevRightBits;
                        final long currCardinality = 1L << currRightBits;
                        final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();
                        final RowSetBuilderSequential toRemoveFromResultIndex = RowSetFactory.builderSequential();
                        final RowSetBuilderSequential toInsertIntoResultIndex = RowSetFactory.builderSequential();

                        if (rowsToShift.isNonempty() && leftChanged && upstreamLeft.shifted().nonempty()) {
                            final MutableBoolean finishShifting = new MutableBoolean();
                            final MutableLong watermark = new MutableLong(0);
                            final MutableInt currLeftShiftIdx = new MutableInt(0);

                            try (final RowSequence.Iterator rsIt =
                                    allRowsShift ? null : resultRowSet.getRowSequenceIterator();
                                    final WritableRowSet unshiftedRowsToShift = rowsToShift.copy()) {
                                upstreamLeft.shifted().unapply(unshiftedRowsToShift);
                                final RowSet.SearchIterator prevIter = unshiftedRowsToShift.searchIterator();

                                final LongConsumer processLeftShiftsUntil = (ii) -> {
                                    // note: if all rows shift, then each row shifts by a different amount and
                                    // rowsToShift is inclusive
                                    if (!finishShifting.booleanValue() && watermark.longValue() >= ii || allRowsShift) {
                                        return;
                                    }

                                    for (; currLeftShiftIdx.intValue() < upstreamLeft.shifted().size(); currLeftShiftIdx
                                            .increment()) {
                                        final int shiftIdx = currLeftShiftIdx.intValue();
                                        final long beginRange =
                                                upstreamLeft.shifted().getBeginRange(shiftIdx) << prevRightBits;
                                        final long endRange =
                                                ((upstreamLeft.shifted().getEndRange(shiftIdx) + 1) << prevRightBits)
                                                        - 1;
                                        final long shiftDelta =
                                                upstreamLeft.shifted().getShiftDelta(shiftIdx) << currRightBits;

                                        if (endRange < watermark.longValue()) {
                                            continue;
                                        }
                                        if (!finishShifting.booleanValue() && beginRange >= ii) {
                                            break;
                                        }

                                        final long maxTouched = Math.min(ii - 1, endRange);
                                        final long minTouched = Math.max(watermark.longValue(), beginRange);
                                        if (!rsIt.advance(minTouched)) {
                                            break;
                                        }

                                        shiftBuilder.shiftRange(minTouched, maxTouched, shiftDelta);
                                        rsIt.getNextRowSequenceThrough(maxTouched).forAllRowKeyRanges((s, e) -> {
                                            toRemoveFromResultIndex.appendRange(s, e);
                                            toInsertIntoResultIndex.appendRange(s + shiftDelta, e + shiftDelta);
                                        });
                                        watermark.setValue(maxTouched + 1);

                                        if (!finishShifting.booleanValue() && maxTouched != endRange) {
                                            break;
                                        }
                                    }
                                };

                                rowsToShift.forAllRowKeys(ii -> {
                                    final long pi = prevIter.nextLong();

                                    final long prevOffset = pi << prevRightBits;
                                    final long currOffset = ii << currRightBits;
                                    final long slotFromLeftIndex = jsm.getSlotFromLeftIndex(ii);

                                    processLeftShiftsUntil.accept(prevOffset);

                                    if (slotFromLeftIndex == RightIncrementalChunkedCrossJoinStateManager.LEFT_MAPPING_MISSING) {
                                        // Since left rows that change key-column-groups are currently removed from all
                                        // JSM data structures,
                                        // they won't have a properly mapped slot. They will be added to their new slot
                                        // after we
                                        // generate-downstream shifts. The result RowSet is also updated for these rows
                                        // in
                                        // the left-rm/left-add code paths. This code path should only be hit when
                                        // prevRightBits != currRightBits.
                                        return;
                                    }
                                    final CrossJoinModifiedSlotTracker.SlotState slotState =
                                            tracker.getFinalSlotState(jsm.getTrackerCookie(slotFromLeftIndex));

                                    if (prevRightBits != currRightBits) {
                                        final RowSet rightRowSet = jsm.getRightRowSet(slotFromLeftIndex);
                                        if (rightRowSet.isNonempty()) {
                                            toInsertIntoResultIndex.appendRange(currOffset,
                                                    currOffset + rightRowSet.size() - 1);
                                        }
                                    } else if (slotState != null) {
                                        final long prevSize = slotState.rightRowSet.sizePrev();
                                        final long currSize = slotState.rightRowSet.size();
                                        // note prevCardinality == currCardinality if prevRightBits == currRightBits
                                        if (prevOffset != currOffset) {
                                            // might be changing to an empty group
                                            if (currSize > 0) {
                                                toInsertIntoResultIndex.appendRange(currOffset,
                                                        currOffset + currSize - 1);
                                            }
                                            // might have changed from an empty group
                                            if (prevSize > 0) {
                                                toRemoveFromResultIndex.appendRange(prevOffset,
                                                        prevOffset + currCardinality - 1);
                                            }
                                        } else if (prevSize < currSize) {
                                            toInsertIntoResultIndex.appendRange(currOffset + prevSize,
                                                    currOffset + currSize - 1);
                                        } else if (currSize < prevSize && prevSize > 0) {
                                            toRemoveFromResultIndex.appendRange(prevOffset + currSize,
                                                    prevOffset + currCardinality - 1);
                                        }
                                    }

                                    // propagate inner shifts
                                    if (slotState != null && slotState.innerShifted.nonempty()) {
                                        shiftBuilder.appendShiftData(slotState.innerShifted, prevOffset,
                                                prevCardinality, currOffset, currCardinality);
                                    } else if (prevOffset != currOffset) {
                                        shiftBuilder.shiftRange(prevOffset, prevOffset + prevCardinality - 1,
                                                currOffset - prevOffset);
                                    }
                                    watermark.setValue((pi + 1) << prevRightBits);
                                });
                                // finish processing all shifts
                                finishShifting.setTrue();
                                processLeftShiftsUntil.accept(Long.MAX_VALUE);
                            }
                        } else if (rowsToShift.isNonempty()) {
                            // note: no left shifts in this branch
                            rowsToShift.forAllRowKeys(ii -> {
                                final long prevOffset = ii << prevRightBits;
                                final long currOffset = ii << currRightBits;
                                final long slotFromLeftIndex = jsm.getSlotFromLeftIndex(ii);

                                if (slotFromLeftIndex == RightIncrementalChunkedCrossJoinStateManager.LEFT_MAPPING_MISSING) {
                                    // Since left rows that change key-column-groups are currently removed from all JSM
                                    // data structures,
                                    // they won't have a properly mapped slot. They will be added to their new slot
                                    // after we
                                    // generate-downstream shifts. The result RowSet is also updated for these rows in
                                    // the left-rm/left-add code paths. This code path should only be hit when
                                    // prevRightBits != currRightBits.
                                    return;
                                }

                                final CrossJoinModifiedSlotTracker.SlotState slotState =
                                        tracker.getFinalSlotState(jsm.getTrackerCookie(slotFromLeftIndex));

                                // calculate modifications to result RowSet
                                if (prevRightBits != currRightBits) {
                                    final RowSet rightRowSet = jsm.getRightRowSet(slotFromLeftIndex);
                                    if (rightRowSet.isNonempty()) {
                                        toInsertIntoResultIndex.appendRange(currOffset,
                                                currOffset + rightRowSet.size() - 1);
                                    }
                                } else if (slotState != null) {
                                    final long prevSize = slotState.rightRowSet.sizePrev();
                                    final long currSize = slotState.rightRowSet.size();

                                    // note: prevOffset == currOffset (because left did not shift and right bits are
                                    // unchanged)
                                    if (prevSize < currSize) {
                                        toInsertIntoResultIndex.appendRange(currOffset + prevSize,
                                                currOffset + currSize - 1);
                                    } else if (currSize < prevSize && prevSize > 0) {
                                        // note prevCardinality == currCardinality if prevRightBits == currRightBits
                                        toRemoveFromResultIndex.appendRange(prevOffset + currSize,
                                                prevOffset + currCardinality - 1);
                                    }
                                }

                                // propagate inner shifts
                                if (slotState != null && slotState.innerShifted.nonempty()) {
                                    shiftBuilder.appendShiftData(slotState.innerShifted, prevOffset, prevCardinality,
                                            currOffset, currCardinality);
                                } else if (prevOffset != currOffset) {
                                    shiftBuilder.shiftRange(prevOffset, prevOffset + prevCardinality - 1,
                                            currOffset - prevOffset);
                                }
                            });
                        } else if (leftChanged && upstreamLeft.shifted().nonempty()) {
                            // upstream-left-shift our result RowSet, and build downstream shifts
                            try (final RowSequence.Iterator rsIt = resultRowSet.getRowSequenceIterator()) {
                                for (int idx = 0; idx < upstreamLeft.shifted().size(); ++idx) {
                                    final long beginRange = upstreamLeft.shifted().getBeginRange(idx) << prevRightBits;
                                    final long endRange =
                                            ((upstreamLeft.shifted().getEndRange(idx) + 1) << prevRightBits) - 1;
                                    final long shiftDelta = upstreamLeft.shifted().getShiftDelta(idx) << prevRightBits;

                                    if (!rsIt.advance(beginRange)) {
                                        break;
                                    }

                                    shiftBuilder.shiftRange(beginRange, endRange, shiftDelta);
                                    rsIt.getNextRowSequenceThrough(endRange).forAllRowKeyRanges((s, e) -> {
                                        toRemoveFromResultIndex.appendRange(s, e);
                                        toInsertIntoResultIndex.appendRange(s + shiftDelta, e + shiftDelta);
                                    });
                                }
                            }
                        }

                        downstream.shifted = shiftBuilder.build();

                        try (final RowSet toRemove = toRemoveFromResultIndex.build();
                                final RowSet toInsert = toInsertIntoResultIndex.build()) {
                            if (prevRightBits != currRightBits) {
                                // every row shifted
                                resultRowSet.clear();
                            } else {
                                resultRowSet.remove(toRemove);
                            }
                            resultRowSet.insert(toInsert);
                        }

                        if (mustCloseRowsToShift) {
                            rowsToShift.close();
                        }

                        // propagate left adds / modded-adds to jsm
                        final boolean insertLeftAdded;
                        if (leftChanged && upstreamLeft.added().isNonempty()) {
                            insertLeftAdded = true;
                            jsm.leftAdded(upstreamLeft.added(), tracker);
                        } else if (leftModifiedMightReslot) {
                            // process any missing modified-adds
                            insertLeftAdded = true;
                            tracker.flushLeftAdds();
                        } else {
                            insertLeftAdded = false;
                        }

                        if (insertLeftAdded) {
                            resultRowSet.insert(tracker.leftAdded);
                            if (downstream.added() != null) {
                                downstream.added().writableCast().insert(tracker.leftAdded);
                                tracker.leftAdded.close();
                            } else {
                                downstream.added = tracker.leftAdded;
                            }
                        }

                        if (downstream.added() == null) {
                            downstream.added = RowSetFactory.empty();
                        }

                        if (leftChanged && tracker.leftModified.isNonempty()) {
                            // We simply exploded the left rows to include all existing right rows; must remove the
                            // recently added.
                            downstream.modified().writableCast().remove(downstream.added());
                        }
                        if (downstream.modified().isEmpty()) {
                            downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                        } else {
                            downstream.modifiedColumnSet = resultTable.modifiedColumnSet;
                            downstream.modifiedColumnSet().clear();
                            if (leftChanged && tracker.hasLeftModifies) {
                                leftTransformer.transform(upstreamLeft.modifiedColumnSet(),
                                        downstream.modifiedColumnSet());
                            }
                            if (rightChanged && tracker.hasRightModifies) {
                                rightTransformer.transform(upstreamRight.modifiedColumnSet(),
                                        downstream.modifiedColumnSet());
                            }
                        }

                        resultTable.notifyListeners(downstream);

                        tracker.clear();
                    }
                };

                leftRecorder.setMergedListener(mergedListener);
                rightRecorder.setMergedListener(mergedListener);
                leftTable.listenForUpdates(leftRecorder);
                rightTable.listenForUpdates(rightRecorder);
                resultTable.addParentReference(mergedListener);
            } else {
                rightTable.listenForUpdates(new BaseTable.ListenerImpl(bucketingContext.listenerDescription,
                        rightTable, resultTable) {
                    private final CrossJoinModifiedSlotTracker tracker = new CrossJoinModifiedSlotTracker(jsm);

                    @Override
                    public void onUpdate(TableUpdate upstream) {
                        tracker.rightShifted = upstream.shifted();

                        final TableUpdateImpl downstream = new TableUpdateImpl();
                        final RowSetShiftData.Builder shifted = new RowSetShiftData.Builder();

                        if (upstream.removed().isNonempty()) {
                            jsm.rightRemove(upstream.removed(), tracker);
                        }
                        if (upstream.shifted().nonempty()) {
                            try (final RowSet prevRowSet = rightTable.getRowSet().copyPrev()) {
                                jsm.rightShift(prevRowSet, upstream.shifted(), tracker);
                            }
                        }
                        if (upstream.added().isNonempty()) {
                            jsm.rightAdd(upstream.added(), tracker);
                        }
                        if (upstream.modified().isNonempty()) {
                            jsm.rightModified(upstream, upstream.modifiedColumnSet().containsAny(rightKeyColumns),
                                    tracker);
                        }

                        // right changes are flushed now
                        if (upstream.shifted().nonempty()) {
                            try (final RowSet prevRowSet = rightTable.getRowSet().copyPrev()) {
                                jsm.shiftRightIndexToSlot(prevRowSet, upstream.shifted());
                            }
                        }
                        tracker.finalizeRightProcessing();

                        // space needed for right RowSet might have changed, let's verify we have enough keyspace
                        jsm.validateKeySpaceSize();

                        final int prevRightBits = jsm.getPrevNumShiftBits();
                        final int currRightBits = jsm.getNumShiftBits();

                        final RowSet leftChanged;
                        final boolean numRightBitsChanged = currRightBits != prevRightBits;
                        if (numRightBitsChanged) {
                            // Must touch all left keys.
                            leftChanged = leftTable.getRowSet();
                            // Must rebuild entire result RowSet.
                            resultRowSet.clear();
                        } else {
                            final RowSetBuilderRandom leftChangedBuilder = RowSetFactory.builderRandom();

                            tracker.forAllModifiedSlots(slotState -> {
                                // filter out slots that only have right shifts (these don't have downstream effects)
                                if (slotState.rightChanged) {
                                    leftChangedBuilder.addRowSet(jsm.getLeftRowSet(slotState.slotLocation));
                                }
                            });

                            leftChanged = leftChangedBuilder.build();
                        }

                        final long prevCardinality = 1L << prevRightBits;
                        final RowSetBuilderSequential added = RowSetFactory.builderSequential();
                        final RowSetBuilderSequential removed = RowSetFactory.builderSequential();
                        final RowSetBuilderSequential modified = RowSetFactory.builderSequential();

                        final RowSetBuilderSequential removeFromResultIndex =
                                numRightBitsChanged ? null : RowSetFactory.builderSequential();
                        final RowSetBuilderSequential addToResultIndex = RowSetFactory.builderSequential();

                        // Accumulate all changes by left row.
                        leftChanged.forAllRowKeys(ii -> {
                            final long prevOffset = ii << prevRightBits;
                            final long currOffset = ii << currRightBits;

                            final long slot = jsm.getSlotFromLeftIndex(ii);
                            final CrossJoinModifiedSlotTracker.SlotState slotState =
                                    tracker.getFinalSlotState(jsm.getTrackerCookie(slot));
                            final TrackingRowSet rightRowSet =
                                    slotState == null ? jsm.getRightRowSet(slot) : slotState.rightRowSet;

                            if (numRightBitsChanged) {
                                if (rightRowSet.isNonempty()) {
                                    addToResultIndex.appendRange(currOffset, currOffset + rightRowSet.size() - 1);
                                }
                            } else if (slotState != null) {
                                final long prevSize = slotState.rightRowSet.sizePrev();
                                final long currSize = slotState.rightRowSet.size();

                                if (prevSize < currSize) {
                                    addToResultIndex.appendRange(currOffset + prevSize, currOffset + currSize - 1);
                                } else if (currSize < prevSize && prevSize > 0) {
                                    // note prevCardinality == currCardinality if prevRightBits == currRightBits
                                    removeFromResultIndex.appendRange(prevOffset + currSize,
                                            prevOffset + prevCardinality - 1);
                                }
                            }

                            if (slotState == null || !slotState.rightChanged) {
                                if (prevOffset != currOffset) {
                                    shifted.shiftRange(prevOffset, prevOffset + rightRowSet.sizePrev() - 1,
                                            currOffset - prevOffset);
                                }
                                return;
                            }

                            final long preShiftShiftAmt = prevOffset - (slotState.lastIndex << prevRightBits);
                            final long postShiftShiftAmt = currOffset - (slotState.lastIndex << currRightBits);
                            if (slotState.rightAdded.isNonempty()) {
                                slotState.rightAdded.shiftInPlace(postShiftShiftAmt);
                                added.appendRowSequence(slotState.rightAdded);
                            }
                            if (slotState.rightRemoved.isNonempty()) {
                                slotState.rightRemoved.shiftInPlace(preShiftShiftAmt);
                                removed.appendRowSequence(slotState.rightRemoved);
                            }
                            if (slotState.rightModified.isNonempty()) {
                                slotState.rightModified.shiftInPlace(postShiftShiftAmt);
                                modified.appendRowSequence(slotState.rightModified);
                            }
                            slotState.lastIndex = ii;

                            shifted.appendShiftData(slotState.innerShifted, prevOffset, rightRowSet.sizePrev(),
                                    currOffset, rightRowSet.size());
                        });

                        downstream.added = added.build();
                        downstream.modified = modified.build();
                        downstream.removed = removed.build();
                        downstream.shifted = shifted.build();

                        if (!numRightBitsChanged) {
                            leftChanged.close();
                            try (final RowSet remove = removeFromResultIndex.build()) {
                                resultRowSet.remove(remove);
                            }
                        }
                        try (final RowSet add = addToResultIndex.build()) {
                            resultRowSet.insert(add);
                        }

                        if (tracker.clear()) {
                            jsm.clearCookies();
                        }

                        if (downstream.modified().isEmpty()) {
                            downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                        } else {
                            downstream.modifiedColumnSet = resultTable.modifiedColumnSet;
                            rightTransformer.clearAndTransform(upstream.modifiedColumnSet(),
                                    downstream.modifiedColumnSet());
                        }

                        resultTable.notifyListeners(downstream);
                    }
                });
            }

            return resultTable;
        }
    }

    private static void validateZeroKeyIndexSpace(final QueryTable leftTable, final QueryTable rightTable,
            int numRightBitsReserved) {
        final long leftLastKey = leftTable.getRowSet().lastRowKey();
        final long rightLastKey = rightTable.getRowSet().lastRowKey();
        final int minLeftBits = CrossJoinShiftState.getMinBits(leftLastKey);
        final int minRightBits = CrossJoinShiftState.getMinBits(rightLastKey);
        numRightBitsReserved = Math.max(numRightBitsReserved, minRightBits);
        if (minLeftBits + numRightBitsReserved > 63) {
            throw new OutOfKeySpaceException(
                    "join with zero key columns out of rowSet space (left reqBits + right reserveBits > 63); "
                            + "(left table: {size: " + leftTable.getRowSet().size() + " maxIndex: " + leftLastKey
                            + " reqBits: " + minLeftBits + "}) X "
                            + "(right table: {size: " + rightTable.getRowSet().size() + " maxIndex: " + rightLastKey
                            + " reqBits: " + minRightBits + " reservedBits: " + numRightBitsReserved + "})"
                            + " exceeds Long.MAX_VALUE. Consider flattening either table or reserving fewer right bits if possible.");
        }
    }

    @NotNull
    private static Table zeroKeyColumnsJoin(QueryTable leftTable, QueryTable rightTable, MatchPair[] columnsToAdd,
            int numRightBitsToReserve, String listenerDescription) {
        // we are a single value join, we do not need to do any hash-related work
        validateZeroKeyIndexSpace(leftTable, rightTable, numRightBitsToReserve);
        final CrossJoinShiftState crossJoinState =
                new CrossJoinShiftState(Math.max(numRightBitsToReserve, CrossJoinShiftState.getMinBits(rightTable)));

        final TrackingWritableRowSet resultRowSet = RowSetFactory.empty().toTracking();
        final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, crossJoinState, resultRowSet,
                cs -> new BitMaskingColumnSource<>(crossJoinState, cs));
        final ModifiedColumnSet.Transformer leftTransformer =
                leftTable.newModifiedColumnSetTransformer(result, leftTable.getDefinition().getColumnNamesArray());
        final ModifiedColumnSet.Transformer rightTransformer =
                rightTable.newModifiedColumnSetTransformer(result, columnsToAdd);

        final BiConsumer<TableUpdate, TableUpdate> onUpdate = (leftUpdate, rightUpdate) -> {

            final boolean leftChanged = leftUpdate != null;
            final boolean rightChanged = rightUpdate != null;

            final int prevRightBits = crossJoinState.getNumShiftBits();
            final int currRightBits = Math.max(prevRightBits, CrossJoinShiftState.getMinBits(rightTable));
            validateZeroKeyIndexSpace(leftTable, rightTable, currRightBits);

            if (currRightBits != prevRightBits) {
                crossJoinState.setNumShiftBitsAndUpdatePrev(currRightBits);
            }

            final TableUpdateImpl downstream = new TableUpdateImpl();
            downstream.added = RowSetFactory.empty();
            downstream.removed = RowSetFactory.empty();
            downstream.modified = RowSetFactory.empty();
            downstream.modifiedColumnSet = result.modifiedColumnSet;
            downstream.modifiedColumnSet().clear();

            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();

            try (final SafeCloseableList closer = new SafeCloseableList()) {
                if (rightChanged && rightUpdate.modified().isNonempty()) {
                    rightTransformer.transform(rightUpdate.modifiedColumnSet(), result.modifiedColumnSet);
                }
                if (leftChanged && leftUpdate.modified().isNonempty()) {
                    leftTransformer.transform(leftUpdate.modifiedColumnSet(), result.modifiedColumnSet);
                }

                long currRightShift = 0; // how far currRight has been shifted
                final WritableRowSet currRight = closer.add(rightTable.getRowSet().copy());

                if (rightChanged) {
                    // Must touch every left row. (Note: this code is accessible iff right changed.)
                    final TrackingRowSet currLeft = leftTable.getRowSet();
                    final RowSet prevLeft = closer.add(currLeft.copyPrev());

                    long prevRightShift = 0; // how far prevRight has been shifted
                    final WritableRowSet prevRight = closer.add(rightTable.getRowSet().copyPrev());

                    long rmRightShift = 0; // how far rmRight has been shifted
                    final WritableRowSet rmRight = closer.add(rightUpdate.removed().copy());

                    long addRightShift = 0; // how far addRight has been shifted
                    final WritableRowSet addRight = closer.add(rightUpdate.added().copy());

                    long modRightShift = 0; // how far modRight has been shifted
                    final WritableRowSet modRight = closer.add(rightUpdate.modified().copy());

                    long existingRightShift = 0; // how far existingRight has been shifted
                    final WritableRowSet existingRight = closer.add(currRight.minus(rightUpdate.added()));

                    final boolean rightHasAdds = addRight.isNonempty();
                    final boolean rightHasRemoves = rmRight.isNonempty();
                    final boolean rightHasModifies = modRight.isNonempty();

                    // Do note that add/mod's are in post-shift keyspace.
                    final RowSet.SearchIterator leftAddIter = leftChanged ? leftUpdate.added().searchIterator() : null;
                    final RowSet.SearchIterator leftRmIter = leftChanged ? leftUpdate.removed().searchIterator() : null;
                    final RowSet.SearchIterator leftModIter =
                            leftChanged ? leftUpdate.modified().searchIterator() : null;
                    boolean moreLeftAdd = leftChanged && advanceIterator(leftAddIter);
                    boolean moreLeftRm = leftChanged && advanceIterator(leftRmIter);
                    boolean moreLeftMod = leftChanged && advanceIterator(leftModIter);

                    // Prepare left-side iterators.
                    final RowSet.SearchIterator leftPrevIter = prevLeft.searchIterator();
                    final RowSet.SearchIterator leftCurrIter = leftTable.getRowSet().searchIterator();
                    boolean moreLeftPrev = advanceIterator(leftPrevIter);
                    boolean moreLeftCurr = advanceIterator(leftCurrIter);

                    // It is more efficient to completely rebuild this RowSet, than to modify each row to right mapping.
                    resultRowSet.clear();

                    final long prevCardinality = 1L << prevRightBits;
                    final long currCardinality = 1L << currRightBits;

                    // Note: This assumes that shifts are not allowed to re-order data.
                    while (moreLeftPrev) {
                        final long currPrevIdx = leftPrevIter.currentValue();
                        final long prevResultOffset = currPrevIdx << prevRightBits;
                        moreLeftPrev = advanceIterator(leftPrevIter);

                        if (moreLeftRm && currPrevIdx == leftRmIter.currentValue()) {
                            // currPrevIdx is a left remove.
                            moreLeftRm = advanceIterator(leftRmIter);
                            prevRightShift = furtherShiftIndex(prevRight, prevRightShift, prevResultOffset);
                            downstream.removed().writableCast().insert(prevRight);
                            continue;
                        }

                        // Note: Pre-existing row was not removed, therefore there must be an entry in curr RowSet.
                        Assert.eqTrue(moreLeftCurr, "moreLeftCurr");
                        long currCurrIdx = leftCurrIter.currentValue();
                        long currResultOffset = currCurrIdx << currRightBits;
                        moreLeftCurr = advanceIterator(leftCurrIter);

                        // Insert adds until we find our currCurrIdx that matches currPrevIdx.
                        while (moreLeftAdd && currCurrIdx == leftAddIter.currentValue()) {
                            // currCurrIdx is a left add.
                            moreLeftAdd = advanceIterator(leftAddIter);
                            currRightShift = furtherShiftIndex(currRight, currRightShift, currResultOffset);
                            downstream.added().writableCast().insert(currRight);
                            resultRowSet.insert(currRight);

                            // Advance left current iterator.
                            Assert.eqTrue(moreLeftCurr, "moreLeftCurr");
                            currCurrIdx = leftCurrIter.currentValue();
                            currResultOffset = currCurrIdx << currRightBits;
                            moreLeftCurr = advanceIterator(leftCurrIter);
                        }

                        if (rightHasRemoves) {
                            rmRightShift = furtherShiftIndex(rmRight, rmRightShift, prevResultOffset);
                            downstream.removed().writableCast().insert(rmRight);
                        }

                        if (rightHasAdds) {
                            addRightShift = furtherShiftIndex(addRight, addRightShift, currResultOffset);
                            downstream.added().writableCast().insert(addRight);
                        }

                        if (moreLeftMod && currCurrIdx == leftModIter.currentValue()) {
                            // currCurrIdx is modify; paint all existing rows as modified
                            moreLeftMod = advanceIterator(leftModIter);
                            existingRightShift = furtherShiftIndex(existingRight, existingRightShift, currResultOffset);
                            downstream.modified().writableCast().insert(existingRight);
                        } else if (rightHasModifies) {
                            modRightShift = furtherShiftIndex(modRight, modRightShift, currResultOffset);
                            downstream.modified().writableCast().insert(modRight);
                        }

                        currRightShift = furtherShiftIndex(currRight, currRightShift, currResultOffset);
                        resultRowSet.insert(currRight);

                        if (rightUpdate.shifted().nonempty()) {
                            shiftBuilder.appendShiftData(rightUpdate.shifted(), prevResultOffset, prevCardinality,
                                    currResultOffset, currCardinality);
                        } else if (currResultOffset != prevResultOffset) {
                            final long shiftDelta = currResultOffset - prevResultOffset;
                            final long lastResultIdx = prevResultOffset + prevCardinality - 1;
                            shiftBuilder.shiftRange(prevResultOffset, lastResultIdx, shiftDelta);
                        }
                    }

                    // Note: Only left adds remain.
                    while (moreLeftCurr) {
                        final long currCurrIdx = leftCurrIter.currentValue();
                        moreLeftCurr = advanceIterator(leftCurrIter);

                        Assert.eqTrue(moreLeftAdd, "moreLeftAdd");
                        assert leftAddIter != null;
                        Assert.eq(currCurrIdx, "currCurrIdx", leftAddIter.currentValue(), "leftAddIter.currentValue()");
                        moreLeftAdd = advanceIterator(leftAddIter);

                        final long currResultIdx = currCurrIdx << currRightBits;
                        currRightShift = furtherShiftIndex(currRight, currRightShift, currResultIdx);
                        downstream.added().writableCast().insert(currRight);
                        resultRowSet.insert(currRight);
                    }

                    downstream.shifted = shiftBuilder.build();
                } else {
                    // Explode left updates to apply to all right rows.
                    assert leftUpdate != null;

                    RowSet.SearchIterator iter = leftUpdate.removed().searchIterator();
                    while (iter.hasNext()) {
                        final long currIdx = iter.nextLong();
                        final long currResultIdx = currIdx << currRightBits;
                        currRightShift = furtherShiftIndex(currRight, currRightShift, currResultIdx);
                        downstream.removed().writableCast().insert(currRight);
                        resultRowSet.removeRange(currResultIdx, ((currIdx + 1) << currRightBits) - 1);
                    }

                    downstream.shifted = expandLeftOnlyShift(leftUpdate.shifted(), crossJoinState);
                    downstream.shifted().apply(resultRowSet);

                    iter = leftUpdate.modified().searchIterator();
                    while (iter.hasNext()) {
                        final long currIdx = iter.nextLong();
                        final long currResultIdx = currIdx << currRightBits;
                        currRightShift = furtherShiftIndex(currRight, currRightShift, currResultIdx);
                        downstream.modified().writableCast().insert(currRight);
                    }

                    iter = leftUpdate.added().searchIterator();
                    while (iter.hasNext()) {
                        final long currIdx = iter.nextLong();
                        final long currResultIdx = currIdx << currRightBits;
                        currRightShift = furtherShiftIndex(currRight, currRightShift, currResultIdx);
                        downstream.added().writableCast().insert(currRight);
                        resultRowSet.insert(currRight);
                    }
                }
            }

            result.notifyListeners(downstream);
        };

        if (leftTable.isRefreshing() && rightTable.isRefreshing()) {
            final JoinListenerRecorder leftRecorder =
                    new JoinListenerRecorder(true, listenerDescription, leftTable, result);
            final JoinListenerRecorder rightRecorder =
                    new JoinListenerRecorder(false, listenerDescription, rightTable, result);

            final MergedListener mergedListener = new MergedListener(Arrays.asList(leftRecorder, rightRecorder),
                    Collections.emptyList(), listenerDescription, result) {
                @Override
                protected void process() {
                    onUpdate.accept(leftRecorder.getUpdate(), rightRecorder.getUpdate());
                }
            };

            leftRecorder.setMergedListener(mergedListener);
            rightRecorder.setMergedListener(mergedListener);
            leftTable.listenForUpdates(leftRecorder);
            rightTable.listenForUpdates(rightRecorder);
            result.addParentReference(mergedListener);
        } else if (leftTable.isRefreshing() && rightTable.size() > 0) {
            leftTable.listenForUpdates(new BaseTable.ListenerImpl(listenerDescription, leftTable, result) {
                @Override
                public void onUpdate(final TableUpdate upstream) {
                    onUpdate.accept(upstream, null);
                }
            });
        } else if (rightTable.isRefreshing() && leftTable.size() > 0) {
            rightTable.listenForUpdates(new BaseTable.ListenerImpl(listenerDescription, rightTable, result) {
                @Override
                public void onUpdate(final TableUpdate upstream) {
                    onUpdate.accept(null, upstream);
                }
            });
        }

        // Initialize result table.
        try (final WritableRowSet currRight = rightTable.getRowSet().copy()) {
            final MutableLong currRightShift = new MutableLong();
            leftTable.getRowSet().forAllRowKeys((currIdx) -> {
                final long currResultIdx = currIdx << crossJoinState.getNumShiftBits();
                currRightShift.setValue(furtherShiftIndex(currRight, currRightShift.longValue(), currResultIdx));
                resultRowSet.insert(currRight);
            });
        }
        resultRowSet.initializePreviousValue();
        return result;
    }

    private static boolean advanceIterator(final RowSet.SearchIterator iter) {
        if (!iter.hasNext()) {
            return false;
        }
        iter.nextLong();
        return true;
    }

    private static long furtherShiftIndex(final WritableRowSet rowSet, final long currShift, final long destShift) {
        final long toShift = destShift - currShift;
        rowSet.shiftInPlace(toShift);
        return destShift;
    }

    private static RowSetShiftData expandLeftOnlyShift(final RowSetShiftData leftShifts,
            final CrossJoinShiftState shiftState) {
        final int currRightBits = shiftState.getNumShiftBits();
        final int prevRightBits = shiftState.getPrevNumShiftBits();
        Assert.eq(currRightBits, "currRightBits", prevRightBits, "prevRightBits");

        if (leftShifts.empty()) {
            return RowSetShiftData.EMPTY;
        }

        final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();

        for (int si = 0; si < leftShifts.size(); ++si) {
            final long ss = leftShifts.getBeginRange(si);
            final long se = leftShifts.getEndRange(si);
            final long sd = leftShifts.getShiftDelta(si);
            shiftBuilder.shiftRange(ss << currRightBits, ((se + 1) << currRightBits) - 1, sd << currRightBits);
        }

        return shiftBuilder.build();
    }

    @NotNull
    private static <T extends ColumnSource<?>> QueryTable makeResult(
            @NotNull final QueryTable leftTable,
            @NotNull final Table rightTable,
            @NotNull final MatchPair[] columnsToAdd,
            @NotNull final CrossJoinShiftState joinState,
            @NotNull final TrackingRowSet resultRowSet,
            @NotNull final Function<ColumnSource<?>, T> newRightColumnSource) {
        final Map<String, ColumnSource<?>> columnSourceMap = new LinkedHashMap<>();

        for (final Map.Entry<String, ColumnSource<?>> leftColumn : leftTable.getColumnSourceMap().entrySet()) {
            final BitShiftingColumnSource<?> wrappedSource =
                    new BitShiftingColumnSource<>(joinState, leftColumn.getValue());
            columnSourceMap.put(leftColumn.getKey(), wrappedSource);
        }

        for (MatchPair mp : columnsToAdd) {
            final T wrappedSource = newRightColumnSource.apply(rightTable.getColumnSource(mp.rightColumn()));
            columnSourceMap.put(mp.leftColumn(), wrappedSource);
        }

        return new QueryTable(resultRowSet, columnSourceMap);
    }
}

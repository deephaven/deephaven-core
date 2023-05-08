/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.base.verify.Assert;
import io.deephaven.configuration.Configuration;
import io.deephaven.engine.exceptions.OutOfKeySpaceException;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.*;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.engine.table.impl.join.JoinListenerRecorder;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
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

import static io.deephaven.engine.table.MatchPair.matchString;

/**
 * Implementation for chunk-oriented joins that produce multiple RHS rows per-LHS row, including {@link Table#join}
 * (referred to as simply join or "cross join") and a left outer join. The left outer join does not currently have any
 * user visible API.
 *
 * <p>
 * When there are zero keys, the result table uses {@link BitShiftingColumnSource}s for the columns derived from the
 * left table and a {@link BitMaskingColumnSource} for the columns on the right. The rowkey space has some number of
 * bits for shifting, the low order bits are directly translated to the right table's rowset; the high order bits are
 * shifted to the right and indexed into the left table. For example if the right table has a rowset of {0, 1, 7}; then
 * 3 bits are required to address the right table and the remainder of the bits are used for the left table.
 * </p>
 *
 * <p>
 * For a bucketed cross join, the number of RHS bits is determined by the size of the largest group. The LHS sources are
 * similarly shifted using a {@link BitShiftingColumnSource}, but instead of using the rowkey space of the right hand
 * side directly each group is flattened. The RHS ues a {@link CrossJoinRightColumnSource} to handle these flat indices.
 * So in the case where we had a group containing an rowset of {0, 1, 7} it only requires 2 bits (for 3 values) not 3
 * bits (to represent the key 7). When values are added or removed from the RHS, the remaining values must appropriately
 * shift to maintain the flat space. If the largest right hand side group increases, then we must increase the number of
 * bits dedicated to the RHS and all of the groups require a shift.
 * </p>
 *
 * <p>
 * The difference between a cross join and a left outer join is that in the case there are zero matching RHS rows, the
 * cross join does not produce any output for that state. For the left outer join, a single row with null RHS values is
 * produced. When a tick causes a transition from empty to non-empty or vice-versa the matched row is added or removed
 * and the corresponding null row is removed or added in the downstream update (as opposed to being represented as a
 * modification).
 * </p>
 *
 * <p>
 * From a user-perspective, when the operation can be suitably performed using a {@link Table#naturalJoin}, that
 * operation should be preferred. The LHS columns and RowSet are passed through unchanged in a naturalJoin and the right
 * columns have a simpler redirection. The simpler naturalJoin is likely to provide better performance, though can not
 * handle results that require multiple RHS rows.
 * </p>
 */
public class CrossJoinHelper {
    // Note: This would be >= 16 to get efficient performance from WritableRowSet#insert and
    // WritableRowSet#shiftInPlace. However, it is too costly for joins of many small groups when the default is high.
    public static final int DEFAULT_NUM_RIGHT_BITS_TO_RESERVE = Configuration.getInstance()
            .getIntegerForClassWithDefault(CrossJoinHelper.class, "numRightBitsToReserve", 10);

    /**
     * Static-use only.
     */
    private CrossJoinHelper() {}

    static Table join(
            final QueryTable leftTable,
            final QueryTable rightTable,
            final MatchPair[] columnsToMatch,
            final MatchPair[] columnsToAdd,
            final int numReserveRightBits) {
        return join(leftTable, rightTable, columnsToMatch, columnsToAdd, numReserveRightBits, new JoinControl());
    }

    static Table join(
            final QueryTable leftTable,
            final QueryTable rightTable,
            final MatchPair[] columnsToMatch,
            final MatchPair[] columnsToAdd,
            final int numReserveRightBits,
            final JoinControl control) {
        final QueryTable result = internalJoin(leftTable, rightTable, columnsToMatch, columnsToAdd, numReserveRightBits,
                control, false);
        leftTable.maybeCopyColumnDescriptions(result, rightTable, columnsToMatch, columnsToAdd);
        return result;
    }

    public static Table leftOuterJoin(
            final QueryTable leftTable,
            final QueryTable rightTable,
            final MatchPair[] columnsToMatch,
            final MatchPair[] columnsToAdd,
            final int numReserveRightBits) {
        return leftOuterJoin(leftTable, rightTable, columnsToMatch, columnsToAdd, numReserveRightBits,
                new JoinControl());
    }

    static Table leftOuterJoin(
            final QueryTable leftTable,
            final QueryTable rightTable,
            final MatchPair[] columnsToMatch,
            final MatchPair[] columnsToAdd,
            final int numReserveRightBits,
            final JoinControl control) {
        return QueryPerformanceRecorder.withNugget("leftJoin(" + rightTable.getDescription() + ","
                + matchString(columnsToMatch) + "," + matchString(columnsToAdd) + ")", leftTable.size(), () -> {
                    final QueryTable result = internalJoin(leftTable, rightTable, columnsToMatch, columnsToAdd,
                            numReserveRightBits, control, true);
                    leftTable.maybeCopyColumnDescriptions(result, rightTable, columnsToMatch, columnsToAdd);
                    return result;
                });
    }

    private static QueryTable internalJoin(
            final QueryTable leftTable,
            final QueryTable rightTable,
            final MatchPair[] columnsToMatch,
            final MatchPair[] columnsToAdd,
            int numRightBitsToReserve,
            final JoinControl control,
            final boolean leftOuterJoin) {
        QueryTable.checkInitiateBinaryOperation(leftTable, rightTable);

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
                        bucketingContext.listenerDescription, leftOuterJoin);
            }

            final ModifiedColumnSet rightKeyColumns =
                    rightTable.newModifiedColumnSet(MatchPair.getRightColumns(columnsToMatch));
            final ModifiedColumnSet leftKeyColumns =
                    leftTable.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToMatch));

            if (!rightTable.isRefreshing()) {
                // TODO: use grouping
                if (!leftTable.isRefreshing()) {
                    final StaticChunkedCrossJoinStateManager jsm = new StaticChunkedCrossJoinStateManager(
                            bucketingContext.leftSources, control.initialBuildSize(), control, leftTable,
                            leftOuterJoin);
                    jsm.setMaximumLoadFactor(control.getMaximumLoadFactor());
                    jsm.setTargetLoadFactor(control.getTargetLoadFactor());

                    final WritableRowSet resultRowSet = control.buildLeft(leftTable, rightTable)
                            ? jsm.buildFromLeft(leftTable, bucketingContext.leftSources, rightTable,
                                    bucketingContext.rightSources)
                            : jsm.buildFromRight(leftTable, bucketingContext.leftSources, rightTable,
                                    bucketingContext.rightSources);

                    final StaticChunkedCrossJoinStateManager.ResultOnlyCrossJoinStateManager resultStateManager =
                            jsm.getResultOnlyStateManager();

                    return makeResult(leftTable, rightTable, columnsToAdd, resultStateManager,
                            resultRowSet.toTracking(),
                            cs -> CrossJoinRightColumnSource.maybeWrap(
                                    resultStateManager, cs, rightTable.isRefreshing()));
                }

                final LeftOnlyIncrementalChunkedCrossJoinStateManager jsm =
                        new LeftOnlyIncrementalChunkedCrossJoinStateManager(
                                bucketingContext.leftSources, control.initialBuildSize(), leftTable,
                                numRightBitsToReserve, leftOuterJoin);
                jsm.setMaximumLoadFactor(control.getMaximumLoadFactor());
                jsm.setTargetLoadFactor(control.getTargetLoadFactor());

                final TrackingWritableRowSet resultRowSet =
                        jsm.buildLeftTicking(leftTable, rightTable, bucketingContext.rightSources).toTracking();
                final QueryTable resultTable = makeResult(leftTable, rightTable, columnsToAdd, jsm, resultRowSet,
                        cs -> CrossJoinRightColumnSource.maybeWrap(jsm, cs, rightTable.isRefreshing()));

                jsm.startTrackingPrevValues();
                final ModifiedColumnSet.Transformer leftTransformer = leftTable.newModifiedColumnSetTransformer(
                        resultTable,
                        leftTable.getColumnSourceMap().keySet().toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));

                leftTable.addUpdateListener(new BaseTable.ListenerImpl(bucketingContext.listenerDescription,
                        leftTable, resultTable) {
                    @Override
                    public void onUpdate(final TableUpdate upstream) {
                        jsm.validateKeySpaceSize();

                        final TableUpdateImpl downstream = new TableUpdateImpl();
                        downstream.added = RowSetFactory.empty();

                        final RowSetBuilderRandom rmBuilder = RowSetFactory.builderRandom();
                        jsm.removeLeft(upstream.removed(), (stateSlot, leftKey) -> {
                            final long prevLeftOffset = leftKey << jsm.getPrevNumShiftBits();
                            if (stateSlot == RowSet.NULL_ROW_KEY) {
                                if (leftOuterJoin) {
                                    rmBuilder.addKey(prevLeftOffset);
                                }
                                return;
                            }
                            // the right table can never change and we always build right, so there should never be a
                            // state that exists w/o the right having a non-empty rowset
                            final long rightSize = jsm.getRightSize(stateSlot);
                            Assert.gtZero(rightSize, "rightSize");
                            final long lastRightRowKey = prevLeftOffset + rightSize - 1;
                            rmBuilder.addRange(prevLeftOffset, lastRightRowKey);
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
                                downstream.modifiedColumnSet = resultTable.getModifiedColumnSetForUpdates();
                                leftTransformer.transform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
                            }
                        } else if (upstream.modified().isNonempty()) {
                            final RowSetBuilderSequential modBuilder = RowSetFactory.builderSequential();
                            upstream.modified().forAllRowKeys(ll -> {
                                final RowSet rightRowSet = jsm.getRightRowSetFromLeftRow(ll);
                                final long currResultOffset = ll << jsm.getNumShiftBits();
                                if (rightRowSet.isNonempty()) {
                                    modBuilder.appendRange(currResultOffset, currResultOffset + rightRowSet.size() - 1);
                                } else if (leftOuterJoin) {
                                    modBuilder.appendKey(currResultOffset);
                                }
                            });
                            downstream.modified = modBuilder.build();
                            downstream.modifiedColumnSet = resultTable.getModifiedColumnSetForUpdates();
                            leftTransformer.transform(upstream.modifiedColumnSet(), downstream.modifiedColumnSet);
                        } else {
                            downstream.modified = RowSetFactory.empty();
                            downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                        }

                        final RowSetBuilderRandom addBuilder = RowSetFactory.builderRandom();
                        jsm.addLeft(upstream.added(), (stateSlot, leftKey, rightRowSet) -> {
                            final long regionStart = leftKey << jsm.getNumShiftBits();
                            if (rightRowSet.isNonempty()) {
                                addBuilder.addRange(regionStart, regionStart + rightRowSet.size() - 1);
                            } else if (leftOuterJoin) {
                                addBuilder.addKey(regionStart);
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
                    numRightBitsToReserve, leftOuterJoin);
            jsm.setMaximumLoadFactor(control.getMaximumLoadFactor());
            jsm.setTargetLoadFactor(control.getTargetLoadFactor());

            final TrackingWritableRowSet resultRowSet = jsm.build(leftTable, rightTable).toTracking();

            final QueryTable resultTable = makeResult(leftTable, rightTable, columnsToAdd, jsm, resultRowSet,
                    cs -> CrossJoinRightColumnSource.maybeWrap(jsm, cs, rightTable.isRefreshing()));

            final ModifiedColumnSet.Transformer rightTransformer =
                    rightTable.newModifiedColumnSetTransformer(resultTable, columnsToAdd);

            if (leftTable.isRefreshing()) {
                // LeftRowSetToSlot needs prev value tracking
                jsm.startTrackingPrevValues();

                final ModifiedColumnSet.Transformer leftTransformer = leftTable.newModifiedColumnSetTransformer(
                        resultTable, leftTable.getDefinition().getColumnNamesArray());

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
                                try (final RowSet prevRowSet = rightTable.getRowSet().copyPrev()) {
                                    jsm.rightShift(prevRowSet, upstreamRight.shifted(), tracker);
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
                                try (final RowSet prevRowSet = rightTable.getRowSet().copyPrev()) {
                                    jsm.shiftRightRowSetToSlot(prevRowSet, upstreamRight.shifted());
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

                        final RowSetBuilderRandom addedBuilder = RowSetFactory.builderRandom();
                        if (rightChanged) {
                            // With left removes (and modified-removes) applied (yet adds and modified-adds pending),
                            // we can now easily calculate which rows are removed due to right removes.

                            final RowSetBuilderRandom leftRowsToVisitForRightRmBuilder = RowSetFactory.builderRandom();
                            tracker.forAllModifiedSlots(slotState -> {
                                if (slotState.leftRowSet.size() > 0 && slotState.rightRemoved.isNonempty()) {
                                    leftRowsToVisitForRightRmBuilder.addRowSet(slotState.leftRowSet);
                                }
                            });

                            try (final RowSet leftRowsToVisitForRightRm = leftRowsToVisitForRightRmBuilder.build()) {
                                final RowSetBuilderSequential toRemoveBuilder = RowSetFactory.builderSequential();
                                leftRowsToVisitForRightRm.forAllRowKeys(ii -> {
                                    final long prevOffset = ii << prevRightBits;
                                    final CrossJoinModifiedSlotTracker.SlotState state = tracker
                                            .getFinalSlotState(jsm.getTrackerCookie(jsm.getSlotFromLeftRowKey(ii)));
                                    toRemoveBuilder.appendRowSequenceWithOffset(state.rightRemoved, prevOffset);
                                });
                                try (final RowSet toRemove = toRemoveBuilder.build()) {
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
                            final RowSetBuilderRandom addsToVisitForOuterJoinRemoval =
                                    leftOuterJoin ? RowSetFactory.builderRandom() : null;
                            final RowSetBuilderRandom modsToVisit = RowSetFactory.builderRandom();
                            tracker.forAllModifiedSlots(slotState -> {
                                if (slotState.leftRowSet.size() == 0) {
                                    return;
                                }
                                if (slotState.rightAdded.isNonempty()) {
                                    addsToVisit.addRowSet(slotState.leftRowSet);
                                    if (leftOuterJoin && slotState.rightRowSet.sizePrev() == 0) {
                                        try (final RowSet leftPrev = slotState.leftRowSet.copyPrev()) {
                                            addsToVisitForOuterJoinRemoval.addRowSet(leftPrev);
                                        }
                                    }
                                }
                                if (slotState.rightModified.isNonempty()) {
                                    modsToVisit.addRowSet(slotState.leftRowSet);
                                }
                            });

                            if (leftOuterJoin) {
                                try (final RowSet leftRowsForNullKeyRemoval = addsToVisitForOuterJoinRemoval.build()) {
                                    final RowSetBuilderSequential toRemoveBuilder = RowSetFactory.builderSequential();
                                    leftRowsForNullKeyRemoval.forAllRowKeys((key) -> {
                                        toRemoveBuilder.appendKey(key << prevRightBits);
                                    });
                                    try (final RowSet toRemove = toRemoveBuilder.build()) {
                                        downstream.removed().writableCast().insert(toRemove);
                                    }
                                }
                            }

                            try (final RowSet leftRowsToVisitForAdds = addsToVisit.build();
                                    final RowSet leftRowsToVisitForMods = modsToVisit.build()) {
                                downstream.added = addedBuilder.build();

                                leftRowsToVisitForAdds.forAllRowKeys(ii -> {
                                    final long currOffset = ii << currRightBits;
                                    final CrossJoinModifiedSlotTracker.SlotState state = tracker
                                            .getFinalSlotState(jsm.getTrackerCookie(jsm.getSlotFromLeftRowKey(ii)));
                                    downstream.added().writableCast().insertWithShift(currOffset, state.rightAdded);
                                });

                                final RowSetBuilderSequential modifiedBuilder = RowSetFactory.builderSequential();
                                leftRowsToVisitForMods.forAllRowKeys(ii -> {
                                    final long currOffset = ii << currRightBits;
                                    final CrossJoinModifiedSlotTracker.SlotState state = tracker
                                            .getFinalSlotState(jsm.getTrackerCookie(jsm.getSlotFromLeftRowKey(ii)));
                                    modifiedBuilder.appendRowSequenceWithOffset(state.rightModified, currOffset);
                                });
                                try (final RowSet modified = modifiedBuilder.build()) {
                                    downstream.modified().writableCast().insert(modified);
                                }

                                mustCloseRowsToShift = leftChanged || !allRowsShift;
                                if (allRowsShift) {
                                    rowsToShift = leftChanged ? leftTable.getRowSet().minus(upstreamLeft.added())
                                            : leftTable.getRowSet();
                                } else {
                                    rowsToShift = leftRowsToVisitForAdds.copy();
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
                        final RowSetBuilderSequential toRemoveFromResultRowSet = RowSetFactory.builderSequential();
                        final RowSetBuilderSequential toInsertIntoResultRowSet = RowSetFactory.builderSequential();

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
                                            toRemoveFromResultRowSet.appendRange(s, e);
                                            toInsertIntoResultRowSet.appendRange(s + shiftDelta, e + shiftDelta);
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
                                    final long slotFromLeftIndex = jsm.getSlotFromLeftRowKey(ii);

                                    processLeftShiftsUntil.accept(prevOffset);

                                    if (slotFromLeftIndex == RightIncrementalChunkedCrossJoinStateManager.LEFT_MAPPING_MISSING) {
                                        // Since left rows that change key-column-groups are currently removed from all
                                        // JSM data structures, they won't have a properly mapped slot. They will be
                                        // added to their new slot after we generate-downstream shifts. The result
                                        // RowSet is also updated for these rows in the left-rm/left-add code paths.
                                        // This code path should only be hit when prevRightBits != currRightBits.
                                        return;
                                    }
                                    final CrossJoinModifiedSlotTracker.SlotState slotState =
                                            tracker.getFinalSlotState(jsm.getTrackerCookie(slotFromLeftIndex));

                                    if (prevRightBits != currRightBits) {
                                        final RowSet rightRowSet = jsm.getRightRowSet(slotFromLeftIndex);
                                        if (rightRowSet.isNonempty()) {
                                            toInsertIntoResultRowSet.appendRange(currOffset,
                                                    currOffset + rightRowSet.size() - 1);
                                        } else if (leftOuterJoin) {
                                            toInsertIntoResultRowSet.appendKey(currOffset);
                                        }
                                    } else if (slotState != null) {
                                        final long prevSize = slotState.rightRowSet.sizePrev();
                                        final long currSize = slotState.rightRowSet.size();
                                        // note prevCardinality == currCardinality if prevRightBits == currRightBits
                                        if (prevOffset != currOffset) {
                                            // might be changing to an empty group
                                            if (currSize > 0) {
                                                toInsertIntoResultRowSet.appendRange(currOffset,
                                                        currOffset + currSize - 1);
                                            } else if (leftOuterJoin) {
                                                toInsertIntoResultRowSet.appendKey(currOffset);
                                            }
                                            // might have changed from an empty group
                                            if (prevSize > 0) {
                                                toRemoveFromResultRowSet.appendRange(prevOffset,
                                                        prevOffset + currCardinality - 1);
                                            } else if (leftOuterJoin) {
                                                toRemoveFromResultRowSet.appendKey(prevOffset);
                                            }
                                        } else if (prevSize < currSize) {
                                            toInsertIntoResultRowSet.appendRange(currOffset + prevSize,
                                                    currOffset + currSize - 1);
                                        } else if (currSize < prevSize && prevSize > 0) {
                                            if (currSize == 0 && leftOuterJoin) {
                                                toRemoveFromResultRowSet.appendRange(prevOffset + 1,
                                                        prevOffset + currCardinality - 1);
                                            } else {
                                                toRemoveFromResultRowSet.appendRange(prevOffset + currSize,
                                                        prevOffset + currCardinality - 1);
                                            }
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
                                final long slotFromLeftIndex = jsm.getSlotFromLeftRowKey(ii);

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
                                        toInsertIntoResultRowSet.appendRange(currOffset,
                                                currOffset + rightRowSet.size() - 1);
                                    } else if (leftOuterJoin) {
                                        toInsertIntoResultRowSet.appendKey(currOffset);
                                    }
                                } else if (slotState != null) {
                                    final long prevSize = slotState.rightRowSet.sizePrev();
                                    final long currSize = slotState.rightRowSet.size();

                                    // note: prevOffset == currOffset (because left did not shift and right bits are
                                    // unchanged)
                                    if (prevSize < currSize) {
                                        toInsertIntoResultRowSet.appendRange(currOffset + prevSize,
                                                currOffset + currSize - 1);
                                    } else if (currSize < prevSize && prevSize > 0) {
                                        // note prevCardinality == currCardinality if prevRightBits == currRightBits
                                        toRemoveFromResultRowSet.appendRange(prevOffset + currSize,
                                                prevOffset + currCardinality - 1);
                                        if (currSize == 0 && leftOuterJoin) {
                                            toInsertIntoResultRowSet.appendKey(currOffset);
                                        }
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
                                        toRemoveFromResultRowSet.appendRange(s, e);
                                        toInsertIntoResultRowSet.appendRange(s + shiftDelta, e + shiftDelta);
                                    });
                                }
                            }
                        }

                        downstream.shifted = shiftBuilder.build();

                        try (final RowSet toRemove = toRemoveFromResultRowSet.build();
                                final RowSet toInsert = toInsertIntoResultRowSet.build()) {
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
                            downstream.modifiedColumnSet = resultTable.getModifiedColumnSetForUpdates();
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
                leftTable.addUpdateListener(leftRecorder);
                rightTable.addUpdateListener(rightRecorder);
                resultTable.addParentReference(mergedListener);
            } else {
                rightTable.addUpdateListener(new BaseTable.ListenerImpl(bucketingContext.listenerDescription,
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
                                jsm.shiftRightRowSetToSlot(prevRowSet, upstream.shifted());
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
                        final RowSetBuilderSequential addToResultRowSet = RowSetFactory.builderSequential();

                        // Accumulate all changes by left row.
                        leftChanged.forAllRowKeys(ii -> {
                            final long prevOffset = ii << prevRightBits;
                            final long currOffset = ii << currRightBits;

                            final long slot = jsm.getSlotFromLeftRowKey(ii);
                            final CrossJoinModifiedSlotTracker.SlotState slotState =
                                    tracker.getFinalSlotState(jsm.getTrackerCookie(slot));
                            final TrackingRowSet rightRowSet =
                                    slotState == null ? jsm.getRightRowSet(slot) : slotState.rightRowSet;
                            final long currSize = rightRowSet.size();
                            final long prevSize = rightRowSet.sizePrev();
                            final long currSizeAdjusted = leftOuterJoin ? Math.max(currSize, 1) : currSize;
                            final long prevSizeAdjusted = leftOuterJoin ? Math.max(prevSize, 1) : prevSize;

                            if (numRightBitsChanged) {
                                if (currSize > 0) {
                                    addToResultRowSet.appendRange(currOffset, currOffset + currSize - 1);
                                } else if (leftOuterJoin) {
                                    addToResultRowSet.appendKey(currOffset);
                                }
                            } else if (slotState != null) {
                                if (prevSize < currSize) {
                                    if (!leftOuterJoin || prevSize != 0) {
                                        addToResultRowSet.appendRange(currOffset + prevSize, currOffset + currSize - 1);
                                    } else if (currSize > 1) {
                                        addToResultRowSet.appendRange(currOffset + 1, currOffset + currSize - 1);
                                    }
                                } else if (currSize < prevSize) {
                                    // note prevCardinality == currCardinality if prevRightBits == currRightBits
                                    if (!leftOuterJoin || currSize != 0) {
                                        removeFromResultIndex.appendRange(
                                                prevOffset + currSize, prevOffset + prevCardinality - 1);
                                    } else if (prevSize > 1) {
                                        removeFromResultIndex.appendRange(
                                                prevOffset + 1, prevOffset + prevCardinality - 1);
                                    }
                                }
                            }

                            if (slotState == null || !slotState.rightChanged) {
                                if (prevOffset != currOffset && prevSizeAdjusted > 0) {
                                    shifted.shiftRange(prevOffset, prevOffset + prevSizeAdjusted - 1,
                                            currOffset - prevOffset);
                                }
                                return;
                            }

                            final long preShiftShiftAmt = prevOffset - (slotState.lastIndex << prevRightBits);
                            final long postShiftShiftAmt = currOffset - (slotState.lastIndex << currRightBits);
                            if (slotState.rightAdded.isNonempty()) {
                                slotState.rightAdded.shiftInPlace(postShiftShiftAmt);
                                added.appendRowSequence(slotState.rightAdded);
                                if (leftOuterJoin && prevSize == 0) {
                                    removed.appendKey(prevOffset);
                                }
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

                            shifted.appendShiftData(slotState.innerShifted, prevOffset, prevSizeAdjusted,
                                    currOffset, currSizeAdjusted);
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
                        try (final RowSet add = addToResultRowSet.build()) {
                            resultRowSet.insert(add);
                        }

                        if (tracker.clear()) {
                            jsm.clearCookies();
                        }

                        if (downstream.modified().isEmpty()) {
                            downstream.modifiedColumnSet = ModifiedColumnSet.EMPTY;
                        } else {
                            downstream.modifiedColumnSet = resultTable.getModifiedColumnSetForUpdates();
                            rightTransformer.clearAndTransform(upstream.modifiedColumnSet(),
                                    downstream.modifiedColumnSet);
                        }

                        resultTable.notifyListeners(downstream);
                    }
                });
            }

            return resultTable;
        }
    }

    private static void validateZeroKeyIndexSpace(
            final QueryTable leftTable,
            final QueryTable rightTable,
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
    private static QueryTable zeroKeyColumnsJoin(
            final QueryTable leftTable,
            final QueryTable rightTable,
            final MatchPair[] columnsToAdd,
            final int numRightBitsToReserve,
            final String listenerDescription,
            final boolean leftOuterJoin) {
        // we are a single value join, we do not need to do any hash-related work
        validateZeroKeyIndexSpace(leftTable, rightTable, numRightBitsToReserve);
        final ZeroKeyCrossJoinShiftState crossJoinState = new ZeroKeyCrossJoinShiftState(
                Math.max(numRightBitsToReserve, CrossJoinShiftState.getMinBits(rightTable)), leftOuterJoin);

        // Initialize result table.
        final TrackingWritableRowSet resultRowSet;
        try (final WritableRowSet currRight = rightTable.getRowSet().copy()) {
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            final MutableLong currRightShift = new MutableLong();
            if (currRight.size() == 0) {
                if (leftOuterJoin) {
                    leftTable.getRowSet().forAllRowKeys((currIdx) -> {
                        final long currResultIdx = currIdx << crossJoinState.getNumShiftBits();
                        builder.appendKey(currResultIdx);
                    });
                }
                crossJoinState.setRightEmpty(true);
            } else {
                leftTable.getRowSet().forAllRowKeys((currIdx) -> {
                    final long currResultIdx = currIdx << crossJoinState.getNumShiftBits();
                    currRightShift.setValue(furtherShiftIndex(currRight, currRightShift.longValue(), currResultIdx));
                    builder.appendRowSequence(currRight);
                });
                crossJoinState.setRightEmpty(false);
            }
            resultRowSet = builder.build().toTracking();
        }

        final QueryTable result = makeResult(leftTable, rightTable, columnsToAdd, crossJoinState, resultRowSet,
                cs -> BitMaskingColumnSource.maybeWrap(crossJoinState, cs));

        if (leftTable.isRefreshing() || rightTable.isRefreshing()) {
            crossJoinState.startTrackingPrevious();
        }
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
            downstream.modifiedColumnSet = result.getModifiedColumnSetForUpdates();
            downstream.modifiedColumnSet.clear();

            final RowSetShiftData.Builder shiftBuilder = new RowSetShiftData.Builder();

            try (final SafeCloseableList closer = new SafeCloseableList()) {
                if (rightChanged && rightUpdate.modified().isNonempty()) {
                    rightTransformer.transform(rightUpdate.modifiedColumnSet(), downstream.modifiedColumnSet);
                }
                if (leftChanged && leftUpdate.modified().isNonempty()) {
                    leftTransformer.transform(leftUpdate.modifiedColumnSet(), downstream.modifiedColumnSet);
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
                    boolean modifyNullKeys = leftOuterJoin && prevRight.isEmpty() && currRight.isEmpty();
                    boolean removeNullKeys = leftOuterJoin && prevRight.isEmpty() && currRight.isNonempty();

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
                    final RowSetBuilderSequential newResultBuilder = RowSetFactory.builderSequential();
                    final RowSetBuilderSequential addedBuilder = RowSetFactory.builderSequential();
                    final RowSetBuilderSequential removedBuilder = RowSetFactory.builderSequential();
                    final RowSetBuilderSequential modifiedBuilder = RowSetFactory.builderSequential();

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
                            if (removeNullKeys) {
                                removedBuilder.appendKey(prevResultOffset);
                            } else {
                                removedBuilder.appendRowSequence(prevRight);
                            }
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

                            if (currRight.isNonempty()) {
                                currRightShift = furtherShiftIndex(currRight, currRightShift, currResultOffset);
                                addedBuilder.appendRowSequence(currRight);
                                newResultBuilder.appendRowSequence(currRight);
                            } else if (leftOuterJoin) {
                                addedBuilder.appendKey(currResultOffset);
                                newResultBuilder.appendKey(currResultOffset);
                            }

                            // Advance left current iterator.
                            Assert.eqTrue(moreLeftCurr, "moreLeftCurr");
                            currCurrIdx = leftCurrIter.currentValue();
                            currResultOffset = currCurrIdx << currRightBits;
                            moreLeftCurr = advanceIterator(leftCurrIter);
                        }

                        if (rightHasRemoves) {
                            rmRightShift = furtherShiftIndex(rmRight, rmRightShift, prevResultOffset);
                            removedBuilder.appendRowSequence(rmRight);
                            // we need to replace the result with a null row
                            if (leftOuterJoin && currRight.isEmpty()) {
                                addedBuilder.appendKey(currResultOffset);
                                newResultBuilder.appendKey(currResultOffset);
                            }
                        }

                        if (rightHasAdds) {
                            addRightShift = furtherShiftIndex(addRight, addRightShift, currResultOffset);
                            addedBuilder.appendRowSequence(addRight);
                            if (leftOuterJoin && prevRight.isEmpty()) {
                                removedBuilder.appendKey(prevResultOffset);
                            }
                        }

                        if (moreLeftMod && currCurrIdx == leftModIter.currentValue()) {
                            // currCurrIdx is modify; paint all existing rows as modified
                            moreLeftMod = advanceIterator(leftModIter);
                            if (existingRight.isNonempty()) {
                                existingRightShift =
                                        furtherShiftIndex(existingRight, existingRightShift, currResultOffset);
                                modifiedBuilder.appendRowSequence(existingRight);
                            } else if (modifyNullKeys) {
                                modifiedBuilder.appendKey(currResultOffset);
                            }
                        } else if (rightHasModifies) {
                            modRightShift = furtherShiftIndex(modRight, modRightShift, currResultOffset);
                            modifiedBuilder.appendRowSequence(modRight);
                        }

                        if (currRight.isNonempty()) {
                            currRightShift = furtherShiftIndex(currRight, currRightShift, currResultOffset);
                            newResultBuilder.appendRowSequence(currRight);
                        }

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
                        if (currRight.isNonempty()) {
                            currRightShift = furtherShiftIndex(currRight, currRightShift, currResultIdx);
                            addedBuilder.appendRowSequence(currRight);
                            newResultBuilder.appendRowSequence(currRight);
                        } else if (leftOuterJoin) {
                            addedBuilder.appendKey(currResultIdx);
                            newResultBuilder.appendKey(currResultIdx);
                        }
                    }

                    try (final RowSet newResult = newResultBuilder.build()) {
                        resultRowSet.insert(newResult);
                    }
                    downstream.added = addedBuilder.build();
                    downstream.removed = removedBuilder.build();
                    downstream.modified = modifiedBuilder.build();

                    downstream.shifted = shiftBuilder.build();

                    crossJoinState.setRightEmpty(currRight.isEmpty());
                } else {
                    // Explode left updates to apply to all right rows.
                    assert leftUpdate != null;

                    RowSet.Iterator iter = leftUpdate.removed().iterator();
                    final RowSetBuilderSequential removeBuilder = RowSetFactory.builderSequential();
                    final RowSetBuilderSequential removeRangeBuilder = RowSetFactory.builderSequential();

                    final long currRightSize = currRight.size();
                    final long currRightRange = (1L << currRightBits) - 1;
                    while (iter.hasNext()) {
                        final long currIdx = iter.nextLong();
                        final long currResultIdx = currIdx << currRightBits;
                        if (currRightSize > 0) {
                            currRightShift = furtherShiftIndex(currRight, currRightShift, currResultIdx);
                            removeBuilder.appendRowSequence(currRight);
                            removeRangeBuilder.appendRange(currResultIdx, ((currIdx + 1) << currRightBits) - 1);
                        } else if (leftOuterJoin) {
                            removeBuilder.appendKey(currResultIdx);
                            removeRangeBuilder.appendRange(currResultIdx, currResultIdx + currRightRange);
                        }
                    }
                    downstream.removed = removeBuilder.build();
                    try (final RowSet removedRanges = removeRangeBuilder.build()) {
                        resultRowSet.remove(removedRanges);
                    }

                    downstream.shifted = expandLeftOnlyShift(leftUpdate.shifted(), crossJoinState);
                    downstream.shifted.apply(resultRowSet);

                    iter = leftUpdate.modified().iterator();
                    final RowSetBuilderSequential modifiedBuilder = RowSetFactory.builderSequential();
                    while (iter.hasNext()) {
                        final long currIdx = iter.nextLong();
                        final long currResultIdx = currIdx << currRightBits;
                        if (currRightSize > 0) {
                            currRightShift = furtherShiftIndex(currRight, currRightShift, currResultIdx);
                            modifiedBuilder.appendRowSequence(currRight);
                        } else if (leftOuterJoin) {
                            modifiedBuilder.appendKey(currResultIdx);
                        }
                    }
                    downstream.modified = modifiedBuilder.build();

                    iter = leftUpdate.added().iterator();
                    final RowSetBuilderSequential addedBuilder = RowSetFactory.builderSequential();
                    while (iter.hasNext()) {
                        final long currIdx = iter.nextLong();
                        final long currResultIdx = currIdx << currRightBits;
                        if (currRightSize > 0) {
                            currRightShift = furtherShiftIndex(currRight, currRightShift, currResultIdx);
                            addedBuilder.appendRowSequence(currRight);
                        } else if (leftOuterJoin) {
                            addedBuilder.appendKey(currResultIdx);
                        }
                    }
                    downstream.added = addedBuilder.build();
                    resultRowSet.insert(downstream.added);
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
            leftTable.addUpdateListener(leftRecorder);
            rightTable.addUpdateListener(rightRecorder);
            result.addParentReference(mergedListener);
        } else if (leftTable.isRefreshing() && rightTable.size() > 0) {
            leftTable.addUpdateListener(new BaseTable.ListenerImpl(listenerDescription, leftTable, result) {
                @Override
                public void onUpdate(final TableUpdate upstream) {
                    onUpdate.accept(upstream, null);
                }
            });
        } else if (rightTable.isRefreshing() && leftTable.size() > 0) {
            rightTable.addUpdateListener(new BaseTable.ListenerImpl(listenerDescription, rightTable, result) {
                @Override
                public void onUpdate(final TableUpdate upstream) {
                    onUpdate.accept(null, upstream);
                }
            });
        }

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
            final ColumnSource<?> wrappedSource = BitShiftingColumnSource.maybeWrap(joinState, leftColumn.getValue());
            columnSourceMap.put(leftColumn.getKey(), wrappedSource);
        }

        for (MatchPair mp : columnsToAdd) {
            final T wrappedSource = newRightColumnSource.apply(rightTable.getColumnSource(mp.rightColumn()));
            columnSourceMap.put(mp.leftColumn(), wrappedSource);
        }

        return new QueryTable(resultRowSet, columnSourceMap);
    }
}

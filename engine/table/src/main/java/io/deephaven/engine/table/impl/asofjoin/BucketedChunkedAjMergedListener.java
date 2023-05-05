/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.asofjoin;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.*;
import io.deephaven.chunk.util.hashing.ChunkEquals;
import io.deephaven.engine.table.impl.join.JoinListenerRecorder;
import io.deephaven.engine.table.impl.sort.LongSortKernel;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.sources.IntegerArraySource;
import io.deephaven.engine.table.impl.sources.ObjectArraySource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.sized.SizedChunk;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.table.impl.ssa.ChunkSsaStamp;
import io.deephaven.engine.table.impl.ssa.SegmentedSortedArray;
import io.deephaven.engine.table.impl.ssa.SsaSsaStamp;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.engine.table.impl.util.compact.CompactKernel;
import io.deephaven.engine.table.impl.util.compact.LongCompactKernel;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.Arrays;
import java.util.Collections;

import static io.deephaven.engine.table.impl.asofjoin.RightIncrementalHashedAsOfJoinStateManager.*;

public class BucketedChunkedAjMergedListener extends MergedListener {
    private final JoinListenerRecorder leftRecorder;
    private final JoinListenerRecorder rightRecorder;

    private final ColumnSource<?>[] leftKeySources;
    private final ColumnSource<?>[] rightKeySources;

    private final QueryTable leftTable;
    private final QueryTable rightTable;
    private final ColumnSource<?> leftStampSource;
    private final ColumnSource<?> rightStampSource;

    private final AsOfJoinHelper.SsaFactory leftSsaFactory;
    private final AsOfJoinHelper.SsaFactory rightSsaFactory;
    private final SortingOrder order;
    private final boolean disallowExactMatch;
    private final SsaSsaStamp ssaSsaStamp;
    private final ChunkSsaStamp chunkSsaStamp;
    private final RightIncrementalHashedAsOfJoinStateManager asOfJoinStateManager;
    private final WritableRowRedirection rowRedirection;
    private final ModifiedColumnSet leftKeyColumns;
    private final ModifiedColumnSet rightKeyColumns;
    private final ModifiedColumnSet leftStampColumn;
    private final ModifiedColumnSet rightStampColumn;
    private final ModifiedColumnSet allRightColumns;
    private final ModifiedColumnSet.Transformer leftTransformer;
    private final ModifiedColumnSet.Transformer rightTransformer;

    private final int leftChunkSize;
    private final int rightChunkSize;

    private final ChunkType stampChunkType;
    private final ChunkEquals stampChunkEquals;
    private final CompactKernel stampCompact;

    private final ModifiedColumnSet resultModifiedColumnSet;

    private final ObjectArraySource<RowSetBuilderSequential> sequentialBuilders =
            new ObjectArraySource<>(RowSetBuilderSequential.class);
    private final IntegerArraySource slots = new IntegerArraySource();

    public BucketedChunkedAjMergedListener(JoinListenerRecorder leftRecorder,
            JoinListenerRecorder rightRecorder,
            String listenerDescription,
            QueryTable result,
            QueryTable leftTable,
            QueryTable rightTable,
            MatchPair[] columnsToMatch,
            MatchPair stampPair,
            MatchPair[] columnsToAdd,
            ColumnSource<?>[] leftSources,
            ColumnSource<?>[] rightSources,
            ColumnSource<?> leftStampSource,
            ColumnSource<?> rightStampSource,
            AsOfJoinHelper.SsaFactory leftSsaFactory, AsOfJoinHelper.SsaFactory rightSsaFactory,
            SortingOrder order,
            boolean disallowExactMatch,
            SsaSsaStamp ssaSsaStamp,
            JoinControl control, RightIncrementalHashedAsOfJoinStateManager asOfJoinStateManager,
            WritableRowRedirection rowRedirection) {
        super(Arrays.asList(leftRecorder, rightRecorder), Collections.emptyList(), listenerDescription, result);
        this.leftRecorder = leftRecorder;
        this.rightRecorder = rightRecorder;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.leftStampSource = leftStampSource;
        this.rightStampSource = rightStampSource;
        this.leftSsaFactory = leftSsaFactory;
        this.rightSsaFactory = rightSsaFactory;
        this.order = order;
        this.disallowExactMatch = disallowExactMatch;
        this.ssaSsaStamp = ssaSsaStamp;
        this.asOfJoinStateManager = asOfJoinStateManager;
        this.rowRedirection = rowRedirection;

        leftKeySources = leftSources;
        rightKeySources = rightSources;

        final boolean reverse = order == SortingOrder.Descending;

        stampChunkType = leftStampSource.getChunkType();
        chunkSsaStamp = ChunkSsaStamp.make(stampChunkType, reverse);
        stampChunkEquals = ChunkEquals.makeEqual(stampChunkType);
        stampCompact = CompactKernel.makeCompact(stampChunkType);

        leftStampColumn = leftTable.newModifiedColumnSet(stampPair.leftColumn());
        rightStampColumn = rightTable.newModifiedColumnSet(stampPair.rightColumn());
        leftKeyColumns = leftTable.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToMatch));
        rightKeyColumns = rightTable.newModifiedColumnSet(MatchPair.getRightColumns(columnsToMatch));
        allRightColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));
        leftTransformer =
                leftTable.newModifiedColumnSetTransformer(result, leftTable.getDefinition().getColumnNamesArray());
        rightTransformer = rightTable.newModifiedColumnSetTransformer(result, columnsToAdd);

        leftChunkSize = control.leftChunkSize();
        rightChunkSize = control.rightChunkSize();

        resultModifiedColumnSet = result.newModifiedColumnSet(result.getDefinition().getColumnNamesArray());
    }

    @Override
    public void process() {
        final TableUpdateImpl downstream = new TableUpdateImpl();
        downstream.modifiedColumnSet = resultModifiedColumnSet;
        downstream.modifiedColumnSet().clear();

        final boolean leftTicked = leftRecorder.recordedVariablesAreValid();
        final boolean rightTicked = rightRecorder.recordedVariablesAreValid();

        final boolean leftStampModified = leftTicked && leftRecorder.getModified().isNonempty()
                && leftRecorder.getModifiedColumnSet().containsAny(leftStampColumn);
        final boolean leftKeysModified = leftTicked && leftRecorder.getModified().isNonempty()
                && leftRecorder.getModifiedColumnSet().containsAny(leftKeyColumns);
        final boolean leftAdditionsOrRemovals = leftKeysModified || leftStampModified
                || (leftTicked && (leftRecorder.getAdded().isNonempty() || leftRecorder.getRemoved().isNonempty()));

        final ColumnSource.FillContext leftFillContext =
                leftAdditionsOrRemovals ? leftStampSource.makeFillContext(leftChunkSize) : null;
        final WritableChunk<Values> leftStampValues =
                leftAdditionsOrRemovals ? stampChunkType.makeWritableChunk(leftChunkSize) : null;
        final WritableLongChunk<RowKeys> leftStampKeys =
                leftAdditionsOrRemovals ? WritableLongChunk.makeWritableChunk(leftChunkSize) : null;
        final LongSortKernel<Values, RowKeys> sortKernel =
                LongSortKernel.makeContext(stampChunkType, order, Math.max(leftChunkSize, rightChunkSize), true);

        final RowSetBuilderRandom modifiedBuilder = RowSetFactory.builderRandom();


        // first we remove anything that is not of interest from the left hand side, because we don't want to
        // process the relevant right hand side changes
        if (leftTicked) {
            final RowSet leftRestampRemovals;
            if (leftStampModified || leftKeysModified) {
                leftRestampRemovals = leftRecorder.getRemoved().union(leftRecorder.getModifiedPreShift());
            } else {
                leftRestampRemovals = leftRecorder.getRemoved();
            }

            sequentialBuilders.ensureCapacity(leftRestampRemovals.size());
            slots.ensureCapacity(leftRestampRemovals.size());

            if (leftRestampRemovals.isNonempty()) {
                rowRedirection.removeAll(leftRestampRemovals);

                // We first do a probe pass, adding all of the removals to a builder in the as of join state manager
                final int removedSlotCount = asOfJoinStateManager.markForRemoval(leftRestampRemovals, leftKeySources,
                        slots, sequentialBuilders);

                final MutableObject<WritableRowSet> leftIndexOutput = new MutableObject<>();

                for (int slotIndex = 0; slotIndex < removedSlotCount; ++slotIndex) {
                    final int slot = slots.getInt(slotIndex);
                    final RowSet leftRemoved = indexFromBuilder(slotIndex);

                    rowRedirection.removeAll(leftRemoved);

                    final SegmentedSortedArray leftSsa = asOfJoinStateManager.getLeftSsaOrIndex(slot, leftIndexOutput);
                    if (leftSsa == null) {
                        leftIndexOutput.getValue().remove(leftRemoved);
                        leftIndexOutput.setValue(null);
                        continue;
                    }

                    try (final RowSequence.Iterator leftRsIt = leftRemoved.getRowSequenceIterator()) {
                        while (leftRsIt.hasMore()) {
                            assert leftFillContext != null;
                            assert leftStampValues != null;

                            final RowSequence chunkOk = leftRsIt.getNextRowSequenceWithLength(leftChunkSize);

                            leftStampSource.fillPrevChunk(leftFillContext, leftStampValues, chunkOk);
                            chunkOk.fillRowKeyChunk(leftStampKeys);

                            sortKernel.sort(leftStampKeys, leftStampValues);

                            leftSsa.remove(leftStampValues, leftStampKeys);
                        }
                    }
                }
            }

            final RowSetShiftData leftShifted = leftRecorder.getShifted();
            if (leftShifted.nonempty()) {

                try (final RowSet fullPrevRowSet = leftTable.getRowSet().copyPrev();
                        final RowSet previousToShift = fullPrevRowSet.minus(leftRestampRemovals);
                        final RowSet relevantShift = getRelevantShifts(leftShifted, previousToShift)) {
                    // now we apply the left shifts, so that anything in our SSA is a relevant thing to stamp
                    rowRedirection.applyShift(previousToShift, leftShifted);

                    if (relevantShift.isNonempty()) {
                        try (final SizedSafeCloseable<ColumnSource.FillContext> leftShiftFillContext =
                                new SizedSafeCloseable<>(leftStampSource::makeFillContext);
                                final SizedSafeCloseable<LongSortKernel<Values, RowKeys>> shiftSortContext =
                                        new SizedSafeCloseable<>(
                                                size -> LongSortKernel.makeContext(stampChunkType, order, size, true));
                                final SizedLongChunk<RowKeys> stampKeys = new SizedLongChunk<>();
                                final SizedChunk<Values> stampValues = new SizedChunk<>(stampChunkType)) {

                            sequentialBuilders.ensureCapacity(relevantShift.size());
                            slots.ensureCapacity(relevantShift.size());

                            final int shiftedSlotCount = asOfJoinStateManager.gatherShiftIndex(relevantShift,
                                    leftKeySources, slots, sequentialBuilders);

                            for (int slotIndex = 0; slotIndex < shiftedSlotCount; ++slotIndex) {
                                final RowSet shiftedRowSet = indexFromBuilder(slotIndex);
                                final int slot = slots.getInt(slotIndex);
                                final byte state = asOfJoinStateManager.getState(slot);

                                final RowSetShiftData shiftDataForSlot = leftShifted.intersect(shiftedRowSet);

                                if ((state & ENTRY_RIGHT_MASK) == ENTRY_RIGHT_IS_EMPTY) {
                                    // if the left is empty, we should be a RowSet entry rather than an SSA, and we can
                                    // not be empty, because we are responsive
                                    final WritableRowSet leftRowSet = asOfJoinStateManager.getLeftIndex(slot);
                                    shiftDataForSlot.apply(leftRowSet);
                                    shiftedRowSet.close();
                                    leftRowSet.compact();
                                    continue;
                                }

                                final SegmentedSortedArray leftSsa = asOfJoinStateManager.getLeftSsa(slot);

                                final RowSetShiftData.Iterator slotSit = shiftDataForSlot.applyIterator();

                                while (slotSit.hasNext()) {
                                    slotSit.next();
                                    final RowSet rowSetToShift =
                                            shiftedRowSet.subSetByKeyRange(slotSit.beginRange(), slotSit.endRange());
                                    ChunkedAjUtils.applyOneShift(leftSsa, leftChunkSize, leftStampSource,
                                            leftShiftFillContext, shiftSortContext, stampKeys, stampValues, slotSit,
                                            rowSetToShift);
                                    rowSetToShift.close();
                                }
                            }
                        }
                    }
                }
            }

            if (leftStampModified || leftKeysModified) {
                leftRestampRemovals.close();
            }
        } else {
            downstream.added = RowSetFactory.empty();
            downstream.removed = RowSetFactory.empty();
            downstream.shifted = RowSetShiftData.EMPTY;
        }

        if (rightTicked) {
            // next we remove and add things from the right hand side

            final boolean rightKeysModified = rightRecorder.getModifiedColumnSet().containsAny(rightKeyColumns);
            final boolean rightStampModified = rightRecorder.getModifiedColumnSet().containsAny(rightStampColumn);

            final RowSet rightRestampRemovals;
            final RowSet rightRestampAdditions;
            if (rightKeysModified || rightStampModified) {
                rightRestampAdditions = rightRecorder.getAdded().union(rightRecorder.getModified());
                rightRestampRemovals = rightRecorder.getRemoved().union(rightRecorder.getModifiedPreShift());
            } else {
                rightRestampAdditions = rightRecorder.getAdded();
                rightRestampRemovals = rightRecorder.getRemoved();
            }

            // We first do a probe pass, adding all of the removals to a builder in the as of join state manager
            final long requiredCapacity = Math.max(rightRestampRemovals.size(), rightRestampAdditions.size());
            sequentialBuilders.ensureCapacity(requiredCapacity);
            slots.ensureCapacity(requiredCapacity);
            final int removedSlotCount = asOfJoinStateManager.markForRemoval(rightRestampRemovals, rightKeySources,
                    slots, sequentialBuilders);

            final MutableObject<WritableRowSet> rowSetOutput = new MutableObject<>();
            try (final WritableLongChunk<RowKeys> priorRedirections =
                    WritableLongChunk.makeWritableChunk(rightChunkSize);
                    final ColumnSource.FillContext fillContext = rightStampSource.makeFillContext(rightChunkSize);
                    final WritableChunk<Values> rightStampValues = stampChunkType.makeWritableChunk(rightChunkSize);
                    final WritableLongChunk<RowKeys> rightStampKeys =
                            WritableLongChunk.makeWritableChunk(rightChunkSize)) {
                for (int slotIndex = 0; slotIndex < removedSlotCount; ++slotIndex) {
                    final int slot = slots.getInt(slotIndex);

                    try (final RowSet rightRemoved = indexFromBuilder(slotIndex)) {
                        final SegmentedSortedArray rightSsa =
                                asOfJoinStateManager.getRightSsaOrIndex(slot, rowSetOutput);
                        if (rightSsa == null) {
                            rowSetOutput.getValue().remove(rightRemoved);
                            continue;
                        }

                        final byte state = asOfJoinStateManager.getState(slot);
                        if ((state & ENTRY_LEFT_MASK) != ENTRY_LEFT_IS_SSA) {
                            throw new IllegalStateException();
                        }
                        final SegmentedSortedArray leftSsa = asOfJoinStateManager.getLeftSsa(slot);

                        try (final RowSequence.Iterator removeIt = rightRemoved.getRowSequenceIterator()) {
                            while (removeIt.hasMore()) {
                                final RowSequence chunkOk = removeIt.getNextRowSequenceWithLength(rightChunkSize);

                                rightStampSource.fillPrevChunk(fillContext, rightStampValues, chunkOk);
                                chunkOk.fillRowKeyChunk(rightStampKeys);
                                sortKernel.sort(rightStampKeys, rightStampValues);

                                priorRedirections.setSize(rightChunkSize);
                                rightSsa.removeAndGetPrior(rightStampValues, rightStampKeys, priorRedirections);

                                ssaSsaStamp.processRemovals(leftSsa, rightStampValues, rightStampKeys,
                                        priorRedirections, rowRedirection, modifiedBuilder, disallowExactMatch);
                            }
                        }
                    }
                }
            }

            final RowSetShiftData rightShifted = rightRecorder.getShifted();

            if (rightShifted.nonempty()) {
                try (final RowSet fullPrevRowSet = rightTable.getRowSet().copyPrev();
                        final RowSet previousToShift = fullPrevRowSet.minus(rightRestampRemovals);
                        final RowSet relevantShift = getRelevantShifts(rightShifted, previousToShift)) {

                    if (relevantShift.isNonempty()) {
                        try (final SizedSafeCloseable<ColumnSource.FillContext> rightShiftFillContext =
                                new SizedSafeCloseable<>(rightStampSource::makeFillContext);
                                final SizedSafeCloseable<LongSortKernel<Values, RowKeys>> shiftSortKernel =
                                        new SizedSafeCloseable<>(
                                                sz -> LongSortKernel.makeContext(stampChunkType, order, sz, true));
                                final SizedLongChunk<RowKeys> rightStampKeys = new SizedLongChunk<>();
                                final SizedChunk<Values> rightStampValues = new SizedChunk<>(stampChunkType)) {

                            sequentialBuilders.ensureCapacity(relevantShift.size());
                            slots.ensureCapacity(relevantShift.size());

                            final int shiftedSlotCount = asOfJoinStateManager.gatherShiftIndex(relevantShift,
                                    rightKeySources, slots, sequentialBuilders);

                            for (int slotIndex = 0; slotIndex < shiftedSlotCount; ++slotIndex) {
                                final RowSet shiftedRowSet = indexFromBuilder(slotIndex);
                                final int slot = slots.getInt(slotIndex);
                                final byte state = asOfJoinStateManager.getState(slot);

                                final SegmentedSortedArray leftSsa;
                                if ((state & ENTRY_LEFT_MASK) == ENTRY_LEFT_IS_EMPTY) {
                                    leftSsa = null;
                                } else {
                                    leftSsa = asOfJoinStateManager.getLeftSsa(slot);
                                }

                                final RowSetShiftData shiftDataForSlot = rightShifted.intersect(shiftedRowSet);

                                if (leftSsa == null) {
                                    // if the left is empty, we should be a RowSet entry rather than an SSA, and we can
                                    // not be empty, because we are responsive
                                    final WritableRowSet rightRowSet = asOfJoinStateManager.getRightIndex(slot);
                                    shiftDataForSlot.apply(rightRowSet);
                                    shiftedRowSet.close();
                                    rightRowSet.compact();
                                    continue;
                                }
                                final SegmentedSortedArray rightSsa = asOfJoinStateManager.getRightSsa(slot);

                                final RowSetShiftData.Iterator slotSit = shiftDataForSlot.applyIterator();

                                while (slotSit.hasNext()) {
                                    slotSit.next();

                                    try (final RowSet rowSetToShift =
                                            shiftedRowSet.subSetByKeyRange(slotSit.beginRange(), slotSit.endRange())) {
                                        if (slotSit.polarityReversed()) {
                                            final int shiftSize = rowSetToShift.intSize();
                                            rightStampSource.fillPrevChunk(
                                                    rightShiftFillContext.ensureCapacity(shiftSize),
                                                    rightStampValues.ensureCapacity(shiftSize), rowSetToShift);
                                            rowSetToShift.fillRowKeyChunk(rightStampKeys.ensureCapacity(shiftSize));
                                            shiftSortKernel.ensureCapacity(shiftSize).sort(rightStampKeys.get(),
                                                    rightStampValues.get());

                                            ssaSsaStamp.applyShift(leftSsa, rightStampValues.get(),
                                                    rightStampKeys.get(), slotSit.shiftDelta(), rowRedirection,
                                                    disallowExactMatch);
                                            rightSsa.applyShiftReverse(rightStampValues.get(), rightStampKeys.get(),
                                                    slotSit.shiftDelta());
                                        } else {
                                            try (final RowSequence.Iterator shiftIt =
                                                    rowSetToShift.getRowSequenceIterator()) {
                                                while (shiftIt.hasMore()) {
                                                    final RowSequence chunkOk =
                                                            shiftIt.getNextRowSequenceWithLength(rightChunkSize);
                                                    final int shiftSize = chunkOk.intSize();
                                                    chunkOk.fillRowKeyChunk(
                                                            rightStampKeys.ensureCapacity(shiftSize));
                                                    rightStampSource.fillPrevChunk(
                                                            rightShiftFillContext.ensureCapacity(shiftSize),
                                                            rightStampValues.ensureCapacity(shiftSize), chunkOk);
                                                    sortKernel.sort(rightStampKeys.get(), rightStampValues.get());

                                                    rightSsa.applyShift(rightStampValues.get(), rightStampKeys.get(),
                                                            slotSit.shiftDelta());
                                                    ssaSsaStamp.applyShift(leftSsa, rightStampValues.get(),
                                                            rightStampKeys.get(), slotSit.shiftDelta(),
                                                            rowRedirection, disallowExactMatch);
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // next we do the additions
            final int addedSlotCount = asOfJoinStateManager.buildAdditions(false, rightRestampAdditions,
                    rightKeySources, slots, sequentialBuilders);

            try (final ColumnSource.FillContext rightFillContext = rightStampSource.makeFillContext(rightChunkSize);
                    final WritableChunk<Values> stampChunk = stampChunkType.makeWritableChunk(rightChunkSize);
                    final WritableChunk<Values> nextRightValue = stampChunkType.makeWritableChunk(rightChunkSize);
                    final WritableLongChunk<RowKeys> insertedIndices =
                            WritableLongChunk.makeWritableChunk(rightChunkSize);
                    final WritableBooleanChunk<Any> retainStamps =
                            WritableBooleanChunk.makeWritableChunk(rightChunkSize)) {
                for (int slotIndex = 0; slotIndex < addedSlotCount; ++slotIndex) {
                    final int slot = slots.getInt(slotIndex);

                    final RowSet rightAdded = indexFromBuilder(slotIndex);

                    final byte state = asOfJoinStateManager.getState(slot);

                    boolean makeRightIndex = false;
                    boolean updateRightIndex = false;
                    boolean processInitial = false;

                    switch (state) {
                        case ENTRY_LEFT_IS_EMPTY | ENTRY_RIGHT_IS_EMPTY:
                            makeRightIndex = true;
                            break;

                        case ENTRY_LEFT_IS_EMPTY | ENTRY_RIGHT_IS_INDEX:
                        case ENTRY_LEFT_IS_EMPTY | ENTRY_RIGHT_IS_BUILDER:
                            updateRightIndex = true;
                            break;

                        case ENTRY_LEFT_IS_BUILDER | ENTRY_RIGHT_IS_EMPTY:
                        case ENTRY_LEFT_IS_INDEX | ENTRY_RIGHT_IS_EMPTY:
                        case ENTRY_LEFT_IS_SSA | ENTRY_RIGHT_IS_EMPTY:
                            processInitial = true;
                            break;

                        case ENTRY_LEFT_IS_BUILDER | ENTRY_RIGHT_IS_INDEX:
                        case ENTRY_LEFT_IS_INDEX | ENTRY_RIGHT_IS_INDEX:
                        case ENTRY_LEFT_IS_SSA | ENTRY_RIGHT_IS_INDEX:
                        case ENTRY_LEFT_IS_BUILDER | ENTRY_RIGHT_IS_BUILDER:
                        case ENTRY_LEFT_IS_INDEX | ENTRY_RIGHT_IS_BUILDER:
                        case ENTRY_LEFT_IS_SSA | ENTRY_RIGHT_IS_BUILDER:
                        case ENTRY_LEFT_IS_EMPTY | ENTRY_RIGHT_IS_SSA:
                        case ENTRY_LEFT_IS_BUILDER | ENTRY_RIGHT_IS_SSA:
                        case ENTRY_LEFT_IS_INDEX | ENTRY_RIGHT_IS_SSA:
                            throw new IllegalStateException();

                        case ENTRY_LEFT_IS_SSA | ENTRY_RIGHT_IS_SSA:
                            break;
                    }

                    if (makeRightIndex) {
                        asOfJoinStateManager.setRightIndex(slot, rightAdded);
                        continue;
                    }
                    if (updateRightIndex) {
                        asOfJoinStateManager.getRightIndex(slot).insert(rightAdded);
                        rightAdded.close();
                        continue;
                    }


                    final SegmentedSortedArray rightSsa = asOfJoinStateManager.getRightSsa(slot, rightSsaFactory);
                    final SegmentedSortedArray leftSsa = asOfJoinStateManager.getLeftSsa(slot, leftSsaFactory);

                    if (processInitial) {
                        ssaSsaStamp.processEntry(leftSsa, rightSsa, rowRedirection, disallowExactMatch);
                        // we've modified everything in the leftssa
                        leftSsa.forAllKeys(modifiedBuilder::addKey);
                    }

                    final int chunks = (rightAdded.intSize() + rightChunkSize - 1) / rightChunkSize;
                    for (int ii = 0; ii < chunks; ++ii) {
                        final int startChunk = chunks - ii - 1;
                        try (final RowSet chunkOk = rightAdded.subSetByPositionRange(startChunk * rightChunkSize,
                                (startChunk + 1) * rightChunkSize)) {
                            rightStampSource.fillChunk(rightFillContext, stampChunk, chunkOk);
                            insertedIndices.setSize(chunkOk.intSize());
                            chunkOk.fillRowKeyChunk(insertedIndices);

                            sortKernel.sort(insertedIndices, stampChunk);

                            final int valuesWithNext =
                                    rightSsa.insertAndGetNextValue(stampChunk, insertedIndices, nextRightValue);

                            final boolean endsWithLastValue = valuesWithNext != stampChunk.size();
                            if (endsWithLastValue) {
                                Assert.eq(valuesWithNext, "valuesWithNext", stampChunk.size() - 1,
                                        "stampChunk.size() - 1");
                                stampChunk.setSize(valuesWithNext);
                                stampChunkEquals.notEqual(stampChunk, nextRightValue, retainStamps);
                                stampCompact.compact(nextRightValue, retainStamps);

                                retainStamps.setSize(chunkOk.intSize());
                                retainStamps.set(valuesWithNext, true);
                                stampChunk.setSize(chunkOk.intSize());
                            } else {
                                // remove duplicates
                                stampChunkEquals.notEqual(stampChunk, nextRightValue, retainStamps);
                                stampCompact.compact(nextRightValue, retainStamps);
                            }
                            LongCompactKernel.compact(insertedIndices, retainStamps);
                            stampCompact.compact(stampChunk, retainStamps);

                            ssaSsaStamp.processInsertion(leftSsa, stampChunk, insertedIndices, nextRightValue,
                                    rowRedirection, modifiedBuilder, endsWithLastValue, disallowExactMatch);
                        }
                    }
                }
            }

            // if the stamp was not modified, then we need to figure out the responsive rows to mark as modified
            if (!rightStampModified && !rightKeysModified && rightRecorder.getModified().isNonempty()) {
                slots.ensureCapacity(rightRecorder.getModified().size());
                sequentialBuilders.ensureCapacity(rightRecorder.getModified().size());

                final int modifiedSlotCount = asOfJoinStateManager.gatherModifications(rightRecorder.getModified(),
                        rightKeySources, slots, sequentialBuilders);

                try (final ColumnSource.FillContext fillContext = rightStampSource.makeFillContext(rightChunkSize);
                        final WritableChunk<Values> rightStampChunk = stampChunkType.makeWritableChunk(rightChunkSize);
                        final WritableLongChunk<RowKeys> rightStampIndices =
                                WritableLongChunk.makeWritableChunk(rightChunkSize)) {
                    for (int slotIndex = 0; slotIndex < modifiedSlotCount; ++slotIndex) {
                        final int slot = slots.getInt(slotIndex);

                        final RowSet rightModified = indexFromBuilder(slotIndex);

                        final byte state = asOfJoinStateManager.getState(slot);
                        if ((state & ENTRY_LEFT_MASK) == ENTRY_LEFT_IS_EMPTY) {
                            continue;
                        }

                        // if we are not empty on the left, then we must already have created the SSA
                        final SegmentedSortedArray leftSsa = asOfJoinStateManager.getLeftSsa(slot);

                        try (final RowSequence.Iterator modit = rightModified.getRowSequenceIterator()) {
                            while (modit.hasMore()) {
                                final RowSequence chunkOk = modit.getNextRowSequenceWithLength(rightChunkSize);
                                rightStampSource.fillChunk(fillContext, rightStampChunk, chunkOk);
                                chunkOk.fillRowKeyChunk(rightStampIndices);
                                sortKernel.sort(rightStampIndices, rightStampChunk);

                                ssaSsaStamp.findModified(leftSsa, rowRedirection, rightStampChunk, rightStampIndices,
                                        modifiedBuilder, disallowExactMatch);
                            }
                        }
                    }
                }
            }

            if (rightStampModified || rightKeysModified) {
                rightRestampAdditions.close();
                rightRestampRemovals.close();
            }

            if (rightStampModified || rightKeysModified || rightRecorder.getAdded().isNonempty()
                    || rightRecorder.getRemoved().isNonempty()) {
                downstream.modifiedColumnSet().setAll(allRightColumns);
            } else {
                rightTransformer.transform(rightRecorder.getModifiedColumnSet(), downstream.modifiedColumnSet());
            }
        }

        if (leftTicked) {
            // we add the left side values now
            final RowSet leftRestampAdditions;
            if (leftStampModified || leftKeysModified) {
                leftRestampAdditions = leftRecorder.getAdded().union(leftRecorder.getModified());
            } else {
                leftRestampAdditions = leftRecorder.getAdded();
            }

            sequentialBuilders.ensureCapacity(leftRestampAdditions.size());
            slots.ensureCapacity(leftRestampAdditions.size());
            final int addedSlotCount = asOfJoinStateManager.buildAdditions(true, leftRestampAdditions, leftKeySources,
                    slots, sequentialBuilders);

            for (int slotIndex = 0; slotIndex < addedSlotCount; ++slotIndex) {
                final int slot = slots.getInt(slotIndex);

                boolean makeLeftIndex = false;
                boolean updateLeftIndex = false;
                boolean processInitial = false;

                final byte state = asOfJoinStateManager.getState(slot);
                switch (state) {
                    case ENTRY_LEFT_IS_EMPTY | ENTRY_RIGHT_IS_EMPTY:
                        makeLeftIndex = true;
                        break;

                    case ENTRY_LEFT_IS_BUILDER | ENTRY_RIGHT_IS_EMPTY:
                    case ENTRY_LEFT_IS_INDEX | ENTRY_RIGHT_IS_EMPTY:
                        updateLeftIndex = true;
                        break;
                    case ENTRY_LEFT_IS_SSA | ENTRY_RIGHT_IS_EMPTY:
                        throw new IllegalStateException();

                    case ENTRY_LEFT_IS_EMPTY | ENTRY_RIGHT_IS_BUILDER:
                    case ENTRY_LEFT_IS_EMPTY | ENTRY_RIGHT_IS_INDEX:
                        processInitial = true;
                        break;

                    case ENTRY_LEFT_IS_BUILDER | ENTRY_RIGHT_IS_BUILDER:
                    case ENTRY_LEFT_IS_INDEX | ENTRY_RIGHT_IS_BUILDER:
                    case ENTRY_LEFT_IS_SSA | ENTRY_RIGHT_IS_BUILDER:
                    case ENTRY_LEFT_IS_BUILDER | ENTRY_RIGHT_IS_INDEX:
                    case ENTRY_LEFT_IS_INDEX | ENTRY_RIGHT_IS_INDEX:
                    case ENTRY_LEFT_IS_SSA | ENTRY_RIGHT_IS_INDEX:
                    case ENTRY_LEFT_IS_EMPTY | ENTRY_RIGHT_IS_SSA:
                    case ENTRY_LEFT_IS_BUILDER | ENTRY_RIGHT_IS_SSA:
                    case ENTRY_LEFT_IS_INDEX | ENTRY_RIGHT_IS_SSA:
                        throw new IllegalStateException(
                                "Bad state: " + state + ", slot=" + slot + ", slotIndex=" + slotIndex);

                    case ENTRY_LEFT_IS_SSA | ENTRY_RIGHT_IS_SSA:
                        break;
                }

                final RowSet leftAdded = indexFromBuilder(slotIndex);

                if (makeLeftIndex) {
                    asOfJoinStateManager.setLeftIndex(slot, leftAdded);
                    continue;
                }
                if (updateLeftIndex) {
                    final WritableRowSet leftRowSet = asOfJoinStateManager.getLeftIndex(slot);
                    leftRowSet.insert(leftAdded);
                    leftAdded.close();
                    leftRowSet.compact();
                    continue;
                }


                final SegmentedSortedArray rightSsa = asOfJoinStateManager.getRightSsa(slot, rightSsaFactory);
                final SegmentedSortedArray leftSsa = asOfJoinStateManager.getLeftSsa(slot, leftSsaFactory);

                if (processInitial) {
                    ssaSsaStamp.processEntry(leftSsa, rightSsa, rowRedirection, disallowExactMatch);
                    leftSsa.forAllKeys(modifiedBuilder::addKey);
                }


                try (final RowSequence.Iterator leftRsIt = leftAdded.getRowSequenceIterator();
                        final WritableLongChunk<RowKeys> rightKeysForLeft =
                                WritableLongChunk.makeWritableChunk(leftChunkSize)) {
                    assert leftFillContext != null;
                    assert leftStampValues != null;

                    while (leftRsIt.hasMore()) {
                        final RowSequence chunkOk = leftRsIt.getNextRowSequenceWithLength(leftChunkSize);
                        leftStampSource.fillChunk(leftFillContext, leftStampValues, chunkOk);
                        chunkOk.fillRowKeyChunk(leftStampKeys);

                        sortKernel.sort(leftStampKeys, leftStampValues);

                        leftSsa.insert(leftStampValues, leftStampKeys);

                        chunkSsaStamp.processEntry(leftStampValues, leftStampKeys, rightSsa, rightKeysForLeft,
                                disallowExactMatch);

                        for (int ii = 0; ii < leftStampKeys.size(); ++ii) {
                            final long leftKey = leftStampKeys.get(ii);
                            final long rightKey = rightKeysForLeft.get(ii);
                            if (rightKey == RowSequence.NULL_ROW_KEY) {
                                rowRedirection.removeVoid(leftKey);
                            } else {
                                rowRedirection.putVoid(leftKey, rightKey);
                            }
                        }
                    }
                }
                leftAdded.close();
            }

            leftTransformer.transform(leftRecorder.getModifiedColumnSet(), downstream.modifiedColumnSet());
            if (leftKeysModified || leftStampModified) {
                downstream.modifiedColumnSet().setAll(allRightColumns);
            }
            downstream.added = leftRecorder.getAdded().copy();
            downstream.removed = leftRecorder.getRemoved().copy();
            downstream.shifted = leftRecorder.getShifted();
        }

        SafeCloseable.closeAll(sortKernel, leftStampKeys, leftStampValues, leftFillContext, leftSsaFactory,
                rightSsaFactory);

        downstream.modified = leftRecorder.getModified().union(modifiedBuilder.build());

        result.notifyListeners(downstream);
    }

    private RowSet getRelevantShifts(RowSetShiftData shifted, RowSet previousToShift) {
        final RowSetBuilderSequential relevantShiftKeys = RowSetFactory.builderSequential();

        final RowSet.RangeIterator it = previousToShift.rangeIterator();

        for (int ii = 0; ii < shifted.size(); ++ii) {
            final long beginRange = shifted.getBeginRange(ii);
            final long endRange = shifted.getEndRange(ii);
            if (!it.advance(beginRange)) {
                break;
            }
            if (it.currentRangeStart() > endRange) {
                continue;
            }

            while (true) {
                final long startOfNewRange = Math.max(it.currentRangeStart(), beginRange);
                final long endOfNewRange = Math.min(it.currentRangeEnd(), endRange);
                relevantShiftKeys.appendRange(startOfNewRange, endOfNewRange);
                if (it.currentRangeEnd() < endRange) {
                    if (!it.hasNext())
                        break;
                    it.next();
                    if (it.currentRangeStart() > endRange) {
                        break;
                    }
                } else {
                    break;
                }
            }
        }

        return relevantShiftKeys.build();
    }

    private RowSet indexFromBuilder(int slotIndex) {
        final RowSet rowSet = sequentialBuilders.get(slotIndex).build();
        sequentialBuilders.set(slotIndex, null);
        return rowSet;
    }

    @Override
    protected void destroy() {
        leftSsaFactory.close();
        rightSsaFactory.close();
    }
}

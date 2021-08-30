package io.deephaven.db.v2.join;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.hashing.ChunkEquals;
import io.deephaven.db.v2.sort.LongSortKernel;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.LongArraySource;
import io.deephaven.db.v2.sources.ObjectArraySource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.sized.SizedChunk;
import io.deephaven.db.v2.sources.chunk.sized.SizedLongChunk;
import io.deephaven.db.v2.ssa.ChunkSsaStamp;
import io.deephaven.db.v2.ssa.SegmentedSortedArray;
import io.deephaven.db.v2.ssa.SsaSsaStamp;
import io.deephaven.db.v2.utils.*;
import io.deephaven.db.v2.utils.compact.CompactKernel;
import io.deephaven.db.v2.utils.compact.LongCompactKernel;
import io.deephaven.util.SafeCloseable;
import org.apache.commons.lang3.mutable.MutableObject;

import java.util.Arrays;
import java.util.Collections;

import static io.deephaven.db.v2.RightIncrementalChunkedAsOfJoinStateManager.*;
import static io.deephaven.db.v2.sources.chunk.Attributes.*;

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
    private final RightIncrementalChunkedAsOfJoinStateManager asOfJoinStateManager;
    private final RedirectionIndex redirectionIndex;
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

    private final ObjectArraySource<Index.SequentialBuilder> sequentialBuilders =
        new ObjectArraySource<>(Index.SequentialBuilder.class);
    private final LongArraySource slots = new LongArraySource();

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
        JoinControl control, RightIncrementalChunkedAsOfJoinStateManager asOfJoinStateManager,
        RedirectionIndex redirectionIndex) {
        super(Arrays.asList(leftRecorder, rightRecorder), Collections.emptyList(),
            listenerDescription, result);
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
        this.redirectionIndex = redirectionIndex;

        leftKeySources = leftSources;
        rightKeySources = rightSources;

        final boolean reverse = order == SortingOrder.Descending;

        stampChunkType = leftStampSource.getChunkType();
        chunkSsaStamp = ChunkSsaStamp.make(stampChunkType, reverse);
        stampChunkEquals = ChunkEquals.makeEqual(stampChunkType);
        stampCompact = CompactKernel.makeCompact(stampChunkType);

        leftStampColumn = leftTable.newModifiedColumnSet(stampPair.left());
        rightStampColumn = rightTable.newModifiedColumnSet(stampPair.right());
        leftKeyColumns = leftTable.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToMatch));
        rightKeyColumns =
            rightTable.newModifiedColumnSet(MatchPair.getRightColumns(columnsToMatch));
        allRightColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));
        leftTransformer = leftTable.newModifiedColumnSetTransformer(result,
            leftTable.getDefinition().getColumnNamesArray());
        rightTransformer = rightTable.newModifiedColumnSetTransformer(result, columnsToAdd);

        leftChunkSize = control.leftChunkSize();
        rightChunkSize = control.rightChunkSize();

        resultModifiedColumnSet =
            result.newModifiedColumnSet(result.getDefinition().getColumnNamesArray());
    }

    @Override
    public void process() {
        final ShiftAwareListener.Update downstream = new ShiftAwareListener.Update();
        downstream.modifiedColumnSet = resultModifiedColumnSet;
        downstream.modifiedColumnSet.clear();

        final boolean leftTicked = leftRecorder.recordedVariablesAreValid();
        final boolean rightTicked = rightRecorder.recordedVariablesAreValid();

        final boolean leftStampModified = leftTicked && leftRecorder.getModified().nonempty()
            && leftRecorder.getModifiedColumnSet().containsAny(leftStampColumn);
        final boolean leftKeysModified = leftTicked && leftRecorder.getModified().nonempty()
            && leftRecorder.getModifiedColumnSet().containsAny(leftKeyColumns);
        final boolean leftAdditionsOrRemovals = leftKeysModified || leftStampModified || (leftTicked
            && (leftRecorder.getAdded().nonempty() || leftRecorder.getRemoved().nonempty()));

        final ColumnSource.FillContext leftFillContext =
            leftAdditionsOrRemovals ? leftStampSource.makeFillContext(leftChunkSize) : null;
        final WritableChunk<Values> leftStampValues =
            leftAdditionsOrRemovals ? stampChunkType.makeWritableChunk(leftChunkSize) : null;
        final WritableLongChunk<KeyIndices> leftStampKeys =
            leftAdditionsOrRemovals ? WritableLongChunk.makeWritableChunk(leftChunkSize) : null;
        final LongSortKernel<Values, KeyIndices> sortKernel = LongSortKernel
            .makeContext(stampChunkType, order, Math.max(leftChunkSize, rightChunkSize), true);

        final Index.RandomBuilder modifiedBuilder = Index.FACTORY.getRandomBuilder();


        // first we remove anything that is not of interest from the left hand side, because we
        // don't want to
        // process the relevant right hand side changes
        if (leftTicked) {
            final Index leftRestampRemovals;
            if (leftStampModified || leftKeysModified) {
                leftRestampRemovals =
                    leftRecorder.getRemoved().union(leftRecorder.getModifiedPreShift());
            } else {
                leftRestampRemovals = leftRecorder.getRemoved();
            }

            sequentialBuilders.ensureCapacity(leftRestampRemovals.size());
            slots.ensureCapacity(leftRestampRemovals.size());

            if (leftRestampRemovals.nonempty()) {
                leftRestampRemovals.forAllLongs(redirectionIndex::removeVoid);

                // We first do a probe pass, adding all of the removals to a builder in the as of
                // join state manager
                final int removedSlotCount = asOfJoinStateManager
                    .markForRemoval(leftRestampRemovals, leftKeySources, slots, sequentialBuilders);

                final MutableObject<Index> leftIndexOutput = new MutableObject<>();

                for (int slotIndex = 0; slotIndex < removedSlotCount; ++slotIndex) {
                    final long slot = slots.getLong(slotIndex);
                    final Index leftRemoved = indexFromBuilder(slotIndex);

                    leftRemoved.forAllLongs(redirectionIndex::removeVoid);


                    final SegmentedSortedArray leftSsa =
                        asOfJoinStateManager.getLeftSsaOrIndex(slot, leftIndexOutput);
                    if (leftSsa == null) {
                        leftIndexOutput.getValue().remove(leftRemoved);
                        leftIndexOutput.setValue(null);
                        continue;
                    }

                    try (final OrderedKeys.Iterator leftOkIt =
                        leftRemoved.getOrderedKeysIterator()) {
                        while (leftOkIt.hasMore()) {
                            assert leftFillContext != null;
                            assert leftStampValues != null;

                            final OrderedKeys chunkOk =
                                leftOkIt.getNextOrderedKeysWithLength(leftChunkSize);

                            leftStampSource.fillPrevChunk(leftFillContext, leftStampValues,
                                chunkOk);
                            chunkOk.fillKeyIndicesChunk(leftStampKeys);

                            sortKernel.sort(leftStampKeys, leftStampValues);

                            leftSsa.remove(leftStampValues, leftStampKeys);
                        }
                    }
                }

                if (leftStampModified || leftKeysModified) {
                    leftRestampRemovals.close();
                }
            }


            final IndexShiftData leftShifted = leftRecorder.getShifted();
            if (leftShifted.nonempty()) {

                try (final Index fullPrevIndex = leftTable.getIndex().getPrevIndex();
                    final Index previousToShift = fullPrevIndex.minus(leftRestampRemovals);
                    final Index relevantShift = getRelevantShifts(leftShifted, previousToShift)) {
                    // now we apply the left shifts, so that anything in our SSA is a relevant thing
                    // to stamp
                    redirectionIndex.applyShift(previousToShift, leftShifted);

                    if (relevantShift.nonempty()) {
                        try (
                            final SizedSafeCloseable<ColumnSource.FillContext> leftShiftFillContext =
                                new SizedSafeCloseable<>(leftStampSource::makeFillContext);
                            final SizedSafeCloseable<LongSortKernel<Values, KeyIndices>> shiftSortContext =
                                new SizedSafeCloseable<>(size -> LongSortKernel
                                    .makeContext(stampChunkType, order, size, true));
                            final SizedLongChunk<KeyIndices> stampKeys = new SizedLongChunk<>();
                            final SizedChunk<Values> stampValues =
                                new SizedChunk<>(stampChunkType)) {

                            sequentialBuilders.ensureCapacity(relevantShift.size());
                            slots.ensureCapacity(relevantShift.size());

                            final int shiftedSlotCount = asOfJoinStateManager.gatherShiftIndex(
                                relevantShift, leftKeySources, slots, sequentialBuilders);

                            for (int slotIndex = 0; slotIndex < shiftedSlotCount; ++slotIndex) {
                                final Index shiftedIndex = indexFromBuilder(slotIndex);
                                final long slot = slots.getLong(slotIndex);
                                final byte state = asOfJoinStateManager.getState(slot);

                                final IndexShiftData shiftDataForSlot =
                                    leftShifted.intersect(shiftedIndex);

                                if ((state & ENTRY_RIGHT_MASK) == ENTRY_RIGHT_IS_EMPTY) {
                                    // if the left is empty, we should be an index entry rather than
                                    // an SSA, and we can not be empty, because we are responsive
                                    final Index leftIndex = asOfJoinStateManager.getLeftIndex(slot);
                                    shiftDataForSlot.apply(leftIndex);
                                    shiftedIndex.close();
                                    leftIndex.compact();
                                    continue;
                                }

                                final SegmentedSortedArray leftSsa =
                                    asOfJoinStateManager.getLeftSsa(slot);

                                final IndexShiftData.Iterator slotSit =
                                    shiftDataForSlot.applyIterator();

                                while (slotSit.hasNext()) {
                                    slotSit.next();
                                    final Index indexToShift = shiftedIndex
                                        .subindexByKey(slotSit.beginRange(), slotSit.endRange());
                                    ChunkedAjUtilities.applyOneShift(leftSsa, leftChunkSize,
                                        leftStampSource, leftShiftFillContext, shiftSortContext,
                                        stampKeys, stampValues, slotSit, indexToShift);
                                    indexToShift.close();
                                }
                            }
                        }
                    }
                }
            }
        } else {
            downstream.added = Index.FACTORY.getEmptyIndex();
            downstream.removed = Index.FACTORY.getEmptyIndex();
            downstream.shifted = IndexShiftData.EMPTY;
        }

        if (rightTicked) {
            // next we remove and add things from the right hand side

            final boolean rightKeysModified =
                rightRecorder.getModifiedColumnSet().containsAny(rightKeyColumns);
            final boolean rightStampModified =
                rightRecorder.getModifiedColumnSet().containsAny(rightStampColumn);

            final Index rightRestampRemovals;
            final Index rightRestampAdditions;
            if (rightKeysModified || rightStampModified) {
                rightRestampAdditions = rightRecorder.getAdded().union(rightRecorder.getModified());
                rightRestampRemovals =
                    rightRecorder.getRemoved().union(rightRecorder.getModifiedPreShift());
            } else {
                rightRestampAdditions = rightRecorder.getAdded();
                rightRestampRemovals = rightRecorder.getRemoved();
            }

            // We first do a probe pass, adding all of the removals to a builder in the as of join
            // state manager
            final long requiredCapacity =
                Math.max(rightRestampRemovals.size(), rightRestampAdditions.size());
            sequentialBuilders.ensureCapacity(requiredCapacity);
            slots.ensureCapacity(requiredCapacity);
            final int removedSlotCount = asOfJoinStateManager.markForRemoval(rightRestampRemovals,
                rightKeySources, slots, sequentialBuilders);

            final MutableObject<Index> indexOutput = new MutableObject<>();
            try (
                final WritableLongChunk<KeyIndices> priorRedirections =
                    WritableLongChunk.makeWritableChunk(rightChunkSize);
                final ColumnSource.FillContext fillContext =
                    rightStampSource.makeFillContext(rightChunkSize);
                final WritableChunk<Values> rightStampValues =
                    stampChunkType.makeWritableChunk(rightChunkSize);
                final WritableLongChunk<KeyIndices> rightStampKeys =
                    WritableLongChunk.makeWritableChunk(rightChunkSize)) {
                for (int slotIndex = 0; slotIndex < removedSlotCount; ++slotIndex) {
                    final long slot = slots.getLong(slotIndex);

                    try (final Index rightRemoved = indexFromBuilder(slotIndex)) {
                        final SegmentedSortedArray rightSsa =
                            asOfJoinStateManager.getRightSsaOrIndex(slot, indexOutput);
                        if (rightSsa == null) {
                            indexOutput.getValue().remove(rightRemoved);
                            continue;
                        }

                        final byte state = asOfJoinStateManager.getState(slot);
                        if ((state & ENTRY_LEFT_MASK) != ENTRY_LEFT_IS_SSA) {
                            throw new IllegalStateException();
                        }
                        final SegmentedSortedArray leftSsa = asOfJoinStateManager.getLeftSsa(slot);

                        try (final OrderedKeys.Iterator removeIt =
                            rightRemoved.getOrderedKeysIterator()) {
                            while (removeIt.hasMore()) {
                                final OrderedKeys chunkOk =
                                    removeIt.getNextOrderedKeysWithLength(rightChunkSize);

                                rightStampSource.fillPrevChunk(fillContext, rightStampValues,
                                    chunkOk);
                                chunkOk.fillKeyIndicesChunk(rightStampKeys);
                                sortKernel.sort(rightStampKeys, rightStampValues);

                                priorRedirections.setSize(rightChunkSize);
                                rightSsa.removeAndGetPrior(rightStampValues, rightStampKeys,
                                    priorRedirections);

                                ssaSsaStamp.processRemovals(leftSsa, rightStampValues,
                                    rightStampKeys, priorRedirections, redirectionIndex,
                                    modifiedBuilder, disallowExactMatch);
                            }
                        }
                    }
                }
            }

            final IndexShiftData rightShifted = rightRecorder.getShifted();

            if (rightShifted.nonempty()) {
                try (final Index fullPrevIndex = rightTable.getIndex().getPrevIndex();
                    final Index previousToShift = fullPrevIndex.minus(rightRestampRemovals);
                    final Index relevantShift = getRelevantShifts(rightShifted, previousToShift)) {

                    if (relevantShift.nonempty()) {
                        try (
                            final SizedSafeCloseable<ColumnSource.FillContext> rightShiftFillContext =
                                new SizedSafeCloseable<>(rightStampSource::makeFillContext);
                            final SizedSafeCloseable<LongSortKernel<Values, KeyIndices>> shiftSortKernel =
                                new SizedSafeCloseable<>(sz -> LongSortKernel
                                    .makeContext(stampChunkType, order, sz, true));
                            final SizedLongChunk<KeyIndices> rightStampKeys =
                                new SizedLongChunk<>();
                            final SizedChunk<Values> rightStampValues =
                                new SizedChunk<>(stampChunkType)) {

                            sequentialBuilders.ensureCapacity(relevantShift.size());
                            slots.ensureCapacity(relevantShift.size());

                            final int shiftedSlotCount = asOfJoinStateManager.gatherShiftIndex(
                                relevantShift, rightKeySources, slots, sequentialBuilders);

                            for (int slotIndex = 0; slotIndex < shiftedSlotCount; ++slotIndex) {
                                final Index shiftedIndex = indexFromBuilder(slotIndex);
                                final long slot = slots.getLong(slotIndex);
                                final byte state = asOfJoinStateManager.getState(slot);

                                final SegmentedSortedArray leftSsa;
                                if ((state & ENTRY_LEFT_MASK) == ENTRY_LEFT_IS_EMPTY) {
                                    leftSsa = null;
                                } else {
                                    leftSsa = asOfJoinStateManager.getLeftSsa(slot);
                                }

                                final IndexShiftData shiftDataForSlot =
                                    rightShifted.intersect(shiftedIndex);

                                if (leftSsa == null) {
                                    // if the left is empty, we should be an index entry rather than
                                    // an SSA, and we can not be empty, because we are responsive
                                    final Index rightIndex =
                                        asOfJoinStateManager.getRightIndex(slot);
                                    shiftDataForSlot.apply(rightIndex);
                                    shiftedIndex.close();
                                    rightIndex.compact();
                                    continue;
                                }
                                final SegmentedSortedArray rightSsa =
                                    asOfJoinStateManager.getRightSsa(slot);

                                final IndexShiftData.Iterator slotSit =
                                    shiftDataForSlot.applyIterator();

                                while (slotSit.hasNext()) {
                                    slotSit.next();

                                    try (final Index indexToShift = shiftedIndex
                                        .subindexByKey(slotSit.beginRange(), slotSit.endRange())) {
                                        if (slotSit.polarityReversed()) {
                                            final int shiftSize = indexToShift.intSize();
                                            rightStampSource.fillPrevChunk(
                                                rightShiftFillContext.ensureCapacity(shiftSize),
                                                rightStampValues.ensureCapacity(shiftSize),
                                                indexToShift);
                                            indexToShift.fillKeyIndicesChunk(
                                                rightStampKeys.ensureCapacity(shiftSize));
                                            shiftSortKernel.ensureCapacity(shiftSize)
                                                .sort(rightStampKeys.get(), rightStampValues.get());

                                            ssaSsaStamp.applyShift(leftSsa, rightStampValues.get(),
                                                rightStampKeys.get(), slotSit.shiftDelta(),
                                                redirectionIndex, disallowExactMatch);
                                            rightSsa.applyShiftReverse(rightStampValues.get(),
                                                rightStampKeys.get(), slotSit.shiftDelta());
                                        } else {
                                            try (final OrderedKeys.Iterator shiftIt =
                                                indexToShift.getOrderedKeysIterator()) {
                                                while (shiftIt.hasMore()) {
                                                    final OrderedKeys chunkOk =
                                                        shiftIt.getNextOrderedKeysWithLength(
                                                            rightChunkSize);
                                                    final int shiftSize = chunkOk.intSize();
                                                    chunkOk.fillKeyIndicesChunk(
                                                        rightStampKeys.ensureCapacity(shiftSize));
                                                    rightStampSource.fillPrevChunk(
                                                        rightShiftFillContext
                                                            .ensureCapacity(shiftSize),
                                                        rightStampValues.ensureCapacity(shiftSize),
                                                        chunkOk);
                                                    sortKernel.sort(rightStampKeys.get(),
                                                        rightStampValues.get());

                                                    rightSsa.applyShift(rightStampValues.get(),
                                                        rightStampKeys.get(), slotSit.shiftDelta());
                                                    ssaSsaStamp.applyShift(leftSsa,
                                                        rightStampValues.get(),
                                                        rightStampKeys.get(), slotSit.shiftDelta(),
                                                        redirectionIndex, disallowExactMatch);
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
            final int addedSlotCount = asOfJoinStateManager.buildAdditions(false,
                rightRestampAdditions, rightKeySources, slots, sequentialBuilders);

            try (
                final ColumnSource.FillContext rightFillContext =
                    rightStampSource.makeFillContext(rightChunkSize);
                final WritableChunk<Values> stampChunk =
                    stampChunkType.makeWritableChunk(rightChunkSize);
                final WritableChunk<Values> nextRightValue =
                    stampChunkType.makeWritableChunk(rightChunkSize);
                final WritableLongChunk<KeyIndices> insertedIndices =
                    WritableLongChunk.makeWritableChunk(rightChunkSize);
                final WritableBooleanChunk<Any> retainStamps =
                    WritableBooleanChunk.makeWritableChunk(rightChunkSize)) {
                for (int slotIndex = 0; slotIndex < addedSlotCount; ++slotIndex) {
                    final long slot = slots.getLong(slotIndex);

                    final Index rightAdded = indexFromBuilder(slotIndex);

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


                    final SegmentedSortedArray rightSsa =
                        asOfJoinStateManager.getRightSsa(slot, rightSsaFactory);
                    final SegmentedSortedArray leftSsa =
                        asOfJoinStateManager.getLeftSsa(slot, leftSsaFactory);

                    if (processInitial) {
                        ssaSsaStamp.processEntry(leftSsa, rightSsa, redirectionIndex,
                            disallowExactMatch);
                        // we've modified everything in the leftssa
                        leftSsa.forAllKeys(modifiedBuilder::addKey);
                    }

                    final int chunks = (rightAdded.intSize() + rightChunkSize - 1) / rightChunkSize;
                    for (int ii = 0; ii < chunks; ++ii) {
                        final int startChunk = chunks - ii - 1;
                        try (final Index chunkOk = rightAdded.subindexByPos(
                            startChunk * rightChunkSize, (startChunk + 1) * rightChunkSize)) {
                            rightStampSource.fillChunk(rightFillContext, stampChunk, chunkOk);
                            insertedIndices.setSize(chunkOk.intSize());
                            chunkOk.fillKeyIndicesChunk(insertedIndices);

                            sortKernel.sort(insertedIndices, stampChunk);

                            final int valuesWithNext = rightSsa.insertAndGetNextValue(stampChunk,
                                insertedIndices, nextRightValue);

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

                            ssaSsaStamp.processInsertion(leftSsa, stampChunk, insertedIndices,
                                nextRightValue, redirectionIndex, modifiedBuilder,
                                endsWithLastValue, disallowExactMatch);
                        }
                    }
                }
            }

            // if the stamp was not modified, then we need to figure out the responsive rows to mark
            // as modified
            if (!rightStampModified && !rightKeysModified
                && rightRecorder.getModified().nonempty()) {
                slots.ensureCapacity(rightRecorder.getModified().size());
                sequentialBuilders.ensureCapacity(rightRecorder.getModified().size());

                final int modifiedSlotCount = asOfJoinStateManager.gatherModifications(
                    rightRecorder.getModified(), rightKeySources, slots, sequentialBuilders);

                try (
                    final ColumnSource.FillContext fillContext =
                        rightStampSource.makeFillContext(rightChunkSize);
                    final WritableChunk<Values> rightStampChunk =
                        stampChunkType.makeWritableChunk(rightChunkSize);
                    final WritableLongChunk<KeyIndices> rightStampIndices =
                        WritableLongChunk.makeWritableChunk(rightChunkSize)) {
                    for (int slotIndex = 0; slotIndex < modifiedSlotCount; ++slotIndex) {
                        final long slot = slots.getLong(slotIndex);

                        final Index rightModified = indexFromBuilder(slotIndex);

                        final byte state = asOfJoinStateManager.getState(slot);
                        if ((state & ENTRY_LEFT_MASK) == ENTRY_LEFT_IS_EMPTY) {
                            continue;
                        }

                        // if we are not empty on the left, then we must already have created the
                        // SSA
                        final SegmentedSortedArray leftSsa = asOfJoinStateManager.getLeftSsa(slot);

                        try (final OrderedKeys.Iterator modit =
                            rightModified.getOrderedKeysIterator()) {
                            while (modit.hasMore()) {
                                final OrderedKeys chunkOk =
                                    modit.getNextOrderedKeysWithLength(rightChunkSize);
                                rightStampSource.fillChunk(fillContext, rightStampChunk, chunkOk);
                                chunkOk.fillKeyIndicesChunk(rightStampIndices);
                                sortKernel.sort(rightStampIndices, rightStampChunk);

                                ssaSsaStamp.findModified(leftSsa, redirectionIndex, rightStampChunk,
                                    rightStampIndices, modifiedBuilder, disallowExactMatch);
                            }
                        }
                    }
                }
            }

            if (rightStampModified || rightKeysModified) {
                rightRestampAdditions.close();
                rightRestampRemovals.close();
            }

            if (rightStampModified || rightKeysModified || rightRecorder.getAdded().nonempty()
                || rightRecorder.getRemoved().nonempty()) {
                downstream.modifiedColumnSet.setAll(allRightColumns);
            } else {
                rightTransformer.transform(rightRecorder.getModifiedColumnSet(),
                    downstream.modifiedColumnSet);
            }
        }

        if (leftTicked) {
            // we add the left side values now
            final Index leftRestampAdditions;
            if (leftStampModified || leftKeysModified) {
                leftRestampAdditions = leftRecorder.getAdded().union(leftRecorder.getModified());
            } else {
                leftRestampAdditions = leftRecorder.getAdded();
            }

            sequentialBuilders.ensureCapacity(leftRestampAdditions.size());
            slots.ensureCapacity(leftRestampAdditions.size());
            final int addedSlotCount = asOfJoinStateManager.buildAdditions(true,
                leftRestampAdditions, leftKeySources, slots, sequentialBuilders);

            for (int slotIndex = 0; slotIndex < addedSlotCount; ++slotIndex) {
                final long slot = slots.getLong(slotIndex);

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

                final Index leftAdded = indexFromBuilder(slotIndex);

                if (makeLeftIndex) {
                    asOfJoinStateManager.setLeftIndex(slot, leftAdded);
                    continue;
                }
                if (updateLeftIndex) {
                    final Index leftIndex = asOfJoinStateManager.getLeftIndex(slot);
                    leftIndex.insert(leftAdded);
                    leftAdded.close();
                    leftIndex.compact();
                    continue;
                }


                final SegmentedSortedArray rightSsa =
                    asOfJoinStateManager.getRightSsa(slot, rightSsaFactory);
                final SegmentedSortedArray leftSsa =
                    asOfJoinStateManager.getLeftSsa(slot, leftSsaFactory);

                if (processInitial) {
                    ssaSsaStamp.processEntry(leftSsa, rightSsa, redirectionIndex,
                        disallowExactMatch);
                    leftSsa.forAllKeys(modifiedBuilder::addKey);
                }


                try (final OrderedKeys.Iterator leftOkIt = leftAdded.getOrderedKeysIterator();
                    final WritableLongChunk<KeyIndices> rightKeysForLeft =
                        WritableLongChunk.makeWritableChunk(leftChunkSize)) {
                    assert leftFillContext != null;
                    assert leftStampValues != null;

                    while (leftOkIt.hasMore()) {
                        final OrderedKeys chunkOk =
                            leftOkIt.getNextOrderedKeysWithLength(leftChunkSize);
                        leftStampSource.fillChunk(leftFillContext, leftStampValues, chunkOk);
                        chunkOk.fillKeyIndicesChunk(leftStampKeys);

                        sortKernel.sort(leftStampKeys, leftStampValues);

                        leftSsa.insert(leftStampValues, leftStampKeys);

                        chunkSsaStamp.processEntry(leftStampValues, leftStampKeys, rightSsa,
                            rightKeysForLeft, disallowExactMatch);

                        for (int ii = 0; ii < leftStampKeys.size(); ++ii) {
                            final long leftKey = leftStampKeys.get(ii);
                            final long rightKey = rightKeysForLeft.get(ii);
                            if (rightKey == Index.NULL_KEY) {
                                redirectionIndex.removeVoid(leftKey);
                            } else {
                                redirectionIndex.putVoid(leftKey, rightKey);
                            }
                        }
                    }
                }
                leftAdded.close();
            }

            leftTransformer.transform(leftRecorder.getModifiedColumnSet(),
                downstream.modifiedColumnSet);
            if (leftKeysModified || leftStampModified) {
                downstream.modifiedColumnSet.setAll(allRightColumns);
            }
            downstream.added = leftRecorder.getAdded().clone();
            downstream.removed = leftRecorder.getRemoved().clone();
            downstream.shifted = leftRecorder.getShifted();
        }

        SafeCloseable.closeArray(sortKernel, leftStampKeys, leftStampValues, leftFillContext,
            leftSsaFactory, rightSsaFactory);

        downstream.modified = leftRecorder.getModified().union(modifiedBuilder.getIndex());

        result.notifyListeners(downstream);
    }

    private Index getRelevantShifts(IndexShiftData shifted, Index previousToShift) {
        final Index.RandomBuilder relevantShiftKeys = Index.CURRENT_FACTORY.getRandomBuilder();
        final IndexShiftData.Iterator sit = shifted.applyIterator();
        while (sit.hasNext()) {
            sit.next();
            final Index indexToShift =
                previousToShift.subindexByKey(sit.beginRange(), sit.endRange());
            if (!indexToShift.empty()) {
                relevantShiftKeys.addIndex(indexToShift);
            }
            indexToShift.close();
        }
        return relevantShiftKeys.getIndex();
    }

    private Index indexFromBuilder(int slotIndex) {
        final Index index = sequentialBuilders.get(slotIndex).getIndex();
        sequentialBuilders.set(slotIndex, null);
        return index;
    }

    @Override
    protected void destroy() {
        leftSsaFactory.close();
        rightSsaFactory.close();
    }
}

package io.deephaven.db.v2.join;

import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.v2.*;
import io.deephaven.db.v2.hashing.ChunkEquals;
import io.deephaven.db.v2.sort.LongSortKernel;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.sources.chunk.Attributes.Any;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.sized.SizedChunk;
import io.deephaven.db.v2.sources.chunk.sized.SizedLongChunk;
import io.deephaven.db.v2.ssa.ChunkSsaStamp;
import io.deephaven.db.v2.ssa.SegmentedSortedArray;
import io.deephaven.db.v2.ssa.SsaSsaStamp;
import io.deephaven.db.v2.utils.*;
import io.deephaven.db.v2.utils.compact.CompactKernel;
import io.deephaven.db.v2.utils.compact.LongCompactKernel;

import java.util.Arrays;
import java.util.Collections;

public class ZeroKeyChunkedAjMergedListener extends MergedListener {
    private final JoinListenerRecorder leftRecorder;
    private final JoinListenerRecorder rightRecorder;
    private final QueryTable leftTable;
    private final QueryTable rightTable;
    private final ColumnSource<?> leftStampSource;
    private final ColumnSource<?> rightStampSource;
    private final SortingOrder order;
    private final boolean disallowExactMatch;
    private final SsaSsaStamp ssaSsaStamp;
    private final ChunkSsaStamp chunkSsaStamp;
    private final SegmentedSortedArray leftSsa;
    private final SegmentedSortedArray rightSsa;
    private final RedirectionIndex redirectionIndex;
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

    public ZeroKeyChunkedAjMergedListener(JoinListenerRecorder leftRecorder,
            JoinListenerRecorder rightRecorder,
            String listenerDescription,
            QueryTable result,
            QueryTable leftTable,
            QueryTable rightTable,
            MatchPair stampPair,
            MatchPair[] columnsToAdd,
            ColumnSource<?> leftStampSource,
            ColumnSource<?> rightStampSource,
            SortingOrder order,
            boolean disallowExactMatch,
            SsaSsaStamp ssaSsaStamp,
            SegmentedSortedArray leftSsa,
            SegmentedSortedArray rightSsa,
            RedirectionIndex redirectionIndex,
            JoinControl joinControl) {
        super(Arrays.asList(leftRecorder, rightRecorder), Collections.emptyList(), listenerDescription, result);
        this.leftRecorder = leftRecorder;
        this.rightRecorder = rightRecorder;
        this.leftTable = leftTable;
        this.rightTable = rightTable;
        this.leftStampSource = leftStampSource;
        this.rightStampSource = rightStampSource;
        this.order = order;
        this.disallowExactMatch = disallowExactMatch;
        this.ssaSsaStamp = ssaSsaStamp;
        this.leftSsa = leftSsa;
        this.rightSsa = rightSsa;
        this.redirectionIndex = redirectionIndex;

        leftChunkSize = joinControl.leftChunkSize();
        rightChunkSize = joinControl.rightChunkSize();

        stampChunkType = leftStampSource.getChunkType();
        chunkSsaStamp = ChunkSsaStamp.make(stampChunkType, order == SortingOrder.Descending);
        stampChunkEquals = ChunkEquals.makeEqual(stampChunkType);
        stampCompact = CompactKernel.makeCompact(stampChunkType);

        leftStampColumn = leftTable.newModifiedColumnSet(stampPair.left());
        rightStampColumn = rightTable.newModifiedColumnSet(stampPair.right());
        allRightColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));
        leftTransformer =
                leftTable.newModifiedColumnSetTransformer(result, leftTable.getDefinition().getColumnNamesArray());
        rightTransformer = rightTable.newModifiedColumnSetTransformer(result, columnsToAdd);

        resultModifiedColumnSet = result.newModifiedColumnSet(result.getDefinition().getColumnNamesArray());
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
        final boolean leftAdditionsOrRemovals = leftStampModified
                || (leftTicked && (leftRecorder.getAdded().nonempty() || leftRecorder.getRemoved().nonempty()));

        try (final ColumnSource.FillContext leftFillContext =
                leftAdditionsOrRemovals ? leftStampSource.makeFillContext(leftChunkSize) : null;
                final WritableChunk<Values> leftStampValues =
                        leftAdditionsOrRemovals ? stampChunkType.makeWritableChunk(leftChunkSize) : null;
                final WritableLongChunk<KeyIndices> leftStampKeys =
                        leftAdditionsOrRemovals ? WritableLongChunk.makeWritableChunk(leftChunkSize) : null;
                final LongSortKernel<Values, KeyIndices> sortKernel = LongSortKernel.makeContext(stampChunkType, order,
                        Math.max(leftChunkSize, rightChunkSize), true)) {
            final Index.RandomBuilder modifiedBuilder = Index.FACTORY.getRandomBuilder();

            // first we remove anything that is not of interest from the left hand side, because we don't want to
            // process the relevant right hand side changes
            if (leftTicked) {
                final Index leftRemoved = leftRecorder.getRemoved();

                final Index leftRestampRemovals;
                if (leftStampModified) {
                    leftRestampRemovals = leftRemoved.union(leftRecorder.getModifiedPreShift());
                } else {
                    leftRestampRemovals = leftRemoved;
                }

                if (leftRestampRemovals.nonempty()) {
                    leftRestampRemovals.forAllLongs(redirectionIndex::removeVoid);

                    try (final OrderedKeys.Iterator leftOkIt = leftRestampRemovals.getOrderedKeysIterator()) {
                        assert leftFillContext != null;
                        assert leftStampValues != null;

                        while (leftOkIt.hasMore()) {
                            final OrderedKeys chunkOk = leftOkIt.getNextOrderedKeysWithLength(leftChunkSize);

                            leftStampSource.fillPrevChunk(leftFillContext, leftStampValues, chunkOk);
                            chunkOk.fillKeyIndicesChunk(leftStampKeys);

                            sortKernel.sort(leftStampKeys, leftStampValues);

                            leftSsa.remove(leftStampValues, leftStampKeys);
                        }
                    }
                }

                if (leftStampModified) {
                    leftRestampRemovals.close();
                }


                final IndexShiftData leftShifted = leftRecorder.getShifted();
                if (leftShifted.nonempty()) {
                    // now we apply the left shifts, so that anything in our SSA is a relevant thing to stamp
                    try (final Index prevIndex = leftTable.getIndex().getPrevIndex()) {
                        redirectionIndex.applyShift(prevIndex, leftShifted);
                    }
                    ChunkedAjUtilities.bothIncrementalLeftSsaShift(leftShifted, leftSsa, leftRestampRemovals, leftTable,
                            leftChunkSize, leftStampSource);
                }
            } else {
                downstream.added = Index.FACTORY.getEmptyIndex();
                downstream.removed = Index.FACTORY.getEmptyIndex();
                downstream.shifted = IndexShiftData.EMPTY;
            }

            if (rightTicked) {
                // next we remove and add things from the right hand side

                final boolean rightStampModified = rightRecorder.getModifiedColumnSet().containsAny(rightStampColumn);

                try (final ColumnSource.FillContext fillContext = rightStampSource.makeFillContext(rightChunkSize);
                        final WritableChunk<Values> rightStampValues = stampChunkType.makeWritableChunk(rightChunkSize);
                        final WritableLongChunk<KeyIndices> rightStampKeys =
                                WritableLongChunk.makeWritableChunk(rightChunkSize)) {
                    final Index rightRestampRemovals;
                    final Index rightRestampAdditions;
                    final Index rightModified = rightRecorder.getModified();
                    if (rightStampModified) {
                        rightRestampAdditions = rightRecorder.getAdded().union(rightModified);
                        rightRestampRemovals = rightRecorder.getRemoved().union(rightRecorder.getModifiedPreShift());
                    } else {
                        rightRestampAdditions = rightRecorder.getAdded();
                        rightRestampRemovals = rightRecorder.getRemoved();
                    }

                    // When removing a row, record the stamp, redirection key, and prior redirection key. Binary search
                    // in the left for the removed key to find the smallest value geq the removed right. Update all rows
                    // with the removed redirection to the previous key.
                    try (final OrderedKeys.Iterator removeit = rightRestampRemovals.getOrderedKeysIterator();
                            final WritableLongChunk<KeyIndices> priorRedirections =
                                    WritableLongChunk.makeWritableChunk(rightChunkSize)) {
                        while (removeit.hasMore()) {
                            final OrderedKeys chunkOk = removeit.getNextOrderedKeysWithLength(rightChunkSize);
                            rightStampSource.fillPrevChunk(fillContext, rightStampValues, chunkOk);
                            chunkOk.fillKeyIndicesChunk(rightStampKeys);
                            sortKernel.sort(rightStampKeys, rightStampValues);

                            rightSsa.removeAndGetPrior(rightStampValues, rightStampKeys, priorRedirections);
                            ssaSsaStamp.processRemovals(leftSsa, rightStampValues, rightStampKeys, priorRedirections,
                                    redirectionIndex, modifiedBuilder, disallowExactMatch);
                        }
                    }

                    final IndexShiftData rightShifted = rightRecorder.getShifted();
                    if (rightShifted.nonempty()) {
                        try (final Index fullPrevIndex = rightTable.getIndex().getPrevIndex();
                                final Index previousToShift = fullPrevIndex.minus(rightRestampRemovals);
                                final SizedSafeCloseable<ColumnSource.FillContext> shiftFillContext =
                                        new SizedSafeCloseable<>(rightStampSource::makeFillContext);
                                final SizedSafeCloseable<LongSortKernel<Values, KeyIndices>> shiftSortContext =
                                        new SizedSafeCloseable<>(
                                                sz -> LongSortKernel.makeContext(stampChunkType, order, sz, true));
                                final SizedChunk<Values> shiftRightStampValues = new SizedChunk<>(stampChunkType);
                                final SizedLongChunk<KeyIndices> shiftRightStampKeys = new SizedLongChunk<>()) {
                            final IndexShiftData.Iterator sit = rightShifted.applyIterator();
                            while (sit.hasNext()) {
                                sit.next();
                                final Index indexToShift =
                                        previousToShift.subindexByKey(sit.beginRange(), sit.endRange());
                                if (indexToShift.empty()) {
                                    indexToShift.close();
                                    continue;
                                }

                                if (sit.polarityReversed()) {
                                    final int shiftSize = indexToShift.intSize();

                                    rightStampSource.fillPrevChunk(shiftFillContext.ensureCapacity(shiftSize),
                                            shiftRightStampValues.ensureCapacity(shiftSize), indexToShift);
                                    indexToShift.fillKeyIndicesChunk(shiftRightStampKeys.ensureCapacity(shiftSize));
                                    shiftSortContext.ensureCapacity(shiftSize).sort(shiftRightStampKeys.get(),
                                            shiftRightStampValues.get());

                                    ssaSsaStamp.applyShift(leftSsa, shiftRightStampValues.get(),
                                            shiftRightStampKeys.get(), sit.shiftDelta(), redirectionIndex,
                                            disallowExactMatch);
                                    rightSsa.applyShiftReverse(shiftRightStampValues.get(), shiftRightStampKeys.get(),
                                            sit.shiftDelta());
                                } else {
                                    try (final OrderedKeys.Iterator shiftIt = indexToShift.getOrderedKeysIterator()) {
                                        while (shiftIt.hasMore()) {
                                            final OrderedKeys chunkOk =
                                                    shiftIt.getNextOrderedKeysWithLength(rightChunkSize);
                                            rightStampSource.fillPrevChunk(fillContext, rightStampValues, chunkOk);
                                            chunkOk.fillKeyIndicesChunk(rightStampKeys);
                                            sortKernel.sort(rightStampKeys, rightStampValues);

                                            rightSsa.applyShift(rightStampValues, rightStampKeys, sit.shiftDelta());
                                            ssaSsaStamp.applyShift(leftSsa, rightStampValues, rightStampKeys,
                                                    sit.shiftDelta(), redirectionIndex, disallowExactMatch);
                                        }
                                    }
                                }

                                indexToShift.close();
                            }
                        }
                    }


                    // When adding a row to the right hand side: we need to know which left hand side might be
                    // responsive. If we are a duplicate stamp and not the last one, we ignore it. Next, we should
                    // binary
                    // search in the left for the first value >=, everything up until the next extant right value should
                    // be
                    // restamped with our value
                    try (final WritableChunk<Values> stampChunk = stampChunkType.makeWritableChunk(rightChunkSize);
                            final WritableChunk<Values> nextRightValue =
                                    stampChunkType.makeWritableChunk(rightChunkSize);
                            final WritableLongChunk<KeyIndices> insertedIndices =
                                    WritableLongChunk.makeWritableChunk(rightChunkSize);
                            final WritableBooleanChunk<Any> retainStamps =
                                    WritableBooleanChunk.makeWritableChunk(rightChunkSize)) {
                        final int chunks = (rightRestampAdditions.intSize() + rightChunkSize - 1) / rightChunkSize;
                        for (int ii = 0; ii < chunks; ++ii) {
                            final int startChunk = chunks - ii - 1;
                            try (final Index chunkOk = rightRestampAdditions.subindexByPos(startChunk * rightChunkSize,
                                    (startChunk + 1) * rightChunkSize)) {
                                final int chunkSize = chunkOk.intSize();
                                rightStampSource.fillChunk(fillContext, stampChunk, chunkOk);
                                insertedIndices.setSize(chunkSize);
                                chunkOk.fillKeyIndicesChunk(insertedIndices);

                                sortKernel.sort(insertedIndices, stampChunk);

                                final int valuesWithNext =
                                        rightSsa.insertAndGetNextValue(stampChunk, insertedIndices, nextRightValue);

                                final boolean endsWithLastValue = valuesWithNext != stampChunk.size();
                                if (endsWithLastValue) {
                                    stampChunk.setSize(valuesWithNext);
                                    stampChunkEquals.notEqual(stampChunk, nextRightValue, retainStamps);
                                    stampCompact.compact(nextRightValue, retainStamps);

                                    retainStamps.setSize(chunkSize);
                                    retainStamps.set(valuesWithNext, true);
                                    stampChunk.setSize(chunkSize);
                                } else {
                                    // remove duplicates
                                    stampChunkEquals.notEqual(stampChunk, nextRightValue, retainStamps);
                                    stampCompact.compact(nextRightValue, retainStamps);
                                }
                                LongCompactKernel.compact(insertedIndices, retainStamps);
                                stampCompact.compact(stampChunk, retainStamps);

                                ssaSsaStamp.processInsertion(leftSsa, stampChunk, insertedIndices, nextRightValue,
                                        redirectionIndex, modifiedBuilder, endsWithLastValue, disallowExactMatch);
                            }
                        }
                    }

                    // if the stamp was not modified, then we need to figure out the responsive rows to mark as modified
                    if (!rightStampModified && rightModified.nonempty()) {
                        try (final OrderedKeys.Iterator modit = rightModified.getOrderedKeysIterator()) {
                            while (modit.hasMore()) {
                                final OrderedKeys chunkOk = modit.getNextOrderedKeysWithLength(rightChunkSize);
                                rightStampSource.fillChunk(fillContext, rightStampValues, chunkOk);
                                chunkOk.fillKeyIndicesChunk(rightStampKeys);
                                sortKernel.sort(rightStampKeys, rightStampValues);
                                ssaSsaStamp.findModified(leftSsa, redirectionIndex, rightStampValues, rightStampKeys,
                                        modifiedBuilder, disallowExactMatch);
                            }
                        }
                    }

                    if (rightStampModified) {
                        rightRestampAdditions.close();
                        rightRestampRemovals.close();
                    }

                    if (rightStampModified || rightRecorder.getAdded().nonempty()
                            || rightRecorder.getRemoved().nonempty()) {
                        downstream.modifiedColumnSet.setAll(allRightColumns);
                    } else {
                        rightTransformer.transform(rightRecorder.getModifiedColumnSet(), downstream.modifiedColumnSet);
                    }
                }
            }

            if (leftTicked) {
                // we add the left side values now
                final Index leftRestampAdditions;
                if (leftStampModified) {
                    leftRestampAdditions = leftRecorder.getAdded().union(leftRecorder.getModified());
                } else {
                    leftRestampAdditions = leftRecorder.getAdded();
                }

                try (final OrderedKeys.Iterator leftOkIt = leftRestampAdditions.getOrderedKeysIterator();
                        final WritableLongChunk<KeyIndices> rightKeysForLeft =
                                WritableLongChunk.makeWritableChunk(leftChunkSize)) {
                    while (leftOkIt.hasMore()) {
                        assert leftFillContext != null;
                        assert leftStampValues != null;

                        final OrderedKeys chunkOk = leftOkIt.getNextOrderedKeysWithLength(leftChunkSize);
                        leftStampSource.fillChunk(leftFillContext, leftStampValues, chunkOk);
                        chunkOk.fillKeyIndicesChunk(leftStampKeys);
                        sortKernel.sort(leftStampKeys, leftStampValues);

                        leftSsa.insert(leftStampValues, leftStampKeys);

                        chunkSsaStamp.processEntry(leftStampValues, leftStampKeys, rightSsa, rightKeysForLeft,
                                disallowExactMatch);

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

                leftTransformer.transform(leftRecorder.getModifiedColumnSet(), downstream.modifiedColumnSet);
                if (leftStampModified) {
                    downstream.modifiedColumnSet.setAll(allRightColumns);
                    leftRestampAdditions.close();
                }
                downstream.added = leftRecorder.getAdded().clone();
                downstream.removed = leftRecorder.getRemoved().clone();
                downstream.shifted = leftRecorder.getShifted();
            }

            downstream.modified = leftRecorder.getModified().union(modifiedBuilder.getIndex());
        }

        result.notifyListeners(downstream);
    }
}

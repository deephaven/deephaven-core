package io.deephaven.engine.table.impl.join;

import io.deephaven.engine.rowset.*;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ModifiedColumnSet;
import io.deephaven.engine.table.impl.TableUpdateImpl;
import io.deephaven.engine.table.impl.SortingOrder;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.impl.*;
import io.deephaven.chunk.util.hashing.ChunkEquals;
import io.deephaven.engine.table.impl.sort.LongSortKernel;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.chunk.*;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.sized.SizedChunk;
import io.deephaven.chunk.sized.SizedLongChunk;
import io.deephaven.engine.table.impl.ssa.ChunkSsaStamp;
import io.deephaven.engine.table.impl.ssa.SegmentedSortedArray;
import io.deephaven.engine.table.impl.ssa.SsaSsaStamp;
import io.deephaven.engine.table.impl.util.*;
import io.deephaven.engine.table.impl.util.compact.CompactKernel;
import io.deephaven.engine.table.impl.util.compact.LongCompactKernel;

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
    private final WritableRowRedirection rowRedirection;
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
            WritableRowRedirection rowRedirection,
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
        this.rowRedirection = rowRedirection;

        leftChunkSize = joinControl.leftChunkSize();
        rightChunkSize = joinControl.rightChunkSize();

        stampChunkType = leftStampSource.getChunkType();
        chunkSsaStamp = ChunkSsaStamp.make(stampChunkType, order == SortingOrder.Descending);
        stampChunkEquals = ChunkEquals.makeEqual(stampChunkType);
        stampCompact = CompactKernel.makeCompact(stampChunkType);

        leftStampColumn = leftTable.newModifiedColumnSet(stampPair.leftColumn());
        rightStampColumn = rightTable.newModifiedColumnSet(stampPair.rightColumn());
        allRightColumns = result.newModifiedColumnSet(MatchPair.getLeftColumns(columnsToAdd));
        leftTransformer =
                leftTable.newModifiedColumnSetTransformer(result, leftTable.getDefinition().getColumnNamesArray());
        rightTransformer = rightTable.newModifiedColumnSetTransformer(result, columnsToAdd);

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
        final boolean leftAdditionsOrRemovals = leftStampModified
                || (leftTicked && (leftRecorder.getAdded().isNonempty() || leftRecorder.getRemoved().isNonempty()));

        try (final ColumnSource.FillContext leftFillContext =
                leftAdditionsOrRemovals ? leftStampSource.makeFillContext(leftChunkSize) : null;
                final WritableChunk<Values> leftStampValues =
                        leftAdditionsOrRemovals ? stampChunkType.makeWritableChunk(leftChunkSize) : null;
                final WritableLongChunk<RowKeys> leftStampKeys =
                        leftAdditionsOrRemovals ? WritableLongChunk.makeWritableChunk(leftChunkSize) : null;
                final LongSortKernel<Values, RowKeys> sortKernel = LongSortKernel.makeContext(stampChunkType, order,
                        Math.max(leftChunkSize, rightChunkSize), true)) {
            final RowSetBuilderRandom modifiedBuilder = RowSetFactory.builderRandom();

            // first we remove anything that is not of interest from the left hand side, because we don't want to
            // process the relevant right hand side changes
            if (leftTicked) {
                final RowSet leftRemoved = leftRecorder.getRemoved();

                final RowSet leftRestampRemovals;
                if (leftStampModified) {
                    leftRestampRemovals = leftRemoved.union(leftRecorder.getModifiedPreShift());
                } else {
                    leftRestampRemovals = leftRemoved;
                }

                if (leftRestampRemovals.isNonempty()) {
                    leftRestampRemovals.forAllRowKeys(rowRedirection::removeVoid);

                    try (final RowSequence.Iterator leftRsIt = leftRestampRemovals.getRowSequenceIterator()) {
                        assert leftFillContext != null;
                        assert leftStampValues != null;

                        while (leftRsIt.hasMore()) {
                            final RowSequence chunkOk = leftRsIt.getNextRowSequenceWithLength(leftChunkSize);

                            leftStampSource.fillPrevChunk(leftFillContext, leftStampValues, chunkOk);
                            chunkOk.fillRowKeyChunk(leftStampKeys);

                            sortKernel.sort(leftStampKeys, leftStampValues);

                            leftSsa.remove(leftStampValues, leftStampKeys);
                        }
                    }
                }

                final RowSetShiftData leftShifted = leftRecorder.getShifted();
                if (leftShifted.nonempty()) {
                    // now we apply the left shifts, so that anything in our SSA is a relevant thing to stamp
                    try (final RowSet prevRowSet = leftTable.getRowSet().copyPrev()) {
                        rowRedirection.applyShift(prevRowSet, leftShifted);
                    }
                    ChunkedAjUtils.bothIncrementalLeftSsaShift(leftShifted, leftSsa, leftRestampRemovals, leftTable,
                            leftChunkSize, leftStampSource);
                }

                if (leftStampModified) {
                    leftRestampRemovals.close();
                }
            } else {
                downstream.added = RowSetFactory.empty();
                downstream.removed = RowSetFactory.empty();
                downstream.shifted = RowSetShiftData.EMPTY;
            }

            if (rightTicked) {
                // next we remove and add things from the right hand side

                final boolean rightStampModified = rightRecorder.getModifiedColumnSet().containsAny(rightStampColumn);

                try (final ColumnSource.FillContext fillContext = rightStampSource.makeFillContext(rightChunkSize);
                        final WritableChunk<Values> rightStampValues = stampChunkType.makeWritableChunk(rightChunkSize);
                        final WritableLongChunk<RowKeys> rightStampKeys =
                                WritableLongChunk.makeWritableChunk(rightChunkSize)) {
                    final RowSet rightRestampRemovals;
                    final RowSet rightRestampAdditions;
                    final RowSet rightModified = rightRecorder.getModified();
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
                    try (final RowSequence.Iterator removeit = rightRestampRemovals.getRowSequenceIterator();
                            final WritableLongChunk<RowKeys> priorRedirections =
                                    WritableLongChunk.makeWritableChunk(rightChunkSize)) {
                        while (removeit.hasMore()) {
                            final RowSequence chunkOk = removeit.getNextRowSequenceWithLength(rightChunkSize);
                            rightStampSource.fillPrevChunk(fillContext, rightStampValues, chunkOk);
                            chunkOk.fillRowKeyChunk(rightStampKeys);
                            sortKernel.sort(rightStampKeys, rightStampValues);

                            rightSsa.removeAndGetPrior(rightStampValues, rightStampKeys, priorRedirections);
                            ssaSsaStamp.processRemovals(leftSsa, rightStampValues, rightStampKeys, priorRedirections,
                                    rowRedirection, modifiedBuilder, disallowExactMatch);
                        }
                    }

                    final RowSetShiftData rightShifted = rightRecorder.getShifted();
                    if (rightShifted.nonempty()) {
                        try (final RowSet fullPrevRowSet = rightTable.getRowSet().copyPrev();
                                final RowSet previousToShift = fullPrevRowSet.minus(rightRestampRemovals);
                                final SizedSafeCloseable<ColumnSource.FillContext> shiftFillContext =
                                        new SizedSafeCloseable<>(rightStampSource::makeFillContext);
                                final SizedSafeCloseable<LongSortKernel<Values, RowKeys>> shiftSortContext =
                                        new SizedSafeCloseable<>(
                                                sz -> LongSortKernel.makeContext(stampChunkType, order, sz, true));
                                final SizedChunk<Values> shiftRightStampValues = new SizedChunk<>(stampChunkType);
                                final SizedLongChunk<RowKeys> shiftRightStampKeys = new SizedLongChunk<>()) {
                            final RowSetShiftData.Iterator sit = rightShifted.applyIterator();
                            while (sit.hasNext()) {
                                sit.next();
                                final RowSet rowSetToShift =
                                        previousToShift.subSetByKeyRange(sit.beginRange(), sit.endRange());
                                if (rowSetToShift.isEmpty()) {
                                    rowSetToShift.close();
                                    continue;
                                }

                                if (sit.polarityReversed()) {
                                    final int shiftSize = rowSetToShift.intSize();

                                    rightStampSource.fillPrevChunk(shiftFillContext.ensureCapacity(shiftSize),
                                            shiftRightStampValues.ensureCapacity(shiftSize), rowSetToShift);
                                    rowSetToShift.fillRowKeyChunk(shiftRightStampKeys.ensureCapacity(shiftSize));
                                    shiftSortContext.ensureCapacity(shiftSize).sort(shiftRightStampKeys.get(),
                                            shiftRightStampValues.get());

                                    ssaSsaStamp.applyShift(leftSsa, shiftRightStampValues.get(),
                                            shiftRightStampKeys.get(), sit.shiftDelta(), rowRedirection,
                                            disallowExactMatch);
                                    rightSsa.applyShiftReverse(shiftRightStampValues.get(), shiftRightStampKeys.get(),
                                            sit.shiftDelta());
                                } else {
                                    try (final RowSequence.Iterator shiftIt = rowSetToShift.getRowSequenceIterator()) {
                                        while (shiftIt.hasMore()) {
                                            final RowSequence chunkOk =
                                                    shiftIt.getNextRowSequenceWithLength(rightChunkSize);
                                            rightStampSource.fillPrevChunk(fillContext, rightStampValues, chunkOk);
                                            chunkOk.fillRowKeyChunk(rightStampKeys);
                                            sortKernel.sort(rightStampKeys, rightStampValues);

                                            rightSsa.applyShift(rightStampValues, rightStampKeys, sit.shiftDelta());
                                            ssaSsaStamp.applyShift(leftSsa, rightStampValues, rightStampKeys,
                                                    sit.shiftDelta(), rowRedirection, disallowExactMatch);
                                        }
                                    }
                                }

                                rowSetToShift.close();
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
                            final WritableLongChunk<RowKeys> insertedIndices =
                                    WritableLongChunk.makeWritableChunk(rightChunkSize);
                            final WritableBooleanChunk<Any> retainStamps =
                                    WritableBooleanChunk.makeWritableChunk(rightChunkSize)) {
                        final int chunks = (rightRestampAdditions.intSize() + rightChunkSize - 1) / rightChunkSize;
                        for (int ii = 0; ii < chunks; ++ii) {
                            final int startChunk = chunks - ii - 1;
                            try (final RowSet chunkOk =
                                    rightRestampAdditions.subSetByPositionRange(startChunk * rightChunkSize,
                                            (startChunk + 1) * rightChunkSize)) {
                                final int chunkSize = chunkOk.intSize();
                                rightStampSource.fillChunk(fillContext, stampChunk, chunkOk);
                                insertedIndices.setSize(chunkSize);
                                chunkOk.fillRowKeyChunk(insertedIndices);

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
                                        rowRedirection, modifiedBuilder, endsWithLastValue, disallowExactMatch);
                            }
                        }
                    }

                    // if the stamp was not modified, then we need to figure out the responsive rows to mark as modified
                    if (!rightStampModified && rightModified.isNonempty()) {
                        try (final RowSequence.Iterator modit = rightModified.getRowSequenceIterator()) {
                            while (modit.hasMore()) {
                                final RowSequence chunkOk = modit.getNextRowSequenceWithLength(rightChunkSize);
                                rightStampSource.fillChunk(fillContext, rightStampValues, chunkOk);
                                chunkOk.fillRowKeyChunk(rightStampKeys);
                                sortKernel.sort(rightStampKeys, rightStampValues);
                                ssaSsaStamp.findModified(leftSsa, rowRedirection, rightStampValues, rightStampKeys,
                                        modifiedBuilder, disallowExactMatch);
                            }
                        }
                    }

                    if (rightStampModified) {
                        rightRestampAdditions.close();
                        rightRestampRemovals.close();
                    }

                    if (rightStampModified || rightRecorder.getAdded().isNonempty()
                            || rightRecorder.getRemoved().isNonempty()) {
                        downstream.modifiedColumnSet().setAll(allRightColumns);
                    } else {
                        rightTransformer.transform(rightRecorder.getModifiedColumnSet(),
                                downstream.modifiedColumnSet());
                    }
                }
            }

            if (leftTicked) {
                // we add the left side values now
                final RowSet leftRestampAdditions;
                if (leftStampModified) {
                    leftRestampAdditions = leftRecorder.getAdded().union(leftRecorder.getModified());
                } else {
                    leftRestampAdditions = leftRecorder.getAdded();
                }

                try (final RowSequence.Iterator leftRsIt = leftRestampAdditions.getRowSequenceIterator();
                        final WritableLongChunk<RowKeys> rightKeysForLeft =
                                WritableLongChunk.makeWritableChunk(leftChunkSize)) {
                    while (leftRsIt.hasMore()) {
                        assert leftFillContext != null;
                        assert leftStampValues != null;

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

                leftTransformer.transform(leftRecorder.getModifiedColumnSet(), downstream.modifiedColumnSet());
                if (leftStampModified) {
                    downstream.modifiedColumnSet().setAll(allRightColumns);
                    leftRestampAdditions.close();
                }
                downstream.added = leftRecorder.getAdded().copy();
                downstream.removed = leftRecorder.getRemoved().copy();
                downstream.shifted = leftRecorder.getShifted();
            }

            downstream.modified = leftRecorder.getModified().union(modifiedBuilder.build());
        }

        result.notifyListeners(downstream);
    }
}

/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.Context;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.impl.join.dupcompact.DupCompactKernel;
import io.deephaven.engine.table.impl.join.stamp.StampKernel;
import io.deephaven.engine.table.impl.sort.LongSortKernel;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.*;
import io.deephaven.engine.table.impl.util.WritableRowRedirection;
import io.deephaven.engine.rowset.RowSet;

class AsOfStampContext implements Context {
    private final ChunkType stampType;
    private final SortingOrder order;

    private final ColumnSource<?> leftStampSource;
    private final ColumnSource<?> rightStampSource;

    private final ColumnSource<?> originalRightStampSource;

    private int sortCapacity = -1;
    private int leftCapacity = -1;
    private int rightCapacity = -1;
    private int rightFillCapacity = -1;

    private WritableChunk<Values> leftStampChunk;
    private WritableChunk<Values> rightStampChunk;

    private WritableLongChunk<RowKeys> leftKeyIndicesChunk;
    private WritableLongChunk<RowKeys> rightKeyIndicesChunk;

    private ChunkSource.FillContext leftFillContext;
    private ChunkSource.FillContext rightFillContext;

    private final DupCompactKernel rightDupCompact;
    private LongSortKernel<Values, RowKeys> sortKernel;

    private WritableLongChunk<RowKeys> leftRedirections;

    private final StampKernel stampKernel;

    AsOfStampContext(SortingOrder order, boolean disallowExactMatch, ColumnSource<?> leftStampSource,
            ColumnSource<?> rightStampSource, ColumnSource<?> originalRightStampSource) {
        this.order = order;
        this.leftStampSource = leftStampSource;
        this.rightStampSource = rightStampSource;
        this.originalRightStampSource = originalRightStampSource;
        final ChunkType leftType = leftStampSource.getChunkType();
        final ChunkType rightType = rightStampSource.getChunkType();
        if (leftType != rightType) {
            throw new IllegalArgumentException(
                    "Stamp columns must have the same type, left=" + leftType + ", right=" + rightType);
        }
        this.stampType = leftType;
        this.stampKernel = StampKernel.makeStampKernel(stampType, order, disallowExactMatch);
        this.rightDupCompact = DupCompactKernel.makeDupCompact(stampType, order == SortingOrder.Descending);
    }

    private void ensureSortCapacity(int length) {
        if (length <= sortCapacity) {
            return;
        }
        if (length < 1 << 16) {
            length = Integer.highestOneBit(length) * 2;
        }
        if (sortKernel != null) {
            sortKernel.close();
        }
        sortKernel = LongSortKernel.makeContext(stampType, order, length, true);
        sortCapacity = length;
    }

    private void ensureLeftCapacity(int length) {
        if (length <= leftCapacity) {
            return;
        }
        if (length < 1 << 16) {
            length = Integer.highestOneBit(length) * 2;
        }
        ensureSortCapacity(length);

        closeLeftThings();

        leftStampChunk = stampType.makeWritableChunk(length);
        leftFillContext = leftStampSource.makeFillContext(length);
        leftKeyIndicesChunk = WritableLongChunk.makeWritableChunk(length);
        leftRedirections = WritableLongChunk.makeWritableChunk(length);

        leftCapacity = length;
    }

    private void closeLeftThings() {
        if (leftStampChunk != null) {
            leftStampChunk.close();
        }
        if (leftFillContext != null) {
            leftFillContext.close();
        }
        if (leftKeyIndicesChunk != null) {
            leftKeyIndicesChunk.close();
        }
        if (leftRedirections != null) {
            leftRedirections.close();
        }
    }

    private void ensureRightFillCapacity(int length) {
        if (length <= rightFillCapacity) {
            return;
        }
        if (length < 1 << 16) {
            length = Integer.highestOneBit(length) * 2;
        }
        ensureSortCapacity(length);
        if (rightFillContext != null) {
            rightFillContext.close();
        }
        rightFillContext = rightStampSource.makeFillContext(length);
        rightFillCapacity = length;
    }

    private void ensureRightCapacity(int length) {
        if (length <= rightCapacity) {
            return;
        }
        if (length < 1 << 16) {
            length = Integer.highestOneBit(length) * 2;
        }
        closeRightChunks();

        rightStampChunk = stampType.makeWritableChunk(length);
        rightKeyIndicesChunk = WritableLongChunk.makeWritableChunk(length);
        rightCapacity = length;
    }

    private void closeRightChunks() {
        if (rightStampChunk != null) {
            rightStampChunk.close();
        }
        if (rightKeyIndicesChunk != null) {
            rightKeyIndicesChunk.close();
        }
    }

    @Override
    public void close() {
        stampKernel.close();
        if (rightFillContext != null) {
            rightFillContext.close();
        }
        if (sortKernel != null) {
            sortKernel.close();
        }
        closeRightChunks();
        closeLeftThings();
    }

    /**
     * Process a single stamp state, reading the values from the right stamp column.
     *
     * @param leftRowSet the row keys of the left values to stamp
     * @param rightRowSet the row keys of the right values in this state
     * @param rowRedirection the row redirection to update
     */
    void processEntry(RowSet leftRowSet, RowSet rightRowSet, WritableRowRedirection rowRedirection) {
        ensureRightCapacity(rightRowSet.intSize());
        getAndCompactStamps(rightRowSet, rightKeyIndicesChunk, rightStampChunk);
        processEntry(leftRowSet, rightStampChunk, rightKeyIndicesChunk, rowRedirection);
    }

    /**
     * Fill and and compact the values in the right RowSet into the rightKeyIndicesChunk and rightStampChunk.
     *
     * @param rightRowSet the row keys of the right values to read and compact
     * @param rightKeyIndicesChunk the output chunk of rightKeyIndices
     * @param rightStampChunk the output chunk of right stamp values
     */
    void getAndCompactStamps(RowSet rightRowSet, WritableLongChunk<RowKeys> rightKeyIndicesChunk,
            WritableChunk<Values> rightStampChunk) {
        ensureRightFillCapacity(rightRowSet.intSize());
        // read the right stamp column
        rightKeyIndicesChunk.setSize(rightRowSet.intSize());
        rightStampSource.fillChunk(rightFillContext, rightStampChunk, rightRowSet);
        rightRowSet.fillRowKeyChunk(rightKeyIndicesChunk);

        sortKernel.sort(rightKeyIndicesChunk, rightStampChunk);

        rightDupCompact.compactDuplicates(rightStampChunk, rightKeyIndicesChunk);
    }

    /**
     * Process a single stamp state, using the supplied chunks
     *
     * @param leftRowSet the row keys of the left values to stamp
     * @param rightStampChunk the right stamp values (already compacted)
     * @param rightKeyIndicesChunk the right key indices (already compacted)
     * @param rowRedirection the row redirection to update
     */
    void processEntry(RowSet leftRowSet, Chunk<Values> rightStampChunk, LongChunk<RowKeys> rightKeyIndicesChunk,
            WritableRowRedirection rowRedirection) {
        ensureLeftCapacity(leftRowSet.intSize());

        // read the left stamp column
        leftStampSource.fillChunk(leftFillContext, leftStampChunk, leftRowSet);
        leftKeyIndicesChunk.setSize(leftRowSet.intSize());
        leftRowSet.fillRowKeyChunk(leftKeyIndicesChunk);

        // sort the left stamp column
        sortKernel.sort(leftKeyIndicesChunk, leftStampChunk);

        // figure out our "merge"
        computeRedirections(rowRedirection, rightStampChunk, rightKeyIndicesChunk);
    }

    private void computeRedirections(WritableRowRedirection rowRedirection, Chunk<Values> rightStampChunk,
            LongChunk<RowKeys> rightKeyIndicesChunk) {
        stampKernel.computeRedirections(leftStampChunk, rightStampChunk, rightKeyIndicesChunk, leftRedirections);
        for (int ii = 0; ii < leftKeyIndicesChunk.size(); ++ii) {
            final long rightKey = leftRedirections.get(ii);
            // the row redirection defaults to NULL_ROW_KEY, so we do not need to put it in there
            if (rightKey != RowSequence.NULL_ROW_KEY) {
                rowRedirection.putVoid(leftKeyIndicesChunk.get(ii), rightKey);
            }
        }
    }
}

package io.deephaven.db.v2;

import io.deephaven.db.tables.SortingOrder;
import io.deephaven.db.v2.join.dupcompact.DupCompactKernel;
import io.deephaven.db.v2.join.stamp.StampKernel;
import io.deephaven.db.v2.sort.LongSortKernel;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes.KeyIndices;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.*;
import io.deephaven.db.v2.utils.Index;
import io.deephaven.db.v2.utils.RedirectionIndex;

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

    private WritableLongChunk<KeyIndices> leftKeyIndicesChunk;
    private WritableLongChunk<KeyIndices> rightKeyIndicesChunk;

    private ChunkSource.FillContext leftFillContext;
    private ChunkSource.FillContext rightFillContext;

    private final DupCompactKernel rightDupCompact;
    private LongSortKernel<Values, KeyIndices> sortKernel;

    private WritableLongChunk<KeyIndices> leftRedirections;

    private final StampKernel stampKernel;

    AsOfStampContext(SortingOrder order, boolean disallowExactMatch,
        ColumnSource<?> leftStampSource, ColumnSource<?> rightStampSource,
        ColumnSource<?> originalRightStampSource) {
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
        this.rightDupCompact =
            DupCompactKernel.makeDupCompact(stampType, order == SortingOrder.Descending);
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
        rightDupCompact.close();
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
     * @param leftIndex the indices of the left values to stamp
     * @param rightIndex the indices of the right values in this state
     * @param redirectionIndex the redirection index to update
     */
    void processEntry(Index leftIndex, Index rightIndex, RedirectionIndex redirectionIndex) {
        ensureRightCapacity(rightIndex.intSize());
        getAndCompactStamps(rightIndex, rightKeyIndicesChunk, rightStampChunk);
        processEntry(leftIndex, rightStampChunk, rightKeyIndicesChunk, redirectionIndex);
    }

    /**
     * Fill and and compact the values in the right index into the rightKeyIndicesChunk and
     * rightStampChunk.
     *
     * @param rightIndex the indices of the right values to read and compact
     * @param rightKeyIndicesChunk the output chunk of rightKeyIndices
     * @param rightStampChunk the output chunk of right stamp values
     */
    void getAndCompactStamps(Index rightIndex, WritableLongChunk<KeyIndices> rightKeyIndicesChunk,
        WritableChunk<Values> rightStampChunk) {
        ensureRightFillCapacity(rightIndex.intSize());
        // read the right stamp column
        rightKeyIndicesChunk.setSize(rightIndex.intSize());
        rightStampSource.fillChunk(rightFillContext, rightStampChunk, rightIndex);
        rightIndex.fillKeyIndicesChunk(rightKeyIndicesChunk);

        sortKernel.sort(rightKeyIndicesChunk, rightStampChunk);

        rightDupCompact.compactDuplicates(rightStampChunk, rightKeyIndicesChunk);
    }

    /**
     * Process a single stamp state, using the supplied chunks
     *
     * @param leftIndex the indices of the left values to stamp
     * @param rightStampChunk the right stamp values (already compacted)
     * @param rightKeyIndicesChunk the right key indices (already compacted)
     * @param redirectionIndex the redirection index to update
     */
    void processEntry(Index leftIndex, Chunk<Values> rightStampChunk,
        LongChunk<KeyIndices> rightKeyIndicesChunk, RedirectionIndex redirectionIndex) {
        ensureLeftCapacity(leftIndex.intSize());

        // read the left stamp column
        leftStampSource.fillChunk(leftFillContext, leftStampChunk, leftIndex);
        leftKeyIndicesChunk.setSize(leftIndex.intSize());
        leftIndex.fillKeyIndicesChunk(leftKeyIndicesChunk);

        // sort the left stamp column
        sortKernel.sort(leftKeyIndicesChunk, leftStampChunk);

        // figure out our "merge"
        computeRedirections(redirectionIndex, rightStampChunk, rightKeyIndicesChunk);
    }

    private void computeRedirections(RedirectionIndex redirectionIndex,
        Chunk<Values> rightStampChunk, LongChunk<KeyIndices> rightKeyIndicesChunk) {
        stampKernel.computeRedirections(leftStampChunk, rightStampChunk, rightKeyIndicesChunk,
            leftRedirections);
        for (int ii = 0; ii < leftKeyIndicesChunk.size(); ++ii) {
            final long rightKey = leftRedirections.get(ii);
            // the redirection index defaults to NULL_KEY so we do not need to put it in there
            if (rightKey != Index.NULL_KEY) {
                redirectionIndex.putVoid(leftKeyIndicesChunk.get(ii), rightKey);
            }
        }
    }
}

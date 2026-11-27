//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

/**
 * Makes rows of an array source addressable without allocating every block below them, so that tests can exercise row
 * keys beyond {@link Integer#MAX_VALUE} with a few kilobytes of data.
 */
public final class SparseArraySourceBlocks {
    private SparseArraySourceBlocks() {}

    /**
     * Gives {@code source} a capacity that ends with the block containing {@code row}, allocating only that block. The
     * block array still holds a reference slot for every block below it. Reading or writing a row in any other block
     * fails.
     *
     * @param source a freshly constructed source that does not track previous values
     * @param row a row in the only block to allocate
     */
    public static void allocateOnlyBlockContaining(final LongArraySource source, final long row) {
        final int blockNo = (int) (row >> ArrayBackedColumnSource.LOG_BLOCK_SIZE);
        final long[][] blocks = new long[blockNo + 1][];
        blocks[blockNo] = new long[ArrayBackedColumnSource.BLOCK_SIZE];
        source.resetBlocks(blocks, null);
        source.maxIndex = (((long) blockNo + 1) << ArrayBackedColumnSource.LOG_BLOCK_SIZE) - 1;
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.sparse;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.mutable.MutableLong;

public class SparseConstants {
    public static final int LOG_BLOCK0_SIZE = 23;
    public static final int LOG_BLOCK1_SIZE = 15;
    public static final int LOG_BLOCK2_SIZE = 15;
    public static final int LOG_BLOCK_SIZE = 10; // Must be >= LOG_INUSE_BITSET_SIZE

    public static final int BLOCK0_SHIFT = LOG_BLOCK_SIZE + LOG_BLOCK2_SIZE + LOG_BLOCK1_SIZE;
    public static final int BLOCK1_SHIFT = LOG_BLOCK_SIZE + LOG_BLOCK2_SIZE;
    public static final int BLOCK2_SHIFT = LOG_BLOCK_SIZE;

    public static final long INDEX_MASK = (1 << LOG_BLOCK_SIZE) - 1;
    public static final int BLOCK0_MASK = (1 << LOG_BLOCK0_SIZE) - 1;
    public static final int BLOCK1_MASK = (1 << LOG_BLOCK1_SIZE) - 1;
    public static final int BLOCK2_MASK = (1 << LOG_BLOCK2_SIZE) - 1;

    public static final int BLOCK_SIZE = 1 << LOG_BLOCK_SIZE;
    public static final int BLOCK0_SIZE = 1 << LOG_BLOCK0_SIZE;
    public static final int BLOCK1_SIZE = 1 << LOG_BLOCK1_SIZE;
    public static final int BLOCK2_SIZE = 1 << LOG_BLOCK2_SIZE;

    public static final int LOG_INUSE_BITSET_SIZE = 6;
    public static final int LOG_INUSE_BLOCK_SIZE = LOG_BLOCK_SIZE - LOG_INUSE_BITSET_SIZE;
    public static final int IN_USE_BLOCK_SIZE = 1 << LOG_INUSE_BLOCK_SIZE;
    public static final int IN_USE_MASK = (1 << LOG_INUSE_BITSET_SIZE) - 1;

    /**
     * When using sparse data structures, a single entry within a block can result in the data size increasing by a
     * factor of BLOCK_SIZE. On the other hand, we would strongly prefer the sparse results because they are generally
     * much faster than the alternatives.
     * <p>
     * To balance those concerns, we can determine if a given rowSet exceeds some overhead threshold and act
     * accordingly.
     *
     * @param rowSet the Index that we should calculate the overhead for
     * @param maximumOverhead the maximum overhead as a fraction (e.g. 1.1 is 10% overhead). Values less than zero
     *        disable overhead checking, and result in always using the sparse structure. A value of zero results in
     *        never using the sparse structure.
     */
    public static boolean sparseStructureExceedsOverhead(final RowSet rowSet, final double maximumOverhead) {
        if (maximumOverhead < 0) {
            return false;
        }
        if (maximumOverhead == 0) {
            return true;
        }

        final long requiredBlocks = (rowSet.size() + BLOCK_SIZE - 1) / BLOCK_SIZE;
        final long acceptableBlocks = (long) (maximumOverhead * (double) requiredBlocks);
        final MutableLong lastBlock = new MutableLong(-1L);
        final MutableLong usedBlocks = new MutableLong(0);
        return !rowSet.forEachRowKeyRange((s, e) -> {
            long startBlock = s >> LOG_BLOCK_SIZE;
            final long endBlock = e >> LOG_BLOCK_SIZE;
            final long lb = lastBlock.longValue();
            if (lb >= 0) {
                if (startBlock == lb) {
                    if (startBlock == endBlock) {
                        return true;
                    }
                    startBlock++;
                }
            }
            lastBlock.setValue(endBlock);
            usedBlocks.add(endBlock - startBlock + 1);
            return usedBlocks.longValue() <= acceptableBlocks;
        });
    }
}

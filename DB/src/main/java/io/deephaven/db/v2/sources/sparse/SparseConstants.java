package io.deephaven.db.v2.sources.sparse;

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
}

package io.deephaven.csv.densestorage;

/**
 * Constants that control the behavior of the {@link DenseStorageWriter} and {@link DenseStorageReader}.
 */
public class DenseStorageConstants {
    /**
     * When input strings are less than this threshold, we pack them tightly into a chunk. When they are greater than or
     * equal to this threshold, we allocate them directly as their own individual byte arrays.
     */
    public static final int LARGE_THRESHOLD = 1024;
    /**
     * Size of the "control queue" blocks. Somewhat arbitrary but should be large-ish. We have arbitrarily chosen
     * 100,000 here.
     */
    public static final int CONTROL_QUEUE_SIZE = 100_000;
    /**
     * Size of the "packed" byte blocks. The number chosen in somewhat arbitrary but it should be large-ish (100K? 1M?)
     * for performance and a decent multiple of LARGE_THRESHOLD to avoid wasting too much space at the end of each
     * block. By making it 1024x the size of LARGE_THRESHOLD, we can show that the fraction of wasted space at the end
     * of each block can never be more than (1/1024).
     */
    public static final int PACKED_QUEUE_SIZE = LARGE_THRESHOLD * 1024;
    /**
     * Size of the "array queue". Somewhat arbitrary but should be large-ish. We have arbitrarily chosen 100K here. 10K
     * might also be reasonable.
     */
    public static final int ARRAY_QUEUE_SIZE = 100_000;
    /**
     * This sentinel value is used to indicate that the next value being read is not bytes packed into a byte block but
     * rather its own byte array.
     */
    public static final int LARGE_BYTE_ARRAY_SENTINEL = -1;
}

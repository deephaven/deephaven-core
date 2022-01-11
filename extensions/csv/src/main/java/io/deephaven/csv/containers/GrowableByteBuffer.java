package io.deephaven.csv.containers;

/**
 * This is like TByteArrayList except that you can get at the underlying data buffer and use it for your own purposes,
 * assuming you know what you're doing. We exploit this ability to (temporarily) point our slices at the underlying
 * array while we are processing slices. In terms of expected usage, this class is only used for holding the data for
 * cells, and only when the cell has escaped characters (like escaped quotes) or the cell spans more than one input
 * chunk (in which case we can no longer do the trick where we point a slice directly at the array that buffers our
 * input). Therefore the max size of this data structure is equal to the size of the largest cell in the input (which
 * likely to be in the 10s or 100s of bytes). Since it's expected to be modest in size, we don't worry too much about
 * our growth strategy, which simply involves doubling when we run out of space. In fact, in practice for "normal"
 * input, this object probably never reallocates.
 */
public final class GrowableByteBuffer {
    private static final int INITIAL_BUFFER_SIZE = 1024;

    /**
     * Underlying buffer. Grows as needed.
     */
    private byte[] data = new byte[INITIAL_BUFFER_SIZE];
    /**
     * Current size of the data.
     */
    private int size = 0;

    /**
     * Appends 'srcSize' characters from 'src', starting at 'srcOffset'.
     */
    public void append(byte[] src, int srcOffset, int srcSize) {
        ensure(srcSize);
        System.arraycopy(src, srcOffset, data, size, srcSize);
        size += srcSize;
    }

    /**
     * Ensure that the buffer can hold at least 'additionalSize' items.
     */
    private void ensure(int additionalSize) {
        final int sizeNeeded = Math.addExact(size, additionalSize);
        if (sizeNeeded <= data.length) {
            return;
        }

        // Ensuring that we always at least double the buffer, but we may not always
        // follow powers of two
        final int newSize = Math.max(sizeNeeded, Math.multiplyExact(size, 2));
        final byte[] newData = new byte[newSize];
        System.arraycopy(data, 0, newData, 0, size);
        data = newData;
    }

    /**
     * Clear the buffer.
     */
    public void clear() {
        size = 0;
    }

    /**
     * Access the underlying data array.
     */
    public byte[] data() {
        return data;
    }

    /**
     * The current size.
     */
    public int size() {
        return size;
    }
}

package io.deephaven.csv.containers;

/**
 * An object that represents a slice of byte data. This object is intended to be reusable.
 */
public final class ByteSlice {
    /**
     * The underlying data.
     */
    private byte[] data;
    /**
     * The index of the first data element.
     */
    private int begin;
    /**
     * The index that is one past the last data element.
     */
    private int end;

    /**
     * Make an empty ByteSlice with a null underlying array.
     */
    public ByteSlice() {}

    /**
     * Constructs a ByteSlice from the half-open interval [{@code begin}, {@code end}) of the array {@code data}.
     */
    public ByteSlice(final byte[] data, final int begin, final int end) {
        reset(data, begin, end);
    }

    /**
     * Reset the ByteSlice to the half-open interval [{@code begin}, {@code end}) of the array {@code data}.
     */
    public void reset(final byte[] data, final int begin, final int end) {
        this.data = data;
        this.begin = begin;
        this.end = end;
    }

    /**
     * Copies the slice to the destination array, starting at the specified destination position.
     */
    public void copyTo(byte[] dest, int destOffset) {
        System.arraycopy(data, begin, dest, destOffset, end - begin);
    }

    /**
     * Gets the 'begin' field of the slice
     */
    public int begin() {
        return begin;
    }

    /**
     * Gets the 'end' field of the slice.
     */
    public int end() {
        return end;
    }

    /**
     * Sets the 'begin' field of the slice.
     */
    public void setBegin(int begin) {
        this.begin = begin;
    }

    /**
     * Sets the 'end' field of the slice.
     */
    public void setEnd(int end) {
        this.end = end;
    }

    /**
     * Gets the first character of the slice. The behavior is unspecified if the slice is empty.
     */
    public byte front() {
        return data[begin];
    }

    /**
     * Gets the last character of the slice. The behavior is unspecified if the slice is empty.
     */
    public byte back() {
        return data[end - 1];
    }

    /**
     * Gets the underlying array from the slice.
     */
    public byte[] data() {
        return data;
    }

    /**
     * Gets the size of the slice.
     */
    public int size() {
        return end - begin;
    }

    @Override
    public String toString() {
        final int size = end - begin;
        return size == 0 ? "" : new String(data, begin, end - begin);
    }
}

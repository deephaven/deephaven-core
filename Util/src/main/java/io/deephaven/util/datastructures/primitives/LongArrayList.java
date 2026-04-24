//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.primitives;

import io.deephaven.base.verify.Require;
import io.deephaven.util.type.ArrayTypeUtils;

/**
 * Limited implementation of a growable/shrinkable List-like structure that holds longs, based roughly on the
 * capabilities of fastUtils IntLongList and trove's TLongArrayList.
 */
public class LongArrayList {
    private long[] arr = ArrayTypeUtils.EMPTY_LONG_ARRAY;
    private int size;

    public int size() {
        return size;
    }

    public long getLong(int position) {
        return arr[position];
    }

    public void set(int position, long value) {
        arr[position] = value;
    }

    public void add(long value) {
        if (size == arr.length) {
            int newSize = arr.length * 2;
            if (newSize == 0) {
                newSize = 1;
            }
            long[] newArr = new long[newSize];
            System.arraycopy(arr, 0, newArr, 0, arr.length);
            arr = newArr;
        }
        arr[size] = value;
        size++;
    }

    /**
     * At this time, limited to only removing trailing elements.
     *
     * @param from the start position (inclusive)
     * @param to the end position (exclusive)
     */
    public void removeElements(final int from, final int to) {
        Require.eq(to, "to", size(), "size()");
        Require.lt(from, "from", size(), "size()");
        Require.geq(from, "from", 0);
        size = from;
    }

    /**
     * At this time, limited to only removing the last element.
     * 
     * @param position the position of the element to remove
     */
    public void removeLong(int position) {
        Require.eq(position, "position", size() - 1, "size() - 1");
        size--;
    }

    public void clear() {
        size = 0;
    }
}

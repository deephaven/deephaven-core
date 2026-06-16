//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharArrayList and run "./gradlew replicatePrimitiveArrayLists" to regenerate
//
// @formatter:off
package io.deephaven.util.datastructures.list;

import io.deephaven.base.verify.Require;
import io.deephaven.util.compare.IntComparisons;
import io.deephaven.util.type.ArrayTypeUtils;

/**
 * Limited implementation of a growable/shrinkable List-like structure that holds ints.
 */
public class IntArrayList {
    private int[] arr = ArrayTypeUtils.EMPTY_INT_ARRAY;
    private int size;

    public int size() {
        return size;
    }

    public int getInt(int position) {
        return arr[position];
    }

    public void set(int position, int value) {
        arr[position] = value;
    }

    public void add(int value) {
        if (size == arr.length) {
            int newSize = arr.length * 2;
            if (newSize == 0) {
                newSize = 1;
            }
            int[] newArr = new int[newSize];
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
    public void removeInt(int position) {
        Require.eq(position, "position", size() - 1, "size() - 1");
        size--;
    }

    public void clear() {
        size = 0;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof IntArrayList)) {
            return false;
        }
        final IntArrayList that = (IntArrayList) other;
        if (size != that.size) {
            return false;
        }
        for (int i = 0; i < size; ++i) {
            if (arr[i] != that.arr[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = 1;
        for (int i = 0; i < size; ++i) {
            result = 31 * result + IntComparisons.hashCode(arr[i]);
        }
        return result;
    }
}

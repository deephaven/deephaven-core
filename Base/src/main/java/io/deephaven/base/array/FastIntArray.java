//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.base.array;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.Copyable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class FastIntArray implements Externalizable, Copyable<FastIntArray> {
    private static final int[] EMPTY = new int[0];

    private int length;
    private int[] array;

    public FastIntArray() {
        this(0);
    }

    public FastIntArray(int initialSize) {
        this.length = 0;
        this.array = initialSize > 0 ? new int[initialSize] : EMPTY;
    }

    public FastIntArray(int[] initValues) {
        this(initValues.length);
        add(initValues, 0, initValues.length);
    }

    public void add(int t) {
        array = ArrayUtil.put(array, length, t);
        ++length;
    }

    public void add(int[] t, int startIndex, int len) {
        array = ArrayUtil.put(array, length, t, startIndex, len);
        length += len;
    }

    public void quickReset() {
        length = 0;
    }

    public void normalReset(int resetValue) {
        for (int i = 0; i < length; ++i) {
            array[i] = resetValue;
        }
        length = 0;
    }

    public void fullReset(int resetValue) {
        for (int i = 0; i < array.length; ++i) {
            array[i] = resetValue;
        }
        length = 0;
    }

    public void arrayReset() {
        length = 0;
        array = new int[0];
    }

    public int getLength() {
        return length;
    }

    public int[] getUnsafeArray() {
        return array;
    }

    public void removeThisIndex(int index) {
        if (index >= length) {
            throw new IllegalArgumentException(
                    "you tried to remove this index: " + index + " when the array is only this long: " + length);
        } else if (index < 0) {
            throw new IllegalArgumentException(
                    "you tried to remove this index: " + index + " when we can only remove positive indices");
        } else {
            // move all the items ahead one index and reduce the length
            for (int i = index; i < length; i++) {
                array[i] = array[i + 1];
            }
            length--;
        }
    }

    @Override
    public void copyValues(final FastIntArray other) {
        if (other != this) {
            length = other.length;
            array = ArrayUtil.ensureSizeNoCopy(array, length);
            System.arraycopy(other.array, 0, array, 0, length);
        }
    }

    @Override
    public FastIntArray clone() {
        FastIntArray clone = new FastIntArray();
        clone.copyValues(this);
        return clone;
    }

    @Override
    public FastIntArray safeClone() {
        return clone();
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {
        out.writeInt(length);
        for (int i = 0; i < length; ++i) {
            out.writeInt(array[i]);
        }
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        quickReset();
        final int len = in.readInt();
        for (int i = 0; i < len; ++i) {
            add(in.readInt());
        }
    }

    @Override
    public String toString() {
        return toStringXml("");
    }

    public String toStringXml(String pre) {
        StringBuilder msg = new StringBuilder();
        String extra = "   ";
        msg.append(pre).append("<FastIntArray>\n");
        for (int i = 0; i < array.length; i++) {
            msg.append(pre).append(extra).append("<index>").append(i).append("</index><length>")
                    .append(length).append("</length><entry>").append(array[i]).append("</entry>\n");
        }
        msg.append(pre).append("</FastIntArray>\n");
        return msg.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof FastIntArray))
            return false;

        FastIntArray that = (FastIntArray) o;

        if (length != that.length)
            return false;

        // here we only care about the items in the array before "length"
        for (int i = 0; i < length; i++) {
            if (this.array[i] != that.array[i]) {
                return false;
            }
        }

        return true;
    }

}

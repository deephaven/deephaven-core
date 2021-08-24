/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.array;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.Copyable;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

public class FastCharArray implements Externalizable, Copyable<FastCharArray> {
    private static final char[] EMPTY = new char[0];

    private int length;
    private char[] array;

    public FastCharArray() {
        this(0);
    }

    public FastCharArray(int initialSize) {
        this.length = 0;
        this.array = initialSize > 0 ? new char[initialSize] : EMPTY;
    }

    public FastCharArray(char[] initValues) {
        this();
        add(initValues, 0, initValues.length);
    }

    public void add(char t) {
        array = ArrayUtil.put(array, length, t);
        ++length;
    }

    public void add(char[] t, int startIndex, int len) {
        array = ArrayUtil.put(array, length, t, startIndex, len);
        length += len;
    }

    public void quickReset() {
        length = 0;
    }

    public void normalReset(char resetValue) {
        for (int i = 0; i < length; ++i) {
            array[i] = resetValue;
        }
        length = 0;
    }

    public void fullReset(char resetValue) {
        for (int i = 0; i < array.length; ++i) {
            array[i] = resetValue;
        }
        length = 0;
    }

    public void arrayReset() {
        length = 0;
        array = new char[0];
    }

    public int getLength() {
        return length;
    }

    public char[] getUnsafeArray() {
        return array;
    }

    public void removeThisIndex(int index) {
        if (index >= length) {
            throw new IllegalArgumentException("you tried to remove this index: " + index
                + " when the array is only this long: " + length);
        } else if (index < 0) {
            throw new IllegalArgumentException("you tried to remove this index: " + index
                + " when we can only remove positive indices");
        } else {
            // move all the items ahead one index and reduce the length
            for (int i = index; i < length; i++) {
                array[i] = array[i + 1];
            }
            length--;
        }
    }

    @Override
    public void copyValues(final FastCharArray other) {
        if (other != this) {
            length = other.length;
            array = ArrayUtil.ensureSizeNoCopy(array, length);
            System.arraycopy(other.array, 0, array, 0, length);
        }
    }

    @Override
    public FastCharArray clone() {
        FastCharArray clone = new FastCharArray();
        clone.copyValues(this);
        return clone;
    }

    @Override
    public FastCharArray safeClone() {
        return clone();
    }

    @Override
    public void writeExternal(final ObjectOutput out) throws IOException {
        out.writeInt(length);
        for (int i = 0; i < length; ++i) {
            out.writeChar(array[i]);
        }
    }

    @Override
    public void readExternal(final ObjectInput in) throws IOException, ClassNotFoundException {
        quickReset();
        final int len = in.readInt();
        for (int i = 0; i < len; ++i) {
            add(in.readChar());
        }
    }

    @Override
    public String toString() {
        return toStringXml("");
    }

    public String toStringXml(String pre) {
        StringBuilder msg = new StringBuilder();
        String extra = "   ";
        msg.append(pre).append("<FastCharArray>\n");
        for (int i = 0; i < array.length; i++) {
            msg.append(pre).append(extra).append("<index>").append(i).append("</index><length>")
                .append(length).append("</length><entry>").append(array[i]).append("</entry>\n");
        }
        msg.append(pre).append("</FastCharArray>\n");
        return msg.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof FastCharArray))
            return false;

        FastCharArray that = (FastCharArray) o;

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

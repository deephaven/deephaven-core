/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.map;


import io.deephaven.base.Copyable;
import io.deephaven.base.Function;
import io.deephaven.base.array.FastArray;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;

public class FastArrayMapLongToObject<V extends Externalizable & Copyable<V>>
        implements Copyable<FastArrayMapLongToObject<V>> {

    private FastArray<KeyValuePairLongToObject<V>> array;

    public FastArrayMapLongToObject() {}

    public FastArrayMapLongToObject(final Function.Nullary<? extends KeyValuePairLongToObject<V>> newInstance) {
        array = new FastArray<KeyValuePairLongToObject<V>>(newInstance);
    }

    public FastArray<KeyValuePairLongToObject<V>> getArray() {
        return array;
    }

    public int size() {
        return array.getLength();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    private int findThisKeyIndex(long key) {
        // todo write this to be faster with binary search
        for (int i = 0; i < array.getLength(); i++) {
            if (array.getUnsafeArray()[i].getKey() == key) {
                return i;
            }
        }
        return -1; // could not find your key
    }

    public boolean containsKey(long key) {
        return findThisKeyIndex(key) >= 0;
    }

    public V get(long key) {
        int index = findThisKeyIndex(key);
        if (index < 0) {
            return null;
        } else {
            return array.getUnsafeArray()[index].getValue();
        }
    }

    public V put(long key, V value) {
        int index = findThisKeyIndex(key);
        if (index >= 0) {
            V oldValue = array.getUnsafeArray()[index].getValue();
            array.getUnsafeArray()[index].setValue(value);
            return oldValue;
        } else {
            KeyValuePairLongToObject<V> newItem = new KeyValuePairLongToObject<V>(key, value); // ALLOCATION
            insertNewlyAllocatedItemIntoArray(newItem);
            return null;
        }
    }

    private void insertNewlyAllocatedItemIntoArray(KeyValuePairLongToObject<V> item) {
        array.add(item);
        sortArray();
    }

    private void sortArray() {
        Arrays.sort(array.getUnsafeArray(), 0, array.getLength());
    }

    public V remove(long key) {
        int index = findThisKeyIndex(key);
        if (index >= 0) {
            V oldValue = array.getUnsafeArray()[index].getValue();
            array.removeThisIndex(index);
            // this just moves all the values ahead one slot so we should still be sorted still
            return oldValue;
        } else {
            return null;
        }
    }

    public void clear() {
        array.quickReset();
    }

    public FastArrayMapLongToObject<V> cloneDeep() {
        FastArrayMapLongToObject<V> copy = new FastArrayMapLongToObject<V>();
        copy.array = FastArray.cloneDeep(this.getArray());
        return copy;
    }


    public static <V extends Externalizable & Copyable<V>> void writeExternal(final FastArrayMapLongToObject<V> THIS,
            ObjectOutput out,
            FastArray.WriteExternalFunction<KeyValuePairLongToObject<V>> writeExternalFunction) throws IOException {
        if (THIS == null) {
            throw new IllegalArgumentException(
                    "FastArrayMapLongToObject.writeExternal(): THIS was null and is not supported");
        }
        out.writeInt(THIS.array.getLength());
        for (int i = 0; i < THIS.array.getLength(); ++i) {
            writeExternalFunction.writeExternal(out, THIS.array.getUnsafeArray()[i]);
        }
    }

    public static <V extends Externalizable & Copyable<V>> void readExternal(final FastArrayMapLongToObject<V> THIS,
            ObjectInput in,
            FastArray.ReadExternalFunction<KeyValuePairLongToObject<V>> readExternalFunction)
            throws IOException, ClassNotFoundException {
        if (THIS == null) {
            throw new IllegalArgumentException(
                    "FastArrayMapLongToObject.readExternal(): THIS was null and is not supported");
        }
        THIS.array.quickReset();
        final int len = in.readInt();
        for (int i = 0; i < len; ++i) {
            KeyValuePairLongToObject<V> nextKeyValuePair = THIS.array.next();
            readExternalFunction.readExternal(in, nextKeyValuePair);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof FastArrayMapLongToObject))
            return false;

        FastArrayMapLongToObject that = (FastArrayMapLongToObject) o;

        if (array != null ? !array.equals(that.array) : that.array != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        return array != null ? array.hashCode() : 0;
    }

    @Override
    public String toString() {
        return toStringXml("");
    }

    public String toStringXml(String pre) {
        StringBuilder msg = new StringBuilder();
        String extra = "   ";
        msg.append(pre).append("<FastArrayMapLongToObject>\n");
        msg.append(pre).append(extra).append("<array>\n");
        msg.append(array.toStringXml(pre + extra));
        msg.append(pre).append(extra).append("</array>\n");
        msg.append(pre).append("</FastArrayMapLongToObject>\n");
        return msg.toString();
    }

    @Override
    public void copyValues(FastArrayMapLongToObject<V> other) {
        FastArray.copyValuesDeep(this.array, other.array);
    }

    @Override
    public FastArrayMapLongToObject<V> safeClone() {
        FastArrayMapLongToObject<V> result = new FastArrayMapLongToObject<V>(array.getNewInstance());
        for (int i = 0; i < array.getLength(); i++) {
            KeyValuePairLongToObject<V> pair = array.getUnsafeArray()[i];
            result.put(pair.getKey(), pair.getValue());
        }
        return result;
    }
}


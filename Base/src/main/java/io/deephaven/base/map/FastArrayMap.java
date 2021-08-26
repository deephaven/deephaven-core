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

public class FastArrayMap<K extends Comparable<K> & Externalizable & Copyable<K>, V extends Externalizable & Copyable<V>>
        implements Copyable<FastArrayMap<K, V>> {

    private FastArray<KeyValuePair<K, V>> array;

    public FastArrayMap() {}

    public FastArrayMap(final Function.Nullary<? extends KeyValuePair<K, V>> newInstance) {
        array = new FastArray<KeyValuePair<K, V>>(newInstance);
    }

    public FastArray<KeyValuePair<K, V>> getArray() {
        return array;
    }

    public int size() {
        return array.getLength();
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    private int findThisKeyIndex(K key) {
        // todo write this to be faster with binary search
        for (int i = 0; i < array.getLength(); i++) {
            if (array.getUnsafeArray()[i].getKey().equals(key)) {
                return i;
            }
        }
        return -1; // could not find your key
    }

    public boolean containsKey(K key) {
        return findThisKeyIndex(key) >= 0;
    }

    public V get(K key) {
        int index = findThisKeyIndex(key);
        if (index < 0) {
            return null;
        } else {
            return array.getUnsafeArray()[index].getValue();
        }
    }

    public V put(K key, V value) {
        int index = findThisKeyIndex(key);
        if (index >= 0) {
            V oldValue = array.getUnsafeArray()[index].getValue();
            array.getUnsafeArray()[index].setValue(value);
            return oldValue;
        } else {
            KeyValuePair<K, V> newItem = new KeyValuePair<K, V>(key, value); // ALLOCATION
            insertNewlyAllocatedItemIntoArray(newItem);
            return null;
        }
    }

    private void insertNewlyAllocatedItemIntoArray(KeyValuePair<K, V> item) {
        array.add(item);
        sortArray();
    }

    private void sortArray() {
        Arrays.sort(array.getUnsafeArray(), 0, array.getLength());
    }

    public V remove(K key) {
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

    public FastArrayMap<K, V> cloneDeep() {
        FastArrayMap<K, V> copy = new FastArrayMap<K, V>();
        copy.array = FastArray.cloneDeep(this.getArray());
        return copy;
    }

    @Override
    public void copyValues(FastArrayMap<K, V> other) {
        FastArray.copyValuesDeep(this.array, other.array);
    }

    @Override
    public FastArrayMap<K, V> safeClone() {
        FastArrayMap<K, V> result = new FastArrayMap<K, V>(array.getNewInstance());
        result.array = FastArray.cloneDeep(array);
        return result;
    }

    public static <K extends Externalizable & Comparable<K> & Copyable<K>, V extends Externalizable & Copyable<V>> void writeExternal(
            final FastArrayMap<K, V> THIS, ObjectOutput out,
            FastArray.WriteExternalFunction<KeyValuePair<K, V>> writeExternalFunction) throws IOException {
        if (THIS == null) {
            throw new IllegalArgumentException("FastArray.writeExternal(): THIS was null and is not supported");
        }
        out.writeInt(THIS.array.getLength());
        for (int i = 0; i < THIS.array.getLength(); ++i) {
            writeExternalFunction.writeExternal(out, THIS.array.getUnsafeArray()[i]);
        }
    }

    public static <K extends Externalizable & Comparable<K> & Copyable<K>, V extends Externalizable & Copyable<V>> void readExternal(
            final FastArrayMap<K, V> THIS, ObjectInput in,
            FastArray.ReadExternalFunction<KeyValuePair<K, V>> readExternalFunction)
            throws IOException, ClassNotFoundException {
        if (THIS == null) {
            throw new IllegalArgumentException("FastArray.readExternal(): THIS was null and is not supported");
        }
        THIS.array.quickReset();
        final int len = in.readInt();
        for (int i = 0; i < len; ++i) {
            KeyValuePair<K, V> nextKeyValuePair = THIS.array.next();
            readExternalFunction.readExternal(in, nextKeyValuePair);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof FastArrayMap))
            return false;

        FastArrayMap that = (FastArrayMap) o;

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
        msg.append(pre).append("<FastArrayMap>\n");
        msg.append(pre).append(extra).append("<array>\n");
        msg.append(array.toStringXml(pre + extra));
        msg.append(pre).append(extra).append("</array>\n");
        msg.append(pre).append("</FastArrayMap>\n");
        return msg.toString();
    }

}


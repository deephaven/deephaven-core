/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.map;

import io.deephaven.base.Copyable;

import java.io.Externalizable;

public class KeyValuePairLongToObject<V extends Externalizable & Copyable<V>>
    implements Comparable<KeyValuePairLongToObject<V>>, Copyable<KeyValuePairLongToObject<V>> {

    private long key;
    private V value;

    public KeyValuePairLongToObject() {}

    public KeyValuePairLongToObject(long key, V value) {
        if (value == null) {
            throw new IllegalArgumentException("value can not be null");
        }
        this.key = key;
        this.value = value;
    }

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public V getValue() {
        return value;
    }

    public void setValue(V value) {
        this.value = value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof KeyValuePairLongToObject))
            return false;

        KeyValuePairLongToObject that = (KeyValuePairLongToObject) o;

        if (key != that.key)
            return false;
        if (value != null ? !value.equals(that.value) : that.value != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (key ^ (key >>> 32));
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(KeyValuePairLongToObject<V> o) {
        if (key < o.key) {
            return -1;
        } else if (key > o.key) {
            return 1;
        } else {
            return 0;
        }
    }

    @Override
    public String toString() {
        return "KeyValuePair: " + key + " " + value;
    }

    @Override
    public void copyValues(KeyValuePairLongToObject<V> other) {
        this.key = other.key;
        this.value.copyValues(other.getValue());
    }

    @Override
    public Object clone() {
        throw new UnsupportedOperationException("You should not be calling this method");
    }

    @Override
    public KeyValuePairLongToObject<V> safeClone() {
        return new KeyValuePairLongToObject<V>(key, value.safeClone());
    }
}

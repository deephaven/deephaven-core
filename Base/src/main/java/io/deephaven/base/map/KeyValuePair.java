/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.map;

import io.deephaven.base.Copyable;

import java.io.Externalizable;

public class KeyValuePair<K extends Externalizable & Comparable<K> & Copyable<K>, V extends Externalizable & Copyable<V>>
        implements Comparable<KeyValuePair<K, V>>, Copyable<KeyValuePair<K, V>> {

    private K key;
    private V value;

    public KeyValuePair() {}

    public KeyValuePair(K key, V value) {
        if (key == null) {
            throw new IllegalArgumentException("key can not be null");
        }
        if (value == null) {
            throw new IllegalArgumentException("value can not be null");
        }
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public void setKey(K key) {
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
        if (!(o instanceof KeyValuePair))
            return false;

        KeyValuePair that = (KeyValuePair) o;

        if (key != null ? !key.equals(that.key) : that.key != null)
            return false;
        if (value != null ? !value.equals(that.value) : that.value != null)
            return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = key != null ? key.hashCode() : 0;
        result = 31 * result + (value != null ? value.hashCode() : 0);
        return result;
    }

    @Override
    public int compareTo(KeyValuePair<K, V> o) {
        return key.compareTo(o.key);
    }

    @Override
    public String toString() {
        return "KeyValuePair: " + key + " " + value;
    }

    @Override
    public void copyValues(KeyValuePair<K, V> other) {
        if (other.key == null) {
            this.key = null;
        } else if (this.key == null) {
            this.key = other.key.safeClone();
        } else {
            this.key.copyValues(other.key);
        }


        if (other.value == null) {
            this.value = null;
        } else if (this.value == null) {
            this.value = other.value.safeClone();
        } else {
            this.value.copyValues(other.value);
        }
    }

    @Override
    public Object clone() {
        throw new UnsupportedOperationException("You should not be calling this method");
    }

    @Override
    public KeyValuePair<K, V> safeClone() {
        return new KeyValuePair<K, V>(key.safeClone(), value.safeClone());
    }
}

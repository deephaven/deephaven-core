/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables;

import io.deephaven.db.tables.libs.StringSet;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.ToIntFunction;

/**
 * Array-backed {@link StringSet} implementation.
 */
public class StringSetArrayWrapper implements StringSet, Serializable {

    private final String[] innerSet;

    public StringSetArrayWrapper(String... values) {
        Arrays.sort(values);
        innerSet = values;
    }

    @Override
    public boolean contains(String value) {
        for (int i = 0; i < innerSet.length; i++) {
            if (value.equals(innerSet[i])) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsAny(String... values) {
        for (int i = 0; i < values.length; i++) {
            if (contains(values[i])) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsAll(String... values) {
        for (int i = 0; i < values.length; i++) {
            if (!contains(values[i])) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int size() {
        return innerSet.length;
    }

    @Override
    public String[] values() {
        return innerSet;
    }

    @Override
    public String[] sortedValues() {
        return values();
    }

    @Override
    public boolean isEmpty() {
        return innerSet.length == 0;
    }

    private ToIntFunction<String> lastToOffset;
    private long lastEncoding;

    @Override
    public synchronized long getEncoding(@NotNull final ToIntFunction<String> toOffset) {
        if (lastToOffset == toOffset) {
            return lastEncoding;
        }
        long encoding = 0;
        for (int i = 0; i < innerSet.length; i++) {
            String s = innerSet[i];
            final int key = toOffset.applyAsInt(s);
            if (key > 63) {
                throw new RuntimeException(
                        "Symbol " + s + " exceeds the limit of 63 symbols for StringSetArrayWrapper");
            }
            encoding |= 1L << key;
        }
        lastEncoding = encoding;
        lastToOffset = toOffset;
        return encoding;
    }

    @Override
    public final String toString() {
        return defaultToString();
    }

    @Override
    public final int hashCode() {
        return defaultHashCode();
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public final boolean equals(final Object other) {
        return defaultEquals(other);
    }

    @Override
    public Iterator<String> iterator() {
        return Arrays.asList(innerSet).iterator();
    }
}

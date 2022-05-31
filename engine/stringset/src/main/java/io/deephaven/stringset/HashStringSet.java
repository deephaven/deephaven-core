/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.stringset;

import io.deephaven.datastructures.util.CollectionUtil;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.*;
import java.util.function.ToIntFunction;

/**
 * {@link HashSet}-backed {@link StringSet} implementation.
 */
public class HashStringSet implements StringSet, Serializable {

    private static final long serialVersionUID = 6033718768047284093L;
    private Set<String> innerSet;

    public HashStringSet(String[] values, int start, int length) {
        innerSet = new HashSet<>(length);
        while (length-- > 0) {
            innerSet.add(values[start++]);
        }
    }

    public HashStringSet(String... values) {
        innerSet = new HashSet<>(values.length);
        innerSet.addAll(Arrays.asList(values));
    }

    public HashStringSet(Collection<String> values) {
        innerSet = new HashSet<>(values);
    }

    public HashStringSet(int initialCapacity) {
        innerSet = new HashSet<>(initialCapacity);
    }

    public void addStringToSet(String val) {
        innerSet.add(val);
    }

    @Override
    public boolean contains(String value) {
        return innerSet.contains(value);
    }

    @Override
    public boolean containsAny(String... values) {
        for (String value : values) {
            if (contains(value)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public boolean containsAll(String... values) {
        for (String value : values) {
            if (!contains(value)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int size() {
        return innerSet.size();
    }

    @Override
    public String[] values() {
        return innerSet.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
    }

    @Override
    public String[] sortedValues() {
        final String[] values = values();
        Arrays.sort(values);
        return values;
    }

    @Override
    public boolean isEmpty() {
        return innerSet.isEmpty();
    }

    @Override
    public long getEncoding(@NotNull final ToIntFunction<String> toOffset) {
        long encoding = 0;
        for (String s : this) {
            final int key = toOffset.applyAsInt(s);
            if (key > 63) {
                throw new RuntimeException("Symbol " + s + " exceeds the limit of 63 symbols for HashStringSet");
            }
            encoding |= 1L << key;
        }
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
    @NotNull
    public Iterator<String> iterator() {
        return innerSet.iterator();
    }
}

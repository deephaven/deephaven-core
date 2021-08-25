/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.libs;

import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.function.ToIntFunction;

/**
 * Interface for sets of Strings that can be mapped to a 64-bit long bitmap.
 */
public interface StringSet extends Iterable<String> {

    boolean contains(String value);

    boolean containsAny(String... values);

    boolean containsAll(String... values);

    int size();

    String[] values();

    /**
     * Get a sorted array of the values in this StringSet. May or may not match the value returned
     * by {@link #values()}.
     *
     * @return A sorted array of this StringSet's values
     */
    String[] sortedValues();

    boolean isEmpty();

    long getEncoding(@NotNull ToIntFunction<String> toOffset);

    default String defaultToString() {
        return Arrays.toString(sortedValues());
    }

    default int defaultHashCode() {
        return Arrays.hashCode(sortedValues());
    }

    default boolean defaultEquals(final Object other) {
        if (this == other) {
            return true;
        }
        if (other == null) {
            return false;
        }
        if (!StringSet.class.isAssignableFrom(other.getClass())) {
            return false;
        }
        return Arrays.equals(sortedValues(), ((StringSet) other).sortedValues());
    }
}

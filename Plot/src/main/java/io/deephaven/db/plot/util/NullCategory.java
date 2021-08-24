package io.deephaven.db.plot.util;

import org.jetbrains.annotations.NotNull;

/**
 * Null category implementation.
 */
public final class NullCategory implements Comparable {

    public static final NullCategory INSTANCE = new NullCategory();

    private NullCategory() {}

    @Override
    public int compareTo(@NotNull Object o) {
        return o instanceof NullCategory ? 0 : 1;
    }

    @Override
    public int hashCode() {
        return 0;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof NullCategory;
    }

    @Override
    public String toString() {
        return "(null)";
    }
}

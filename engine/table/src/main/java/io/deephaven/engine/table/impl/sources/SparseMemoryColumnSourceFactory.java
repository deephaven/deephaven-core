//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.WritableColumnSource;
import org.jetbrains.annotations.Nullable;

import java.time.Instant;

/**
 * Creates the in-memory column sources that select and update write at the row keys of their results.
 * <p>
 * The sources are sparse array sources, such as {@link DoubleAdaptiveSparseArraySource}. When {@link #USE_ADAPTIVE} is
 * set, as it is by default, they hold a leaf with few values in a compact leaf, with the default thresholds; otherwise
 * they are the sources of {@link SparseArrayColumnSource#getSparseMemoryColumnSource(long, Class, Class)}, whose leaves
 * are full blocks.
 */
public final class SparseMemoryColumnSourceFactory {

    /**
     * Whether to create sparse sources with compact leaves. Configured by the
     * {@code SparseMemoryColumnSourceFactory.adaptive} property, which defaults to true; false makes every leaf a full
     * block.
     */
    public static boolean USE_ADAPTIVE = Configuration.getInstance()
            .getBooleanWithDefault("SparseMemoryColumnSourceFactory.adaptive", true);

    private SparseMemoryColumnSourceFactory() {}

    /**
     * @param size the number of rows to make room for
     * @param type the type of the values
     * @param componentType the component type of the values, if they are arrays or vectors
     * @return an empty column source for values of {@code type}
     */
    public static <T> WritableColumnSource<T> getMemoryColumnSource(
            final long size,
            final Class<T> type,
            @Nullable final Class<?> componentType) {
        if (!USE_ADAPTIVE) {
            return SparseArrayColumnSource.getSparseMemoryColumnSource(size, type, componentType);
        }
        final int sortedThreshold = CharacterAdaptiveSparseArraySource.DEFAULT_SORTED_THRESHOLD;
        final int compactThreshold = CharacterAdaptiveSparseArraySource.DEFAULT_COMPACT_THRESHOLD;
        final WritableColumnSource<?> result;
        if (type == byte.class || type == Byte.class) {
            result = new ByteAdaptiveSparseArraySource(sortedThreshold, compactThreshold);
        } else if (type == char.class || type == Character.class) {
            result = new CharacterAdaptiveSparseArraySource(sortedThreshold, compactThreshold);
        } else if (type == double.class || type == Double.class) {
            result = new DoubleAdaptiveSparseArraySource(sortedThreshold, compactThreshold);
        } else if (type == float.class || type == Float.class) {
            result = new FloatAdaptiveSparseArraySource(sortedThreshold, compactThreshold);
        } else if (type == int.class || type == Integer.class) {
            result = new IntegerAdaptiveSparseArraySource(sortedThreshold, compactThreshold);
        } else if (type == long.class || type == Long.class) {
            result = new LongAdaptiveSparseArraySource(sortedThreshold, compactThreshold);
        } else if (type == short.class || type == Short.class) {
            result = new ShortAdaptiveSparseArraySource(sortedThreshold, compactThreshold);
        } else if (type == boolean.class || type == Boolean.class) {
            result = new BooleanAdaptiveSparseArraySource(
                    new ByteAdaptiveSparseArraySource(sortedThreshold, compactThreshold));
        } else if (type == Instant.class) {
            result = new InstantAdaptiveSparseArraySource(
                    new LongAdaptiveSparseArraySource(sortedThreshold, compactThreshold));
        } else {
            result = new ObjectAdaptiveSparseArraySource<>(type, componentType, sortedThreshold, compactThreshold);
        }
        // noinspection unchecked
        return (WritableColumnSource<T>) result;
    }

    /**
     * @param size the number of rows to make room for
     * @param type the type of the values
     * @return an empty column source for values of {@code type}
     */
    public static <T> WritableColumnSource<T> getMemoryColumnSource(final long size, final Class<T> type) {
        return getMemoryColumnSource(size, type, null);
    }
}

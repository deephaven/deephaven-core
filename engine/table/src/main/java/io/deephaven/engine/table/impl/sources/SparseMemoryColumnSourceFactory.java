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
 * When {@link #USE_ADAPTIVE} is set, as it is by default, the sources are adaptive sparse array sources, such as
 * {@link DoubleAdaptiveSparseArraySource}, that hold a leaf with few values in a compact leaf, with the default
 * thresholds. Otherwise they are sparse array sources, such as {@link DoubleSparseArraySource}, that hold every leaf as
 * a full block.
 */
public final class SparseMemoryColumnSourceFactory {

    /**
     * Whether sparse sources adapt to the row keys they hold. Configured by the
     * {@code SparseMemoryColumnSourceFactory.adaptive} property, which defaults to true. When true, the sources this
     * factory creates are adaptive sparse array sources with compact leaves, and the static factory methods of
     * {@link SparseArrayColumnSource} create adaptive sparse array sources whose leaves are full blocks. When false,
     * both create sparse array sources, such as {@link DoubleSparseArraySource}, whose page table levels hold one child
     * in their own fields or every child in an array over every child index, and whose leaves are full blocks.
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
        final WritableColumnSource<?> result;
        if (type == byte.class || type == Byte.class) {
            result = new ByteAdaptiveSparseArraySource();
        } else if (type == char.class || type == Character.class) {
            result = new CharacterAdaptiveSparseArraySource();
        } else if (type == double.class || type == Double.class) {
            result = new DoubleAdaptiveSparseArraySource();
        } else if (type == float.class || type == Float.class) {
            result = new FloatAdaptiveSparseArraySource();
        } else if (type == int.class || type == Integer.class) {
            result = new IntegerAdaptiveSparseArraySource();
        } else if (type == long.class || type == Long.class) {
            result = new LongAdaptiveSparseArraySource();
        } else if (type == short.class || type == Short.class) {
            result = new ShortAdaptiveSparseArraySource();
        } else if (type == boolean.class || type == Boolean.class) {
            result = new BooleanAdaptiveSparseArraySource();
        } else if (type == Instant.class) {
            result = new InstantAdaptiveSparseArraySource();
        } else {
            result = new ObjectAdaptiveSparseArraySource<>(type, componentType);
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

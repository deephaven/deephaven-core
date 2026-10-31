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
 * The sources are the adaptive sparse sources, such as {@link DoubleAdaptiveSparseArraySource}, when
 * {@link #USE_ADAPTIVE} is set, and the sources of
 * {@link SparseArrayColumnSource#getSparseMemoryColumnSource(long, Class, Class)} otherwise.
 */
public final class SparseMemoryColumnSourceFactory {

    /**
     * Whether to create adaptive sparse sources. Configured by the {@code SparseMemoryColumnSourceFactory.adaptive}
     * property, which defaults to false.
     */
    public static boolean USE_ADAPTIVE = Configuration.getInstance()
            .getBooleanWithDefault("SparseMemoryColumnSourceFactory.adaptive", false);

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

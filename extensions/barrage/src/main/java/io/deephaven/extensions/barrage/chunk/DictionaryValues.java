//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSequenceFactory;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Typed, boxing-free storage for one Arrow dictionary's decoded values.
 *
 * <p>
 * Backed by a typed primitive {@link WritableColumnSource} selected at construction by {@link ChunkType}. Values are
 * loaded in bulk via {@link WritableColumnSource#fillFromChunk}. Read access via {@link #getByte}, {@link #getLong},
 * etc. delegates directly to the underlying column source and is allocation-free for all primitive types.
 */
final class DictionaryValues {

    final ChunkType chunkType;
    private WritableColumnSource<?> source;
    private int size;

    DictionaryValues(@NotNull final ChunkType chunkType) {
        this.chunkType = chunkType;
        this.source = newSource(chunkType);
    }

    int size() {
        return size;
    }

    /**
     * Replaces all existing values with the contents of {@code chunk}.
     */
    void replace(@NotNull final WritableChunk<Values> chunk) {
        source = newSource(chunkType);
        size = 0;
        append(chunk);
    }

    /**
     * Appends the contents of {@code chunk} after the current values.
     */
    void append(@NotNull final WritableChunk<Values> chunk) {
        final int n = chunk.size();
        if (n == 0) {
            return;
        }
        final int newSize = size + n;
        source.ensureCapacity(newSize);
        try (final WritableColumnSource.FillFromContext ffc = source.makeFillFromContext(n)) {
            source.fillFromChunk(ffc, chunk, RowSequenceFactory.forRange(size, newSize - 1L));
        }
        size = newSize;
    }

    byte getByte(final int idx) {
        return source.getByte(idx);
    }

    char getChar(final int idx) {
        return source.getChar(idx);
    }

    short getShort(final int idx) {
        return source.getShort(idx);
    }

    int getInt(final int idx) {
        return source.getInt(idx);
    }

    long getLong(final int idx) {
        return source.getLong(idx);
    }

    float getFloat(final int idx) {
        return source.getFloat(idx);
    }

    double getDouble(final int idx) {
        return source.getDouble(idx);
    }

    @SuppressWarnings("unchecked")
    @Nullable
    <T> T getObject(final int idx) {
        return ((ColumnSource<T>) source).get(idx);
    }

    private static WritableColumnSource<?> newSource(@NotNull final ChunkType chunkType) {
        switch (chunkType) {
            case Byte:
                return ArrayBackedColumnSource.getMemoryColumnSource(byte.class, null);
            case Char:
                return ArrayBackedColumnSource.getMemoryColumnSource(char.class, null);
            case Short:
                return ArrayBackedColumnSource.getMemoryColumnSource(short.class, null);
            case Int:
                return ArrayBackedColumnSource.getMemoryColumnSource(int.class, null);
            case Long:
                return ArrayBackedColumnSource.getMemoryColumnSource(long.class, null);
            case Float:
                return ArrayBackedColumnSource.getMemoryColumnSource(float.class, null);
            case Double:
                return ArrayBackedColumnSource.getMemoryColumnSource(double.class, null);
            default:
                return ArrayBackedColumnSource.getMemoryColumnSource(Object.class, null);
        }
    }
}

/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.iterators;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iteration support for values supplied by a {@link ChunkSource}.
 *
 * @apiNote ColumnIterators must be explicitly {@link #close() closed} or used until exhausted in order to avoid
 *          resource leaks.
 */
public abstract class ColumnIterator<TYPE, CHUNK_TYPE extends Chunk<? extends Any>>
        implements Iterator<TYPE>, SafeCloseable {

    /**
     * The default for {@code chunkSize} used by constructors that don't accept an explicit size.
     */
    public static final int DEFAULT_CHUNK_SIZE = 1 << 11; // This is the block size for ArrayBackedColumnSource

    private final int chunkSize;
    private final long size;

    private ChunkSource<? extends Any> chunkSource;
    private ChunkSource.GetContext getContext;
    private RowSequence.Iterator rowKeyIterator;

    CHUNK_TYPE currentData;
    int currentOffset;

    /**
     * Create a new iterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from
     * @param rowSequence The {@link RowSequence} to iterate over
     * @param chunkSize The internal buffer size to use when fetching data
     */
    ColumnIterator(@NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence,
            final int chunkSize) {
        this.chunkSize = Require.gtZero(chunkSize, "chunkSize");
        this.size = rowSequence.size();

        this.chunkSource = chunkSource;
        getContext = chunkSource.makeGetContext(chunkSize);
        rowKeyIterator = rowSequence.getRowSequenceIterator();

        currentData = null;
        currentOffset = Integer.MAX_VALUE;
    }

    static ChunkSource<? extends Any> validateChunkType(@NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final ChunkType expectedChunkType) {
        final ChunkType chunkType = chunkSource.getChunkType();
        if (chunkType != expectedChunkType) {
            throw new IllegalArgumentException("Illegal chunk type " + chunkType + ", expected " + expectedChunkType);
        }
        return chunkSource;
    }

    long size() {
        return size;
    }

    @Override
    public final boolean hasNext() {
        if ((currentData == null || currentOffset >= currentData.size()) &&
                (rowKeyIterator == null || !rowKeyIterator.hasMore())) {
            close();
            return false;
        }
        return true;
    }

    final void maybeAdvance() {
        if (currentData == null || currentOffset >= currentData.size()) {
            if (rowKeyIterator == null || !rowKeyIterator.hasMore()) {
                close();
                throw new NoSuchElementException();
            }
            final RowSequence currentRowKeys = rowKeyIterator.getNextRowSequenceWithLength(chunkSize);
            currentData = castChunk(chunkSource.getChunk(getContext, currentRowKeys));
            currentOffset = 0;
        }
    }

    abstract CHUNK_TYPE castChunk(@NotNull final Chunk<? extends Any> chunk);

    final void consumeRemainingByChunks(@NotNull final Runnable consumeCurrentChunk) {
        while (hasNext()) {
            maybeAdvance();
            consumeCurrentChunk.run();
        }
    }

    @Override
    public final void close() {
        try (final SafeCloseable ignored1 = getContext;
                final SafeCloseable ignored2 = rowKeyIterator) {
            chunkSource = null;
            getContext = null;
            rowKeyIterator = null;
            currentData = null;
            currentOffset = Integer.MAX_VALUE;
        }
    }

    public static <TYPE> ColumnIterator<TYPE, ?> make(@NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence) {
        return make(chunkSource, rowSequence, DEFAULT_CHUNK_SIZE);
    }

    public static <TYPE> ColumnIterator<TYPE, ?> make(@NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence,
            final int chunkSize) {
        final ColumnIterator<?, ?> result;
        switch (chunkSource.getChunkType()) {
            case Char:
                result = new CharacterColumnIterator(chunkSource, rowSequence, chunkSize);
                break;
            case Byte:
                result = new ByteColumnIterator(chunkSource, rowSequence, chunkSize);
                break;
            case Short:
                result = new ShortColumnIterator(chunkSource, rowSequence, chunkSize);
                break;
            case Int:
                result = new IntegerColumnIterator(chunkSource, rowSequence, chunkSize);
                break;
            case Long:
                result = new LongColumnIterator(chunkSource, rowSequence, chunkSize);
                break;
            case Float:
                result = new FloatColumnIterator(chunkSource, rowSequence, chunkSize);
                break;
            case Double:
                result = new DoubleColumnIterator(chunkSource, rowSequence, chunkSize);
                break;
            case Object:
                result = new ObjectColumnIterator<>(chunkSource, rowSequence, chunkSize);
                break;
            case Boolean:
            default:
                throw new UnsupportedOperationException("Unexpected chunk type: " + chunkSource.getChunkType());
        }
        // noinspection unchecked
        return (ColumnIterator<TYPE, ?>) result;
    }
}

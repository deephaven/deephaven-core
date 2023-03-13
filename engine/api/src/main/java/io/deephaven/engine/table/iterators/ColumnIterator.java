/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.iterators;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;

/**
 * Iteration support for values supplied by a {@link ChunkSource}.
 *
 * @apiNote ColumnIterators must be explicitly {@link #close() closed} or used until exhausted in order to avoid
 *          resource leaks.
 */
public abstract class ColumnIterator<TYPE, CHUNK_TYPE extends Chunk<? extends Any>>
        implements CloseableIterator<TYPE> {

    /**
     * The default for {@code chunkSize} used by constructors that don't accept an explicit size.
     */
    public static final int DEFAULT_CHUNK_SIZE = 1 << 11; // This is the block size for ArrayBackedColumnSource

    private final int chunkSize;

    private ChunkSource<? extends Any> chunkSource;
    private ChunkSource.GetContext getContext;
    private RowSequence.Iterator rowKeyIterator;
    private long remainingRowKeys;

    CHUNK_TYPE currentData;
    int currentOffset;

    /**
     * Create a new iterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from
     * @param rowSequence The {@link RowSequence} to iterate over
     * @param chunkSize The internal buffer size to use when fetching data
     * @param firstRowKey The first row key from {@code rowSequence} to iterate
     * @param length The total number of rows to iterate
     */
    ColumnIterator(@NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence,
            final int chunkSize,
            final long firstRowKey,
            final long length) {
        this.chunkSize = Require.gtZero(chunkSize, "chunkSize");

        this.chunkSource = chunkSource;
        getContext = chunkSource.makeGetContext(chunkSize);
        rowKeyIterator = rowSequence.getRowSequenceIterator();
        final long consumed = rowKeyIterator.advanceAndGetPositionDistance(firstRowKey);
        if (rowKeyIterator.peekNextKey() != firstRowKey) {
            throw new IllegalArgumentException(String.format(
                    "Invalid first row key %d, not present in iteration row sequence", firstRowKey));
        }
        if (rowSequence.size() - consumed < length) {
            throw new IllegalArgumentException(String.format(
                    "Invalid length %d, iteration row sequence only contains %d rows (%d already consumed)",
                    length, rowSequence.size(), consumed));
        }
        remainingRowKeys = length;

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

    /**
     * @return The remaining number of elements in this iterator
     */
    long remaining() {
        return remainingRowKeys + (currentData == null ? 0 : currentData.size() - currentData.size());
    }

    @Override
    public final boolean hasNext() {
        if ((currentData == null || currentOffset >= currentData.size())
                && remainingRowKeys <= 0) {
            close();
            return false;
        }
        return true;
    }

    final void maybeAdvance() {
        if (currentData == null || currentOffset >= currentData.size()) {
            if (remainingRowKeys <= 0) {
                close();
                throw new NoSuchElementException();
            }
            final RowSequence currentRowKeys =
                    rowKeyIterator.getNextRowSequenceWithLength(Math.min(chunkSize, remainingRowKeys));
            remainingRowKeys -= currentRowKeys.size();
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
            remainingRowKeys = 0;
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
                result = new CharacterColumnIterator(chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(),
                        rowSequence.size());
                break;
            case Byte:
                result = new ByteColumnIterator(chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(),
                        rowSequence.size());
                break;
            case Short:
                result = new ShortColumnIterator(chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(),
                        rowSequence.size());
                break;
            case Int:
                result = new IntegerColumnIterator(chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(),
                        rowSequence.size());
                break;
            case Long:
                result = new LongColumnIterator(chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(),
                        rowSequence.size());
                break;
            case Float:
                result = new FloatColumnIterator(chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(),
                        rowSequence.size());
                break;
            case Double:
                result = new DoubleColumnIterator(chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(),
                        rowSequence.size());
                break;
            case Object:
                result = new ObjectColumnIterator<>(chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(),
                        rowSequence.size());
                break;
            case Boolean:
            default:
                throw new UnsupportedOperationException("Unexpected chunk type: " + chunkSource.getChunkType());
        }
        // noinspection unchecked
        return (ColumnIterator<TYPE, ?>) result;
    }
}

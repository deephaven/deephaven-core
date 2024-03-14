//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.iterators;

import io.deephaven.base.verify.Require;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Any;
import io.deephaven.engine.rowset.RowSequence;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.NoSuchElementException;

import static io.deephaven.chunk.util.pools.ChunkPoolConstants.SMALLEST_POOLED_CHUNK_CAPACITY;

/**
 * Iteration support for values supplied by a {@link ChunkSource}. Implementations retrieve {@link Chunk chunks} of
 * values at a time in a common Deephaven engine retrieval pattern. This is expected to be high throughput relative to
 * {@link SerialColumnIterator} implementations, but may have material initialization and teardown costs for small or
 * sparse iterations.
 */
public abstract class ChunkedColumnIterator<DATA_TYPE, CHUNK_TYPE extends Chunk<? extends Any>>
        implements ColumnIterator<DATA_TYPE> {

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
     * Create a new ChunkedColumnIterator.
     *
     * @param chunkSource The {@link ChunkSource} to fetch values from
     * @param rowSequence The {@link RowSequence} to iterate over
     * @param chunkSize The internal buffer size to use when fetching data
     * @param firstRowKey The first row key from {@code rowSequence} to iterate
     * @param length The total number of rows to iterate
     */
    ChunkedColumnIterator(
            @NotNull final ChunkSource<? extends Any> chunkSource,
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

    static ChunkSource<? extends Any> validateChunkType(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final ChunkType expectedChunkType) {
        final ChunkType chunkType = chunkSource.getChunkType();
        if (chunkType != expectedChunkType) {
            throw new IllegalArgumentException("Illegal chunk type " + chunkType + ", expected " + expectedChunkType);
        }
        return chunkSource;
    }

    @Override
    public final long remaining() {
        return remainingRowKeys + (currentData == null ? 0 : currentData.size() - currentOffset);
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

    /**
     * Maybe advance this ChunkedColumnIterator if necessary (that is, if there is no current chunk or no remaining
     * elements in the current chunk), by reading the next chunk and setting {@link #currentData} and
     * {@link #currentOffset} accordingly.
     * 
     * @throws NoSuchElementException If this ChunkedColumnIterator is exhausted
     */
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

    /**
     * Cast {@code chunk} to the appropriate class for this implementation.
     *
     * @param chunk The {@link Chunk} to cast
     * @return {@code chunk} with the appropriate cast applied
     */
    abstract CHUNK_TYPE castChunk(@NotNull final Chunk<? extends Any> chunk);

    /**
     * Invoke {@code consumeCurrentChunk} to consume all data in each remaining chunk of data in this
     * ChunkedColumnIterator.
     *
     * @param consumeCurrentChunk The procedure to invoke. Must result in {@code currentOffset == currentData.size()}.
     *        Takes no arguments, because this method is only called by tightly-coupled classes with access to
     *        {@link #currentData} and {@link #currentOffset}.
     */
    final void consumeRemainingByChunks(@NotNull final Runnable consumeCurrentChunk) {
        while (hasNext()) {
            maybeAdvance();
            consumeCurrentChunk.run();
        }
    }

    @Override
    public final void close() {
        // @formatter:off
        try (final SafeCloseable ignored1 = getContext;
             final SafeCloseable ignored2 = rowKeyIterator) {
            // @formatter:on
            chunkSource = null;
            getContext = null;
            rowKeyIterator = null;
            remainingRowKeys = 0;
            currentData = null;
            currentOffset = Integer.MAX_VALUE;
        }
    }

    public static <DATA_TYPE> ColumnIterator<DATA_TYPE> make(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence) {
        return make(chunkSource, rowSequence, DEFAULT_CHUNK_SIZE);
    }

    public static <DATA_TYPE> ColumnIterator<DATA_TYPE> make(
            @NotNull final ChunkSource<? extends Any> chunkSource,
            @NotNull final RowSequence rowSequence,
            int chunkSize) {
        chunkSize = Math.max((int) Math.min(chunkSize, rowSequence.size()), SMALLEST_POOLED_CHUNK_CAPACITY);
        final ColumnIterator<?> result;
        switch (chunkSource.getChunkType()) {
            case Char:
                result = new ChunkedCharacterColumnIterator(
                        chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(), rowSequence.size());
                break;
            case Byte:
                result = new ChunkedByteColumnIterator(
                        chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(), rowSequence.size());
                break;
            case Short:
                result = new ChunkedShortColumnIterator(
                        chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(), rowSequence.size());
                break;
            case Int:
                result = new ChunkedIntegerColumnIterator(
                        chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(), rowSequence.size());
                break;
            case Long:
                result = new ChunkedLongColumnIterator(
                        chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(), rowSequence.size());
                break;
            case Float:
                result = new ChunkedFloatColumnIterator(
                        chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(), rowSequence.size());
                break;
            case Double:
                result = new ChunkedDoubleColumnIterator(
                        chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(), rowSequence.size());
                break;
            case Object:
                result = new ChunkedObjectColumnIterator<>(
                        chunkSource, rowSequence, chunkSize, rowSequence.firstRowKey(), rowSequence.size());
                break;
            case Boolean:
            default:
                throw new UnsupportedOperationException("Unexpected chunk type: " + chunkSource.getChunkType());
        }
        // noinspection unchecked
        return (ColumnIterator<DATA_TYPE>) result;
    }
}

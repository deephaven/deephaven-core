//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.MathUtil;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableDoubleChunk;
import io.deephaven.chunk.WritableFloatChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.ChunkPoolConstants;
import io.deephaven.configuration.Configuration;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Arrays;

/**
 * Typed, boxing-free storage for one Arrow dictionary's decoded values.
 *
 * <p>
 * Values are copied into fixed-capacity internal chunks of {@link #CHUNK_CAPACITY} entries (configurable via the
 * {@code DictionaryReaderValues.chunkCapacity} property, default
 * {@link ChunkPoolConstants#LARGEST_POOLED_CHUNK_CAPACITY}); the caller retains ownership of the chunks it passes to
 * {@link #replace} and {@link #append}. Because the chunk capacity is sanitized to a power of two, logical index
 * {@code idx} resolves to a (chunk, offset) pair with a shift and a mask: O(1). Read access via {@link #getByte},
 * {@link #getLong}, etc. is allocation-free and cast-free for all primitive types.
 */
final class DictionaryReaderValues implements SafeCloseable {

    /**
     * The configured internal chunk capacity, sanitized to a power of two so index lookups remain a shift and a mask.
     */
    static final int CHUNK_CAPACITY = MathUtil.roundUpPowerOf2(Configuration.getInstance()
            .getIntegerForClassWithDefault(
                    DictionaryReaderValues.class, "chunkCapacity", ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY));
    private static final int CHUNK_SHIFT = Integer.numberOfTrailingZeros(CHUNK_CAPACITY);
    private static final int CHUNK_MASK = CHUNK_CAPACITY - 1;

    private static final int INITIAL_NUM_CHUNKS = 4;

    final ChunkType chunkType;

    /**
     * The typed chunk array matching {@link #chunkType} is the only non-null one; {@link #chunksArray()} exposes it as
     * a generic view for {@link #append} and {@link #releaseChunks()}, while the getters read the typed field directly
     * and cast-free. The first {@link #numChunks} entries are live; {@link #addNewChunk()} doubles the array when full,
     * and {@link #releaseChunks()} nulls the entries without shrinking the array.
     */
    private WritableByteChunk<Values>[] byteChunks;
    private WritableCharChunk<Values>[] charChunks;
    private WritableShortChunk<Values>[] shortChunks;
    private WritableIntChunk<Values>[] intChunks;
    private WritableLongChunk<Values>[] longChunks;
    private WritableFloatChunk<Values>[] floatChunks;
    private WritableDoubleChunk<Values>[] doubleChunks;
    private WritableObjectChunk<Object, Values>[] objectChunks;

    private int numChunks;
    private int size;

    @SuppressWarnings("unchecked")
    DictionaryReaderValues(@NotNull final ChunkType chunkType) {
        this.chunkType = chunkType;
        switch (chunkType) {
            case Byte:
                byteChunks = new WritableByteChunk[INITIAL_NUM_CHUNKS];
                break;
            case Char:
                charChunks = new WritableCharChunk[INITIAL_NUM_CHUNKS];
                break;
            case Short:
                shortChunks = new WritableShortChunk[INITIAL_NUM_CHUNKS];
                break;
            case Int:
                intChunks = new WritableIntChunk[INITIAL_NUM_CHUNKS];
                break;
            case Long:
                longChunks = new WritableLongChunk[INITIAL_NUM_CHUNKS];
                break;
            case Float:
                floatChunks = new WritableFloatChunk[INITIAL_NUM_CHUNKS];
                break;
            case Double:
                doubleChunks = new WritableDoubleChunk[INITIAL_NUM_CHUNKS];
                break;
            case Object:
                objectChunks = new WritableObjectChunk[INITIAL_NUM_CHUNKS];
                break;
            default:
                throw new UnsupportedOperationException(
                        "DictionaryReaderValues does not support chunk type " + chunkType);
        }
    }

    /**
     * Returns the typed chunk array matching {@link #chunkType} as a generic view.
     */
    private WritableChunk<Values>[] chunksArray() {
        switch (chunkType) {
            case Byte:
                return byteChunks;
            case Char:
                return charChunks;
            case Short:
                return shortChunks;
            case Int:
                return intChunks;
            case Long:
                return longChunks;
            case Float:
                return floatChunks;
            case Double:
                return doubleChunks;
            case Object:
                return objectChunks;
            default:
                throw new UnsupportedOperationException(
                        "DictionaryReaderValues does not support chunk type " + chunkType);
        }
    }

    /**
     * Appends a freshly allocated chunk to the typed array matching {@link #chunkType}, doubling the array when full.
     */
    private void addNewChunk() {
        switch (chunkType) {
            case Byte:
                if (numChunks == byteChunks.length) {
                    byteChunks = Arrays.copyOf(byteChunks, numChunks * 2);
                }
                byteChunks[numChunks] = WritableByteChunk.makeWritableChunk(CHUNK_CAPACITY);
                break;
            case Char:
                if (numChunks == charChunks.length) {
                    charChunks = Arrays.copyOf(charChunks, numChunks * 2);
                }
                charChunks[numChunks] = WritableCharChunk.makeWritableChunk(CHUNK_CAPACITY);
                break;
            case Short:
                if (numChunks == shortChunks.length) {
                    shortChunks = Arrays.copyOf(shortChunks, numChunks * 2);
                }
                shortChunks[numChunks] = WritableShortChunk.makeWritableChunk(CHUNK_CAPACITY);
                break;
            case Int:
                if (numChunks == intChunks.length) {
                    intChunks = Arrays.copyOf(intChunks, numChunks * 2);
                }
                intChunks[numChunks] = WritableIntChunk.makeWritableChunk(CHUNK_CAPACITY);
                break;
            case Long:
                if (numChunks == longChunks.length) {
                    longChunks = Arrays.copyOf(longChunks, numChunks * 2);
                }
                longChunks[numChunks] = WritableLongChunk.makeWritableChunk(CHUNK_CAPACITY);
                break;
            case Float:
                if (numChunks == floatChunks.length) {
                    floatChunks = Arrays.copyOf(floatChunks, numChunks * 2);
                }
                floatChunks[numChunks] = WritableFloatChunk.makeWritableChunk(CHUNK_CAPACITY);
                break;
            case Double:
                if (numChunks == doubleChunks.length) {
                    doubleChunks = Arrays.copyOf(doubleChunks, numChunks * 2);
                }
                doubleChunks[numChunks] = WritableDoubleChunk.makeWritableChunk(CHUNK_CAPACITY);
                break;
            case Object:
                if (numChunks == objectChunks.length) {
                    objectChunks = Arrays.copyOf(objectChunks, numChunks * 2);
                }
                objectChunks[numChunks] = WritableObjectChunk.makeWritableChunk(CHUNK_CAPACITY);
                break;
            default:
                throw new UnsupportedOperationException(
                        "DictionaryReaderValues does not support chunk type " + chunkType);
        }
        ++numChunks;
    }

    int size() {
        return size;
    }

    /**
     * Replaces all existing values with the contents of {@code chunk}. The caller retains ownership of {@code chunk}.
     * All existing internal chunks are released; the chunk array keeps its capacity.
     */
    void replace(@NotNull final Chunk<Values> chunk) {
        releaseChunks();
        append(chunk);
    }

    /**
     * Appends the contents of {@code chunk} after the current values, copying them into internal chunks. The caller
     * retains ownership of {@code chunk}.
     */
    void append(@NotNull final Chunk<Values> chunk) {
        int srcOffset = 0;
        int remaining = chunk.size();
        while (remaining > 0) {
            final int chunkIndex = size >> CHUNK_SHIFT;
            final int destOffset = size & CHUNK_MASK;
            if (chunkIndex == numChunks) {
                addNewChunk();
            }
            final int copySize = Math.min(remaining, CHUNK_CAPACITY - destOffset);
            chunksArray()[chunkIndex].copyFromChunk(chunk, srcOffset, destOffset, copySize);
            srcOffset += copySize;
            size += copySize;
            remaining -= copySize;
        }
    }

    byte getByte(final int idx) {
        return byteChunks[idx >> CHUNK_SHIFT].get(idx & CHUNK_MASK);
    }

    char getChar(final int idx) {
        return charChunks[idx >> CHUNK_SHIFT].get(idx & CHUNK_MASK);
    }

    short getShort(final int idx) {
        return shortChunks[idx >> CHUNK_SHIFT].get(idx & CHUNK_MASK);
    }

    int getInt(final int idx) {
        return intChunks[idx >> CHUNK_SHIFT].get(idx & CHUNK_MASK);
    }

    long getLong(final int idx) {
        return longChunks[idx >> CHUNK_SHIFT].get(idx & CHUNK_MASK);
    }

    float getFloat(final int idx) {
        return floatChunks[idx >> CHUNK_SHIFT].get(idx & CHUNK_MASK);
    }

    double getDouble(final int idx) {
        return doubleChunks[idx >> CHUNK_SHIFT].get(idx & CHUNK_MASK);
    }

    @Nullable
    <T> T getObject(final int idx) {
        // noinspection unchecked
        return (T) objectChunks[idx >> CHUNK_SHIFT].get(idx & CHUNK_MASK);
    }

    private void releaseChunks() {
        final WritableChunk<Values>[] chunks = chunksArray();
        for (int ci = 0; ci < numChunks; ++ci) {
            chunks[ci].close();
            chunks[ci] = null;
        }
        numChunks = 0;
        size = 0;
    }

    @Override
    public void close() {
        releaseChunks();
    }
}

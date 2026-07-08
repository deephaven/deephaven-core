//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Typed, boxing-free storage for one Arrow dictionary's decoded values.
 *
 * <p>
 * Incoming value chunks are retained as-is (no copy); this class takes ownership and closes them on {@link #replace}
 * and {@link #close}. Logical index {@code idx} is resolved to a (chunk, offset) pair via a prefix-sum array of
 * cumulative chunk ends: O(log c) where c is the number of retained chunks (one per DictionaryBatch delta). Read access
 * via {@link #getByte}, {@link #getLong}, etc. is allocation-free for all primitive types.
 */
final class DictionaryReaderValues implements SafeCloseable {

    final ChunkType chunkType;

    private final List<WritableChunk<Values>> chunks = new ArrayList<>();
    /** cumulativeEnds[i] is the total number of values in chunks 0..i inclusive. */
    private int[] chunkCumulativeEnds = new int[8];
    private int size;

    DictionaryReaderValues(@NotNull final ChunkType chunkType) {
        this.chunkType = chunkType;
    }

    int size() {
        return size;
    }

    /**
     * Replaces all existing values with the contents of {@code chunk}, taking ownership of it.
     */
    void replace(@NotNull final WritableChunk<Values> chunk) {
        closeChunks();
        append(chunk);
    }

    /**
     * Appends the contents of {@code chunk} after the current values, taking ownership of it. The chunk is retained
     * without copying; the caller must not close or reuse it.
     */
    void append(@NotNull final WritableChunk<Values> chunk) {
        final int chunkSize = chunk.size();
        if (chunkSize == 0) {
            chunk.close();
            return;
        }
        final int nextChunkIndex = chunks.size();
        if (nextChunkIndex == chunkCumulativeEnds.length) {
            chunkCumulativeEnds = Arrays.copyOf(chunkCumulativeEnds, chunkCumulativeEnds.length * 2);
        }
        size += chunkSize;
        chunkCumulativeEnds[nextChunkIndex] = size;
        chunks.add(chunk);
    }

    /** Returns the index of the retained chunk containing logical index {@code idx}. */
    private int chunkIndex(final int idx) {
        if (chunks.size() == 1) {
            return 0;
        }
        // Find the smallest i with cumulativeEnds[i] > idx by searching for idx + 1 and taking the first
        // chunk that is >= that key
        final int r = Arrays.binarySearch(chunkCumulativeEnds, 0, chunks.size(), idx + 1);
        return r >= 0 ? r : -r - 1;
    }

    /** Returns the offset of logical index {@code idx} within chunk {@code chunkIdx}. */
    private int chunkOffset(final int chunkIdx, final int idx) {
        return chunkIdx == 0 ? idx : idx - chunkCumulativeEnds[chunkIdx - 1];
    }

    byte getByte(final int idx) {
        final int ci = chunkIndex(idx);
        return chunks.get(ci).asByteChunk().get(chunkOffset(ci, idx));
    }

    char getChar(final int idx) {
        final int ci = chunkIndex(idx);
        return chunks.get(ci).asCharChunk().get(chunkOffset(ci, idx));
    }

    short getShort(final int idx) {
        final int ci = chunkIndex(idx);
        return chunks.get(ci).asShortChunk().get(chunkOffset(ci, idx));
    }

    int getInt(final int idx) {
        final int ci = chunkIndex(idx);
        return chunks.get(ci).asIntChunk().get(chunkOffset(ci, idx));
    }

    long getLong(final int idx) {
        final int ci = chunkIndex(idx);
        return chunks.get(ci).asLongChunk().get(chunkOffset(ci, idx));
    }

    float getFloat(final int idx) {
        final int ci = chunkIndex(idx);
        return chunks.get(ci).asFloatChunk().get(chunkOffset(ci, idx));
    }

    double getDouble(final int idx) {
        final int ci = chunkIndex(idx);
        return chunks.get(ci).asDoubleChunk().get(chunkOffset(ci, idx));
    }

    @Nullable
    <T> T getObject(final int idx) {
        final int ci = chunkIndex(idx);
        return chunks.get(ci).<T>asObjectChunk().get(chunkOffset(ci, idx));
    }

    private void closeChunks() {
        chunks.forEach(WritableChunk::close);
        chunks.clear();
        size = 0;
    }

    @Override
    public void close() {
        closeChunks();
    }
}

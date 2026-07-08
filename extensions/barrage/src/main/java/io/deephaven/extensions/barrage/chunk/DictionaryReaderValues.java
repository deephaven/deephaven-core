//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.MathUtil;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.util.pools.ChunkPoolConstants;
import io.deephaven.configuration.Configuration;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;

/**
 * Typed, boxing-free storage for one Arrow dictionary's decoded values.
 *
 * <p>
 * Values are copied into fixed-capacity pages of {@link #PAGE_CAPACITY} entries (configurable via the
 * {@code DictionaryReaderValues.pageCapacity} property, default
 * {@link ChunkPoolConstants#LARGEST_POOLED_CHUNK_CAPACITY}); the caller retains ownership of the chunks it passes to
 * {@link #replace} and {@link #append}. Because the page capacity is sanitized to a power of two, logical index
 * {@code idx} resolves to a (page, offset) pair with a shift and a mask: O(1). Read access via {@link #getByte},
 * {@link #getLong}, etc. is allocation-free for all primitive types.
 */
final class DictionaryReaderValues implements SafeCloseable {

    /**
     * The configured page capacity, sanitized to a power of two so index lookups remain a shift and a mask.
     */
    static final int PAGE_CAPACITY = MathUtil.roundUpPowerOf2(Configuration.getInstance().getIntegerForClassWithDefault(
            DictionaryReaderValues.class, "pageCapacity", ChunkPoolConstants.LARGEST_POOLED_CHUNK_CAPACITY));
    private static final int PAGE_SHIFT = Integer.numberOfTrailingZeros(PAGE_CAPACITY);
    private static final int PAGE_MASK = PAGE_CAPACITY - 1;

    final ChunkType chunkType;

    private final List<WritableChunk<Values>> pages = new ArrayList<>();
    private int size;

    DictionaryReaderValues(@NotNull final ChunkType chunkType) {
        this.chunkType = chunkType;
    }

    int size() {
        return size;
    }

    /**
     * Replaces all existing values with the contents of {@code chunk}. The caller retains ownership of {@code chunk}.
     * The first page is kept for reuse; any additional pages are released.
     */
    void replace(@NotNull final Chunk<Values> chunk) {
        while (pages.size() > 1) {
            pages.remove(pages.size() - 1).close();
        }
        if (chunkType == ChunkType.Object && !pages.isEmpty()) {
            // do not pin the replaced dictionary's objects
            pages.get(0).fillWithNullValue(0, Math.min(size, PAGE_CAPACITY));
        }
        size = 0;
        append(chunk);
    }

    /**
     * Appends the contents of {@code chunk} after the current values, copying them into internal pages. The caller
     * retains ownership of {@code chunk}.
     */
    void append(@NotNull final Chunk<Values> chunk) {
        int srcOffset = 0;
        int remaining = chunk.size();
        while (remaining > 0) {
            final int pageIndex = size >> PAGE_SHIFT;
            final int destOffset = size & PAGE_MASK;
            if (pageIndex == pages.size()) {
                pages.add(chunkType.makeWritableChunk(PAGE_CAPACITY));
            }
            final int copySize = Math.min(remaining, PAGE_CAPACITY - destOffset);
            pages.get(pageIndex).copyFromChunk(chunk, srcOffset, destOffset, copySize);
            srcOffset += copySize;
            size += copySize;
            remaining -= copySize;
        }
    }

    byte getByte(final int idx) {
        return pages.get(idx >> PAGE_SHIFT).asByteChunk().get(idx & PAGE_MASK);
    }

    char getChar(final int idx) {
        return pages.get(idx >> PAGE_SHIFT).asCharChunk().get(idx & PAGE_MASK);
    }

    short getShort(final int idx) {
        return pages.get(idx >> PAGE_SHIFT).asShortChunk().get(idx & PAGE_MASK);
    }

    int getInt(final int idx) {
        return pages.get(idx >> PAGE_SHIFT).asIntChunk().get(idx & PAGE_MASK);
    }

    long getLong(final int idx) {
        return pages.get(idx >> PAGE_SHIFT).asLongChunk().get(idx & PAGE_MASK);
    }

    float getFloat(final int idx) {
        return pages.get(idx >> PAGE_SHIFT).asFloatChunk().get(idx & PAGE_MASK);
    }

    double getDouble(final int idx) {
        return pages.get(idx >> PAGE_SHIFT).asDoubleChunk().get(idx & PAGE_MASK);
    }

    @Nullable
    <T> T getObject(final int idx) {
        return pages.get(idx >> PAGE_SHIFT).<T>asObjectChunk().get(idx & PAGE_MASK);
    }

    @Override
    public void close() {
        pages.forEach(WritableChunk::close);
        pages.clear();
        size = 0;
    }
}

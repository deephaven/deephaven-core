package io.deephaven.engine.rowset.impl;

import io.deephaven.engine.chunk.Attributes;
import io.deephaven.engine.chunk.Chunk;
import io.deephaven.engine.chunk.LongChunk;
import io.deephaven.engine.chunk.WritableLongChunk;
import io.deephaven.util.annotations.VisibleForTesting;
import io.deephaven.util.datastructures.SizeException;

/**
 * Utilities for working with {@link LongChunk chunks} of row keys.
 */
public class RowKeyChunkUtil {

    /**
     * Generates a {@code LongChunk<OrderedRowKeyRanges>} from a {@code LongChunk<Attributes.OrderedRowKeys>}.
     *
     * @param chunk the chunk to convert
     * @return the generated chunk
     */
    public static WritableLongChunk<Attributes.OrderedRowKeyRanges> convertToOrderedKeyRanges(
            final LongChunk<Attributes.OrderedRowKeys> chunk) {
        return convertToOrderedKeyRanges(chunk, Chunk.MAXIMUM_SIZE);
    }

    @VisibleForTesting
    public static WritableLongChunk<Attributes.OrderedRowKeyRanges> convertToOrderedKeyRanges(
            final LongChunk<Attributes.OrderedRowKeys> chunk,
            final long maxChunkSize) {
        if (chunk.size() == 0) {
            return WritableLongChunk.makeWritableChunk(0);
        }

        // First we'll count the number of ranges so that we can allocate the exact amount of space needed.
        long numRanges = 1;
        for (int idx = 1; idx < chunk.size(); ++idx) {
            if (chunk.get(idx - 1) + 1 != chunk.get(idx)) {
                ++numRanges;
            }
        }

        final long newSize = numRanges * 2L;
        if (newSize > maxChunkSize) {
            throw new SizeException("Cannot expand RowKeys Chunk into KeyRanges Chunk.", newSize, maxChunkSize);
        }

        final WritableLongChunk<Attributes.OrderedRowKeyRanges> newChunk =
                WritableLongChunk.makeWritableChunk((int) newSize);

        convertToOrderedKeyRanges(chunk, newChunk);

        return newChunk;
    }

    /**
     * Fills {@code OrderedRowKeyRanges} into {@code dest} from the provided {@code chunk} and specified source range.
     *
     * @param chunk the chunk to convert
     * @param dest the chunk to fill with ranges
     */
    public static void convertToOrderedKeyRanges(final LongChunk<Attributes.OrderedRowKeys> chunk,
            final WritableLongChunk<Attributes.OrderedRowKeyRanges> dest) {
        int destOffset = 0;
        if (chunk.size() == 0) {
            dest.setSize(destOffset);
            return;
        }

        int srcOffset = 0;
        dest.set(destOffset++, chunk.get(srcOffset));
        for (++srcOffset; srcOffset < chunk.size(); ++srcOffset) {
            if (chunk.get(srcOffset - 1) + 1 != chunk.get(srcOffset)) {
                // we now know that the currently open range ends at srcOffset - 1
                dest.set(destOffset++, chunk.get(srcOffset - 1));
                dest.set(destOffset++, chunk.get(srcOffset));
            }
        }
        dest.set(destOffset++, chunk.get(srcOffset - 1));

        dest.setSize(destOffset);
    }

    /**
     * Generates a {@code LongChunk<Attributes.OrderedRowKeys>} from {@code LongChunk<Attributes.OrderedRowKeyRanges>}.
     *
     * @param chunk the chunk to convert
     * @return the generated chunk
     */
    public static LongChunk<Attributes.OrderedRowKeys> convertToOrderedKeyIndices(
            final LongChunk<Attributes.OrderedRowKeyRanges> chunk) {
        return convertToOrderedKeyIndices(0, chunk);
    }

    /**
     * Generates a {@code LongChunk<Attributes.OrderedRowKeys>} from {@code LongChunk<Attributes.OrderedRowKeyRanges>}.
     *
     * @param srcOffset the offset into {@code chunk} to begin including in the generated chunk
     * @param chunk the chunk to convert
     * @return the generated chunk
     */
    public static LongChunk<Attributes.OrderedRowKeys> convertToOrderedKeyIndices(int srcOffset,
            final LongChunk<Attributes.OrderedRowKeyRanges> chunk) {
        srcOffset += srcOffset % 2; // ensure that we are using the correct range edges

        long numElements = 0;
        for (int idx = 0; idx < chunk.size(); idx += 2) {
            numElements += chunk.get(idx + 1) - chunk.get(idx) + 1;
        }

        // Note that maximum range is [0, Long.MAX_VALUE] and all ranges are non-overlapping. Therefore we will never
        // overflow past Long.MIN_VALUE.
        if (numElements < 0 || numElements > Chunk.MAXIMUM_SIZE) {
            throw new SizeException("Cannot expand OrderedRowKeyRanges Chunk into OrderedRowKeys Chunk.", numElements,
                    Chunk.MAXIMUM_SIZE);
        }

        final WritableLongChunk<Attributes.OrderedRowKeys> newChunk =
                WritableLongChunk.makeWritableChunk((int) numElements);
        convertToOrderedKeyIndices(srcOffset, chunk, newChunk, 0);
        return newChunk;
    }

    /**
     * Fill a {@code LongChunk<Attributes.OrderedRowKeys>} from {@code LongChunk<Attributes.OrderedRowKeyRanges>}.
     *
     * @param srcOffset the offset into {@code chunk} to begin including in the generated chunk
     * @param chunk the chunk to convert
     * @param dest the chunk to fill with indices
     */
    public static void convertToOrderedKeyIndices(int srcOffset, final LongChunk<Attributes.OrderedRowKeyRanges> chunk,
            final WritableLongChunk<Attributes.OrderedRowKeys> dest, int destOffset) {
        srcOffset += srcOffset & 1; // ensure that we are using the correct range edges

        for (int idx = srcOffset; idx + 1 < chunk.size() && destOffset < dest.size(); idx += 2) {
            final long start = chunk.get(idx);
            final long range = chunk.get(idx + 1) - start + 1; // note that due to checks above, range cannot overflow
            for (long jdx = 0; jdx < range && destOffset < dest.size(); ++jdx) {
                dest.set(destOffset++, start + jdx);
            }
        }

        dest.setSize(destOffset);
    }
}

package io.deephaven.chunk;

import io.deephaven.chunk.attributes.Any;

import java.util.Arrays;

public class OrderedChunkUtils {

    /**
     * Finds the insertion point in {@code chunk} for {@code value}.
     *
     * @param chunk to search through
     * @param value the value to find
     * @return The insertion point in {@code [0, chunk.size())}.
     */
    public static <ATTR extends Any> int findInChunk(final LongChunk<ATTR> chunk, final long value) {
        return findInChunk(chunk, value, 0, chunk.size());
    }

    /**
     * Finds the insertion point in {@code chunk} for {@code value}.
     *
     * @param chunk to search through
     * @param value the value to find
     * @param startOffset the minimum offset allowed
     * @param endOffsetExclusive the offset just beyond the maximum allowed
     * @return The insertion point in {@code [startOffset, endOffsetExclusive)}.
     */
    public static <ATTR extends Any> int findInChunk(final LongChunk<ATTR> chunk, final long value,
            final int startOffset, final int endOffsetExclusive) {
        int retVal = Arrays.binarySearch(chunk.data, chunk.offset + startOffset,
                chunk.offset + endOffsetExclusive, value);
        // Note that Arrays.binarySearch returns `-i + 1` if element not found.
        retVal = (retVal < 0) ? ~retVal : retVal;
        return Math.min(retVal - chunk.offset, chunk.size);
    }
}

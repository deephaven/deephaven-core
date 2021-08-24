package io.deephaven.db.v2.utils;

import io.deephaven.db.v2.sources.chunk.LongChunk;
import org.junit.Test;

public class ShiftedOrderedKeysTest extends OrderedKeysTestBase {

    private final long SHIFT = 1 << 20;

    @Override
    protected OrderedKeys create(long... values) {
        final long[] shifted = new long[values.length];
        for (int i = 0; i < values.length; ++i) {
            shifted[i] = values[i] + SHIFT;
        }
        final OrderedKeys other =
            OrderedKeys.wrapKeyIndicesChunkAsOrderedKeys(LongChunk.chunkWrap(shifted));
        return ShiftedOrderedKeys.wrap(closeOnTearDownCase(other), -SHIFT);
    }

    @Test
    @Override
    // The original test uses some large keys that overflow when shifted.
    public void testCanConstructOrderedKeys() {
        final long[] indices =
            indicesFromRanges(0, 4, Long.MAX_VALUE - 4 - SHIFT, Long.MAX_VALUE - SHIFT);
        try (final OrderedKeys OK = create(indices)) {
            assertContentsByIndices(indices, OK);
        }
    }
}

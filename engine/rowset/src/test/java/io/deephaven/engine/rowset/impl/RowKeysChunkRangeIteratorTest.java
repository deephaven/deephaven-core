//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;

/**
 * {@link RowKeysChunkRangeIterator} moves through a chunk's ranges the same way a {@link RowSet}'s range iterator moves
 * through the same keys.
 */
public class RowKeysChunkRangeIteratorTest {

    @Test
    public void testMatchesRowSetRangeIterator() {
        final Random random = new Random(0);
        for (int trial = 0; trial < 200; ++trial) {
            // runs of 1 to 8 keys separated by gaps of 1 to 64 keys
            final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
            long key = random.nextInt(16);
            final int runs = 1 + random.nextInt(300);
            for (int ri = 0; ri < runs; ++ri) {
                final long runLength = 1 + random.nextInt(8);
                builder.appendRange(key, key + runLength - 1);
                key += runLength + 1 + random.nextInt(64);
            }
            try (final RowSet rowSet = builder.build();
                    final WritableLongChunk<OrderedRowKeys> keys =
                            WritableLongChunk.makeWritableChunk(rowSet.intSize());
                    final RowSet.RangeIterator expected = rowSet.rangeIterator()) {
                rowSet.fillRowKeyChunk(keys);
                final RowKeysChunkRangeIterator actual = new RowKeysChunkRangeIterator(keys);
                long target = 0;
                while (true) {
                    final String what = "trial " + trial + " target " + target;
                    if (random.nextInt(4) == 0) {
                        assertEquals(what, expected.hasNext(), actual.hasNext());
                        if (!expected.hasNext()) {
                            break;
                        }
                        assertEquals(what, expected.next(), actual.next());
                        target = actual.currentRangeStart();
                    } else {
                        // usually step past a few ranges; sometimes stay inside the current one or leap far ahead
                        target += random.nextInt(4) == 0 ? random.nextInt(4)
                                : random.nextInt(random.nextBoolean() ? 40 : 4000);
                        final boolean found = expected.advance(target);
                        assertEquals(what, found, actual.advance(target));
                        if (!found) {
                            break;
                        }
                    }
                    assertEquals(what, expected.currentRangeStart(), actual.currentRangeStart());
                    assertEquals(what, expected.currentRangeEnd(), actual.currentRangeEnd());
                }
            }
        }
    }
}

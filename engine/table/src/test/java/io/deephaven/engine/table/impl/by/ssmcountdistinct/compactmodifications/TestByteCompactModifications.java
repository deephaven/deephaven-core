//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharCompactModifications and run "./gradlew replicateSegmentedSortedMultisetTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.by.ssmcountdistinct.compactmodifications;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.test.types.ParallelTest;
import io.deephaven.util.mutable.MutableInt;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

@Category(ParallelTest.class)
public class TestByteCompactModifications {
    @Test
    public void testPureRemovals() {
        // every value drops out entirely (nothing added survives)
        check(new byte[] {(byte) ('a' + 0), (byte) ('a' + 0), (byte) ('a' + 1)},
                new byte[] {(byte) ('a' + 2), (byte) ('a' + 2), (byte) ('a' + 2)},
                0, 3, true, true,
                new byte[] {(byte) ('a' + 0), (byte) ('a' + 1)}, new int[] {2, 1},
                new byte[] {(byte) ('a' + 2)}, new int[] {3});
    }

    @Test
    public void testFullyCancelled() {
        // identical pre/post: every value is both removed and added in equal quantity, so nothing survives
        check(new byte[] {(byte) ('a' + 0), (byte) ('a' + 0), (byte) ('a' + 1)},
                new byte[] {(byte) ('a' + 0), (byte) ('a' + 0), (byte) ('a' + 1)},
                0, 3, true, true,
                new byte[] {}, new int[] {},
                new byte[] {}, new int[] {});
    }

    @Test
    public void testPartialDeltaMoreRemoved() {
        // 'a' goes from 3 occurrences to 1 (net remove 2); 'b' and 'c' are wholly new
        check(new byte[] {(byte) ('a' + 0), (byte) ('a' + 0), (byte) ('a' + 0)},
                new byte[] {(byte) ('a' + 0), (byte) ('a' + 1), (byte) ('a' + 2)},
                0, 3, true, true,
                new byte[] {(byte) ('a' + 0)}, new int[] {2},
                new byte[] {(byte) ('a' + 1), (byte) ('a' + 2)}, new int[] {1, 1});
    }

    @Test
    public void testPartialDeltaMoreAdded() {
        // mirror image: 'a' goes from 1 to 3 (net add 2); 'b' and 'c' are wholly removed
        check(new byte[] {(byte) ('a' + 0), (byte) ('a' + 1), (byte) ('a' + 2)},
                new byte[] {(byte) ('a' + 0), (byte) ('a' + 0), (byte) ('a' + 0)},
                0, 3, true, true,
                new byte[] {(byte) ('a' + 1), (byte) ('a' + 2)}, new int[] {1, 1},
                new byte[] {(byte) ('a' + 0)}, new int[] {2});
    }

    @Test
    public void testNonZeroStart() {
        // values before start must be left untouched; survivors are written beginning at start
        final byte[] removed = new byte[] {(byte) ('a' + 3), (byte) ('a' + 3),
                (byte) ('a' + 0), (byte) ('a' + 0), (byte) ('a' + 1)};
        final byte[] added = new byte[] {(byte) ('a' + 3), (byte) ('a' + 3),
                (byte) ('a' + 2), (byte) ('a' + 2), (byte) ('a' + 2)};
        final int[] removedCounts = new int[removed.length];
        final int[] addedCounts = new int[added.length];

        final MutableInt removedSize = new MutableInt();
        final MutableInt addedSize = new MutableInt();
        ByteCompactModifications.compactAndCountModifications(
                WritableByteChunk.writableChunkWrap(removed), WritableIntChunk.writableChunkWrap(removedCounts),
                WritableByteChunk.writableChunkWrap(added), WritableIntChunk.writableChunkWrap(addedCounts),
                2, 3, 2, 3, true, true, removedSize, addedSize);

        assertEquals(2, removedSize.get());
        assertEquals(1, addedSize.get());
        // the prefix is untouched
        assertEquals((byte) ('a' + 3), removed[0]/*EXTRA*/);
        assertEquals((byte) ('a' + 3), removed[1]/*EXTRA*/);
        assertEquals((byte) ('a' + 3), added[0]/*EXTRA*/);
        assertEquals((byte) ('a' + 3), added[1]/*EXTRA*/);
        assertArrayEquals(new byte[] {(byte) ('a' + 0), (byte) ('a' + 1)},
                Arrays.copyOfRange(removed, 2, 2 + removedSize.get())/*EXTRA*/);
        assertArrayEquals(new int[] {2, 1}, Arrays.copyOfRange(removedCounts, 2, 2 + removedSize.get()));
        assertArrayEquals(new byte[] {(byte) ('a' + 2)},
                Arrays.copyOfRange(added, 2, 2 + addedSize.get())/*EXTRA*/);
        assertArrayEquals(new int[] {3}, Arrays.copyOfRange(addedCounts, 2, 2 + addedSize.get()));
    }

    @Test
    public void testNullIgnored() {
        // with countNull == false a null run is dropped even though its count changed (here removed 2, added 1)
        check(new byte[] {(byte) ('a' + 0), NULL_BYTE, NULL_BYTE},
                new byte[] {(byte) ('a' + 0), (byte) ('a' + 0), NULL_BYTE},
                0, 3, false, true,
                new byte[] {}, new int[] {},
                new byte[] {(byte) ('a' + 0)}, new int[] {1});
    }

    @Test
    public void testNullCounted() {
        // with countNull == true the null run participates: net remove one null, net add one 'a'
        check(new byte[] {NULL_BYTE, NULL_BYTE, (byte) ('a' + 0)},
                new byte[] {NULL_BYTE, (byte) ('a' + 0), (byte) ('a' + 0)},
                0, 3, true, true,
                new byte[] {NULL_BYTE}, new int[] {1},
                new byte[] {(byte) ('a' + 0)}, new int[] {1});
    }

    @Test
    public void testEmpty() {
        check(new byte[] {}, new byte[] {}, 0, 0, true, true,
                new byte[] {}, new int[] {},
                new byte[] {}, new int[] {});
    }

    @Test
    public void testDistinctRemovedAddedRanges() {
        // the removed and added ranges have independent starts and lengths (as when each side is a separate run of
        // flattened child deltas); survivors are written beginning at each side's own start
        final byte[] removed = new byte[] {(byte) ('a' + 3), (byte) ('a' + 1), (byte) ('a' + 0)};
        final byte[] added = new byte[] {(byte) ('a' + 3), (byte) ('a' + 3), (byte) ('a' + 3),
                (byte) ('a' + 0), (byte) ('a' + 0), (byte) ('a' + 2)};
        final int[] removedCounts = new int[removed.length];
        final int[] addedCounts = new int[added.length];

        final MutableInt removedSize = new MutableInt();
        final MutableInt addedSize = new MutableInt();
        ByteCompactModifications.compactAndCountModifications(
                WritableByteChunk.writableChunkWrap(removed), WritableIntChunk.writableChunkWrap(removedCounts),
                WritableByteChunk.writableChunkWrap(added), WritableIntChunk.writableChunkWrap(addedCounts),
                1, 2, 3, 3, true, true, removedSize, addedSize);

        // removed[1,2) = {b, a}; added[3,3) = {a, a, c}: 'a' cancels (removed 1, added 2 -> net add 1), 'b' is a pure
        // removal, 'c' is a pure addition
        assertEquals(1, removedSize.get());
        assertEquals(2, addedSize.get());
        // removed survivors begin at removedStart == 1
        assertEquals((byte) ('a' + 3), removed[0]/*EXTRA*/);
        assertArrayEquals(new byte[] {(byte) ('a' + 1)}, Arrays.copyOfRange(removed, 1, 1 + removedSize.get())/*EXTRA*/);
        assertArrayEquals(new int[] {1}, Arrays.copyOfRange(removedCounts, 1, 1 + removedSize.get()));
        // added survivors begin at addedStart == 3
        assertArrayEquals(new byte[] {(byte) ('a' + 0), (byte) ('a' + 2)},
                Arrays.copyOfRange(added, 3, 3 + addedSize.get())/*EXTRA*/);
        assertArrayEquals(new int[] {1, 1}, Arrays.copyOfRange(addedCounts, 3, 3 + addedSize.get()));
    }

    private static void check(byte[] removed, byte[] added, int start, int length, boolean countNull,
            boolean countNaN, byte[] expectedRemoved, int[] expectedRemovedCounts, byte[] expectedAdded,
            int[] expectedAddedCounts) {
        final int[] removedCounts = new int[removed.length];
        final int[] addedCounts = new int[added.length];

        final MutableInt removedSize = new MutableInt();
        final MutableInt addedSize = new MutableInt();
        ByteCompactModifications.compactAndCountModifications(
                WritableByteChunk.writableChunkWrap(removed), WritableIntChunk.writableChunkWrap(removedCounts),
                WritableByteChunk.writableChunkWrap(added), WritableIntChunk.writableChunkWrap(addedCounts),
                start, length, start, length, countNull, countNaN, removedSize, addedSize);

        assertEquals(expectedRemoved.length, removedSize.get());
        assertEquals(expectedAdded.length, addedSize.get());
        assertArrayEquals(expectedRemoved, Arrays.copyOfRange(removed, start, start + removedSize.get())/*EXTRA*/);
        assertArrayEquals(expectedRemovedCounts, Arrays.copyOfRange(removedCounts, start, start + removedSize.get()));
        assertArrayEquals(expectedAdded, Arrays.copyOfRange(added, start, start + addedSize.get())/*EXTRA*/);
        assertArrayEquals(expectedAddedCounts, Arrays.copyOfRange(addedCounts, start, start + addedSize.get()));
    }
}

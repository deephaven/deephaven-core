//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.rowset.impl;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.function.LongPredicate;
import java.util.function.Supplier;

import static io.deephaven.engine.rowset.impl.RowSetTestCommon.keysOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.rspOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.singleRangeOf;
import static io.deephaven.engine.rowset.impl.RowSetTestCommon.sortedRangesOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * {@link RowSet#invert(io.deephaven.chunk.LongChunk, WritableLongChunk)} produces the same positions as
 * {@link RowSet#invert(RowSet)} for every backing.
 */
public class RowSetChunkInvertTest {

    private static List<Supplier<WritableRowSet>> rowSets() {
        // strided keys across several RSP blocks, a full block, and a run inside a block
        final List<long[]> rspRanges = new ArrayList<>();
        for (long key = 0; key < 200_000; key += 7) {
            rspRanges.add(new long[] {key, key});
        }
        rspRanges.add(new long[] {1L << 20, (1L << 20) + 3 * 65536 - 1});
        rspRanges.add(new long[] {1L << 22, (1L << 22) + 99});
        final long[][] rsp = rspRanges.toArray(long[][]::new);
        return List.of(
                () -> singleRangeOf(100, 10_000),
                () -> sortedRangesOf(new long[] {0, 0}, new long[] {8, 8}, new long[] {16, 20},
                        new long[] {100, 163}, new long[] {1000, 1000}),
                () -> rspOf(rsp));
    }

    private static WritableRowSet keysWhere(final RowSet rowSet, final LongPredicate predicate) {
        final RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        rowSet.forAllRowKeys(key -> {
            if (predicate.test(key)) {
                builder.appendKey(key);
            }
        });
        return builder.build();
    }

    private static void assertInvertMatches(final String what, final RowSet rowSet, final RowSet keys) {
        try (final WritableRowSet expected = rowSet.invert(keys);
                final WritableLongChunk<OrderedRowKeys> positions =
                        WritableLongChunk.makeWritableChunk(Math.max(1, keys.intSize()))) {
            rowSet.invert(keys.asRowKeyChunk(), positions);
            final List<Long> actual = new ArrayList<>(positions.size());
            for (int ii = 0; ii < positions.size(); ++ii) {
                actual.add(positions.get(ii));
            }
            assertEquals(what, keysOf(expected), actual);
        }
    }

    @Test
    public void testMatchesRowSetInvert() {
        for (final Supplier<WritableRowSet> supplier : rowSets()) {
            final Random random = new Random(0);
            try (final WritableRowSet rowSet = supplier.get();
                    final WritableRowSet everyThird = keysWhere(rowSet, key -> key % 3 == 0);
                    final WritableRowSet randomTenth = keysWhere(rowSet, key -> random.nextInt(10) == 0);
                    final WritableRowSet single = RowSetFactory.fromKeys(rowSet.get(rowSet.size() / 2));
                    final WritableRowSet empty = RowSetFactory.empty()) {
                final String name = ((WritableRowSetImpl) rowSet).getInnerSet().getClass().getSimpleName();
                assertInvertMatches(name + " all keys", rowSet, rowSet);
                assertInvertMatches(name + " every third key", rowSet, everyThird);
                assertInvertMatches(name + " random tenth", rowSet, randomTenth);
                assertInvertMatches(name + " single key", rowSet, single);
                assertInvertMatches(name + " no keys", rowSet, empty);
            }
        }
    }

    @Test
    public void testInvertInPlace() {
        for (final Supplier<WritableRowSet> supplier : rowSets()) {
            final Random random = new Random(0);
            try (final WritableRowSet rowSet = supplier.get();
                    final WritableRowSet randomTenth = keysWhere(rowSet, key -> random.nextInt(10) == 0);
                    final WritableRowSet expected = rowSet.invert(randomTenth);
                    final WritableLongChunk<OrderedRowKeys> chunk =
                            WritableLongChunk.makeWritableChunk(randomTenth.intSize())) {
                final String name = ((WritableRowSetImpl) rowSet).getInnerSet().getClass().getSimpleName();
                randomTenth.fillRowKeyChunk(chunk);
                rowSet.invert(chunk, chunk);
                final List<Long> actual = new ArrayList<>(chunk.size());
                for (int ii = 0; ii < chunk.size(); ++ii) {
                    actual.add(chunk.get(ii));
                }
                assertEquals(name + " in place", keysOf(expected), actual);
            }
        }
    }

    @Test
    public void testBuilderAppendsRangeEndingAtMaxValue() {
        try (final WritableLongChunk<OrderedRowKeys> chunk = WritableLongChunk.makeWritableChunk(3)) {
            final RowKeysChunkBuilderSequential builder = new RowKeysChunkBuilderSequential(chunk);
            builder.appendRange(Long.MAX_VALUE - 2, Long.MAX_VALUE);
            assertEquals(3, chunk.size());
            assertEquals(Long.MAX_VALUE - 2, chunk.get(0));
            assertEquals(Long.MAX_VALUE - 1, chunk.get(1));
            assertEquals(Long.MAX_VALUE, chunk.get(2));
        }
    }

    @Test
    public void testMissingKeyIsRejected() {
        for (final Supplier<WritableRowSet> supplier : rowSets()) {
            try (final WritableRowSet rowSet = supplier.get();
                    final WritableRowSet keys = RowSetFactory.fromKeys(rowSet.firstRowKey(), rowSet.lastRowKey() + 1);
                    final WritableLongChunk<OrderedRowKeys> positions = WritableLongChunk.makeWritableChunk(2)) {
                final String name = ((WritableRowSetImpl) rowSet).getInnerSet().getClass().getSimpleName();
                try {
                    rowSet.invert(keys.asRowKeyChunk(), positions);
                    fail(name + " accepted a key that is not in the set");
                } catch (IllegalArgumentException | IllegalStateException expected) {
                    // the same rejections as the RowSet-producing invert
                }
            }
        }
    }
}

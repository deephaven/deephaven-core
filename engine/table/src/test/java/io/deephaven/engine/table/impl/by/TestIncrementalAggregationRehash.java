//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.by;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.RowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.by.typed.TypedHasherFactory;
import io.deephaven.engine.table.impl.sources.ArrayBackedColumnSource;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.mutable.MutableInt;
import org.junit.Rule;
import org.junit.Test;

import java.lang.ref.WeakReference;
import java.util.stream.LongStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestIncrementalAggregationRehash {
    @Rule
    public final EngineCleanup base = new EngineCleanup();

    /**
     * Once an incremental rehash has moved every entry out of the old table, nothing holds the old table's keys, though
     * the result's key columns read the hash table through alternating sources.
     */
    @Test
    public void testDrainedAlternateKeysAreReleased() {
        final int initialTableSize = 1024;
        final int beforeRehash = 700;
        final int added = 100;
        final ColumnSource<?>[] keySources = new ColumnSource[] {
                ArrayBackedColumnSource.getMemoryColumnSource(LongStream.range(0, beforeRehash + added).toArray())};
        final IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase manager = TypedHasherFactory.make(
                IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase.class, keySources, keySources,
                initialTableSize, 0.75, 0.70);
        // the result's key columns hold the alternating sources
        final ColumnSource<?>[] resultKeySources = manager.getKeyHashTableSources();
        manager.beginUpdateCycle();

        final MutableInt nextOutputPosition = new MutableInt(0);
        try (final RowSet first = RowSetFactory.fromRange(0, beforeRehash - 1);
                final RowSet second = RowSetFactory.fromRange(beforeRehash, beforeRehash + added - 1)) {
            add(manager, keySources, first, nextOutputPosition);
            assertEquals(0, manager.rehashPointer);
            // crossing the maximum load factor starts an incremental rehash into a table twice the size
            add(manager, keySources, second, nextOutputPosition);
        }
        assertEquals(2 * initialTableSize, manager.tableSize);
        assertTrue(manager.rehashPointer > 0);
        final WeakReference<ColumnSource<?>> drainedKeys = new WeakReference<>(manager.alternateKeySources[0]);

        // the next cycle's rehash work moves the rest of the entries
        manager.beginUpdateCycle();
        assertEquals(0, manager.rehashPointer);
        assertEquals(2 * initialTableSize, manager.tableSize);

        for (int attempt = 0; attempt < 10 && drainedKeys.get() != null; ++attempt) {
            System.gc();
        }
        assertNull(drainedKeys.get());
        assertEquals(beforeRehash + added, nextOutputPosition.get());
        // the result's key sources were reachable throughout
        assertEquals(1, resultKeySources.length);
    }

    private static void add(final IncrementalChunkedOperatorAggregationStateManagerOpenAddressedBase manager,
            final ColumnSource<?>[] keySources, final RowSet rows, final MutableInt nextOutputPosition) {
        try (final SafeCloseable bc = manager.makeAggregationStateBuildContext(keySources, rows.size());
                final WritableIntChunk<RowKeys> outputPositions = WritableIntChunk.makeWritableChunk(rows.intSize())) {
            manager.add(bc, rows, keySources, nextOutputPosition, outputPositions);
        }
    }
}

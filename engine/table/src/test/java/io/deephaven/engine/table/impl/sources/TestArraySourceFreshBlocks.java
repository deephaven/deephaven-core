//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.engine.testutil.junit4.EngineCleanup;
import io.deephaven.util.QueryConstants;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * When a block is allocated during an update cycle, there are no previous values to preserve. Therefore, all
 * freshly-allocated blocks share read-only arrays for their prevBlocks and prevInUse entries. There is a single in-use
 * array, and one previous-value array for each element type and shape, where the shape is whether the block is
 * null-filled or zeroed.
 */
public class TestArraySourceFreshBlocks {
    private static final int BLOCK_SIZE = ArrayBackedColumnSource.BLOCK_SIZE;

    @Rule
    public final EngineCleanup base = new EngineCleanup();

    private static ControlledUpdateGraph updateGraph() {
        return ExecutionContext.getContext().getUpdateGraph().cast();
    }

    @Test
    public void testPreviousValuesOfBlocksAllocatedDuringACycle() {
        final LongArraySource longs = new LongArraySource();
        final LongArraySource defaults = new LongArraySource();
        final ObjectArraySource<String> objects = new ObjectArraySource<>(String.class);
        final BooleanArraySource booleans = new BooleanArraySource();
        // one block before the cycle, whose previous values are recorded as usual
        longs.ensureCapacity(BLOCK_SIZE);
        defaults.ensureCapacity(BLOCK_SIZE, false);
        objects.ensureCapacity(BLOCK_SIZE);
        booleans.ensureCapacity(BLOCK_SIZE);
        for (int ii = 0; ii < BLOCK_SIZE; ++ii) {
            longs.set(ii, 7L);
            defaults.set(ii, 7L);
            objects.set(ii, "old");
            booleans.set(ii, true);
        }
        longs.startTrackingPrevValues();
        defaults.startTrackingPrevValues();
        objects.startTrackingPrevValues();
        booleans.startTrackingPrevValues();

        updateGraph().runWithinUnitTestCycle(() -> {
            // a second block, allocated during the cycle
            longs.ensureCapacity(2L * BLOCK_SIZE);
            defaults.ensureCapacity(2L * BLOCK_SIZE, false);
            objects.ensureCapacity(2L * BLOCK_SIZE);
            booleans.ensureCapacity(2L * BLOCK_SIZE);
            for (long key = 0; key < 2L * BLOCK_SIZE; key += 3) {
                longs.set(key, key);
                defaults.set(key, key);
                objects.set(key, "new");
                booleans.set(key, false);
            }
            for (long key = 0; key < 2L * BLOCK_SIZE; ++key) {
                final boolean written = key % 3 == 0;
                final boolean fresh = key >= BLOCK_SIZE;
                // written rows of the old block record their old values; rows of the new block keep their allocated
                // ones
                assertEquals(fresh ? QueryConstants.NULL_LONG : 7L, longs.getPrevLong(key));
                assertEquals(fresh ? 0L : 7L, defaults.getPrevLong(key));
                assertEquals(fresh ? null : "old", objects.getPrev(key));
                assertEquals(fresh ? null : Boolean.TRUE, booleans.getPrev(key));
                assertEquals(written ? key : fresh ? QueryConstants.NULL_LONG : 7L, longs.getLong(key));
                assertEquals(written ? key : fresh ? 0L : 7L, defaults.getLong(key));
                assertEquals(written ? "new" : fresh ? null : "old", objects.get(key));
                assertEquals(written ? Boolean.FALSE : fresh ? null : Boolean.TRUE, booleans.get(key));
            }
        });

        // after the cycle, previous values are current values, and the new block records previous values as usual
        for (long key = BLOCK_SIZE; key < 2L * BLOCK_SIZE; ++key) {
            assertEquals(longs.getLong(key), longs.getPrevLong(key));
            assertEquals(defaults.getLong(key), defaults.getPrevLong(key));
            assertEquals(objects.get(key), objects.getPrev(key));
            assertEquals(booleans.get(key), booleans.getPrev(key));
        }
        updateGraph().runWithinUnitTestCycle(() -> {
            // a row of the new block that the first cycle wrote
            final long key = BLOCK_SIZE + 1;
            longs.set(key, -1L);
            defaults.set(key, 9L);
            objects.set(key, "newer");
            booleans.set(key, Boolean.TRUE);
            assertEquals(key, longs.getPrevLong(key));
            assertEquals(key, defaults.getPrevLong(key));
            assertEquals("new", objects.getPrev(key));
            assertEquals(Boolean.FALSE, booleans.getPrev(key));
        });
    }

    @Test
    public void testEveryBlockAllocatedDuringACycleIsCleared() {
        final LongArraySource longs = new LongArraySource();
        longs.startTrackingPrevValues();
        // two allocations in one cycle, each of a new block
        updateGraph().runWithinUnitTestCycle(() -> {
            longs.ensureCapacity(BLOCK_SIZE);
            longs.set(0, 1L);
            longs.ensureCapacity(2L * BLOCK_SIZE);
            longs.set(BLOCK_SIZE, 2L);
        });
        // both blocks record previous values in the next cycle
        updateGraph().runWithinUnitTestCycle(() -> {
            longs.set(0, -1L);
            longs.set(BLOCK_SIZE, -2L);
            assertEquals(1L, longs.getPrevLong(0));
            assertEquals(2L, longs.getPrevLong(BLOCK_SIZE));
        });
    }

    @Test
    public void testParallelPopulationLeavesTheSharedBlockUnwritten() {
        final LongArraySource populated = new LongArraySource();
        final LongArraySource other = new LongArraySource();
        final ObjectArraySource<String> populatedObjects = new ObjectArraySource<>(String.class);
        final ObjectArraySource<String> otherObjects = new ObjectArraySource<>(String.class);
        populated.startTrackingPrevValues();
        other.startTrackingPrevValues();
        populatedObjects.startTrackingPrevValues();
        otherObjects.startTrackingPrevValues();

        updateGraph().runWithinUnitTestCycle(() -> {
            populated.ensureCapacity(BLOCK_SIZE);
            populatedObjects.ensureCapacity(BLOCK_SIZE);
            for (int ii = 0; ii < BLOCK_SIZE; ++ii) {
                populated.set(ii, 5L);
                populatedObjects.set(ii, "value");
            }
            // parallel population of a block allocated this cycle must not copy its values into the shared block
            populated.prepareForParallelPopulation(RowSetFactory.flat(BLOCK_SIZE));
            populatedObjects.prepareForParallelPopulation(RowSetFactory.flat(BLOCK_SIZE));
            other.ensureCapacity(BLOCK_SIZE);
            otherObjects.ensureCapacity(BLOCK_SIZE);
            for (int ii = 0; ii < BLOCK_SIZE; ++ii) {
                assertEquals(QueryConstants.NULL_LONG, populated.getPrevLong(ii));
                assertEquals(QueryConstants.NULL_LONG, other.getPrevLong(ii));
                assertNull(populatedObjects.getPrev(ii));
                assertNull(otherObjects.getPrev(ii));
            }
        });
    }
}

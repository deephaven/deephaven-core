/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterDeltaAwareColumnSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.deltaaware;

import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.chunk.ArrayGenerator;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderSequential;
import io.deephaven.engine.rowset.RowSetFactory;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import static junit.framework.TestCase.*;

public class TestObjectDeltaAwareColumnSource {
    @Before
    public void setUp() throws Exception {
        UpdateGraphProcessor.DEFAULT.enableUnitTestMode();
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(false);
    }

    @After
    public void tearDown() throws Exception {
        UpdateGraphProcessor.DEFAULT.resetForUnitTests(true);
    }

    @Test
    public void simple1() {
        final Random rng = new Random(832952914);
        final long key0 = 5;
        final long key1 = 6;
        final Object expected1 = ArrayGenerator.randomObjects(rng, 1)[0];

        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        final DeltaAwareColumnSource<Object> source = new DeltaAwareColumnSource<>(Object.class);
        source.ensureCapacity(10);

        source.set(key1, expected1);
        final Object actual0 = source.get(key0);
        final Object actual1 = source.get(key1);
        assertEquals(null, actual0);
        assertEquals(expected1, actual1);
        UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    }

    @Test
    public void simple2() {
        final Random rng = new Random(275128810);
        final long key0 = 5;
        final long key1 = 6;
        final Object[] values = ArrayGenerator.randomObjects(rng, 3);
        final Object expected0_0 = values[0];
        final Object expected0_1 = values[1];
        final Object expected1 = values[2];
        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        final DeltaAwareColumnSource<Object> source = new DeltaAwareColumnSource<>(Object.class);
        source.ensureCapacity(10);
        source.set(key0, expected0_0);
        UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();

        source.startTrackingPrevValues();

        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        source.set(key0, expected0_1);
        source.set(key1, expected1);

        final Object actual0_0 = source.getPrev(key0);
        final Object actual0_1 = source.get(key0);
        final Object actual1_0 = source.getPrev(key1);
        final Object actual1_1 = source.get(key1);

        assertEquals(expected0_0, actual0_0);
        assertEquals(expected0_1, actual0_1);
        assertEquals(null, actual1_0);
        assertEquals(expected1, actual1_1);

        UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    }

    /**
     * We make a structure that looks like this. Then we query the whole thing with one range and see if we get what
     * we expect. Then we query with three subranges and again see if we get what we expect. Then in a second generation,
     * we write some new values, which creates a baseline/delta situation. We do those same queries again (note that
     * the subranges have been carefully chosen to span baseline and delta, so they're challenging) and again see if we
     * get what we expect.
     * Pictorially, the situation looks like this (best viewed with a monospace font).
     * baseline: BBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBBB
     * query1:   ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
     * query2:             ^^^^^^^^^^^^^^^^^^^^               ^^^^^^^^^^               ^^^^^^^^^^^^^^^^^^^^
     * delta:                        DDDDDDDDDDDDDDDDDDDD                    DDDDDDDDDDDDDDDDDDDD
     */
    @Test
    public void overlapping() {
        final Random rng = new Random(912366186);
        final int length = 100;
        final Object[] valuesPhase1 = ArrayGenerator.randomObjects(rng, length);
        final Object[] valuesPhase2 = ArrayGenerator.randomObjects(rng, length);
        final HashMap<Long, Object> expectedPrev = new HashMap<>();
        final HashMap<Long, Object> expectedCurrent = new HashMap<>();
        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        final DeltaAwareColumnSource<Object> source = new DeltaAwareColumnSource<>(Object.class);
        source.ensureCapacity(length);
        for (long ii = 0; ii < length; ++ii) {
            final Object value = valuesPhase1[(int)ii];
            source.set(ii, value);
            expectedPrev.put(ii, value);
            expectedCurrent.put(ii, value);
        }
        source.startTrackingPrevValues();
        // Check the whole range using individual "get" calls.
        checkUsingGet(source, expectedCurrent, expectedPrev, 0, length);
        // Check the whole range in a chunked fashion using a single range.
        final long[] singleRange = {0, length};
        checkUsingChunk(source, expectedCurrent, expectedPrev, singleRange);

        // Check some subranges using three ranges.
        final long[] threeRanges = {10, 30, 45, 55, 70, 90};
        checkUsingChunk(source, expectedCurrent, expectedPrev, threeRanges);
        UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();

        // Now start the second cycle so we have different current and prev values.
        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        for (long ii = 20; ii < 40; ++ii) {
            final Object value = valuesPhase2[(int)ii];
            source.set(ii, value);
            expectedCurrent.put(ii, value);
        }
        for (long ii = 60; ii < 80; ++ii) {
            final Object value = valuesPhase2[(int)ii];
            source.set(ii, value);
            expectedCurrent.put(ii, value);
        }
        checkUsingGet(source, expectedCurrent, expectedPrev, 0, length);
        checkUsingChunk(source, expectedCurrent, expectedPrev, singleRange);
        checkUsingChunk(source, expectedCurrent, expectedPrev, threeRanges);
        UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    }

    private static void checkUsingGet(DeltaAwareColumnSource<Object> source, Map<Long, Object> expectedCurrent,
                                      Map<Long, Object> expectedPrev, int begin, int end) {
        // Check the whole thing by using individual get calls: current and prev.
        // current...
        for (long ii = begin; ii < end; ++ii) {
            final Object expectedValue = expectedCurrent.get(ii);
            final Object actualValue = source.get(ii);
            assertEquals(expectedValue, actualValue);
        }
        // prev...
        for (long ii = begin; ii < end; ++ii) {
            final Object expectedValue = expectedPrev.get(ii);
            final Object actualValue = source.getPrev(ii);
            assertEquals(expectedValue, actualValue);
        }
    }

    private static void checkUsingChunk(DeltaAwareColumnSource<Object> dacs, Map<Long, Object> expectedCurrent,
                                        Map<Long, Object> expectedPrev, long[] ranges) {
        final RowSet rowSet = rangesToIndex(ranges);
        assertEquals(rowSet.size() % 2, 0);

        // We're going to get creative and pull down the data as two slices
        final int chunkSize = (int) (rowSet.size() / 2);

        // So we'll also split the RowSet in half
        final RowSet rowSet0 = rowSet.subSetByPositionRange(0, chunkSize);
        final RowSet rowSet1 = rowSet.subSetByPositionRange(chunkSize, rowSet.size());

        // Current...
        try (ChunkSource.GetContext context = dacs.makeGetContext(chunkSize)) {
            ObjectChunk<?, ? extends Values> chunk;

            chunk = dacs.getChunk(context, rowSet0).asObjectChunk();
            checkChunk(chunk, expectedCurrent, rowSet0);
            chunk = dacs.getChunk(context, rowSet1).asObjectChunk();
            checkChunk(chunk, expectedCurrent, rowSet1);

            chunk = dacs.getPrevChunk(context, rowSet0).asObjectChunk();
            checkChunk(chunk, expectedPrev, rowSet0);
            chunk = dacs.getPrevChunk(context, rowSet1).asObjectChunk();
            checkChunk(chunk, expectedPrev, rowSet1);
        }
    }

    private static void checkChunk(ObjectChunk<?, ? extends Values> values, Map<Long, Object> expected, RowSet keys) {
        int sliceOffset = 0;
        for (final RowSet.Iterator it = keys.iterator(); it.hasNext(); ) {
            final long key = it.nextLong();
            final Object expectedValue = expected.get(key);
            final Object actualValue = values.get(sliceOffset++);
            assertEquals(expectedValue, actualValue);
        }
    }

    private static RowSet rangesToIndex(long[] ranges) {
        RowSetBuilderSequential builder = RowSetFactory.builderSequential();
        for (int ii = 0; ii < ranges.length; ii += 2) {
            builder.appendRange(ranges[ii], ranges[ii + 1] - 1);
        }
        return builder.build();
    }
}

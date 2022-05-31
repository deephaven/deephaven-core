/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit TestCharacterDeltaAwareColumnSource and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.table.impl.sources.deltaaware;

import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.chunk.ArrayGenerator;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.chunk.IntChunk;
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

import static io.deephaven.util.QueryConstants.*;
import static junit.framework.TestCase.*;

public class TestIntegerDeltaAwareColumnSource {
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
        final int expected1 = ArrayGenerator.randomInts(rng, 1)[0];

        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        final DeltaAwareColumnSource<Integer> source = new DeltaAwareColumnSource<>(int.class);
        source.ensureCapacity(10);

        source.set(key1, expected1);
        final int actual0 = source.getInt(key0);
        final int actual1 = source.getInt(key1);
        assertEquals(NULL_INT, actual0);
        assertEquals(expected1, actual1);
        UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    }

    @Test
    public void simple2() {
        final Random rng = new Random(275128810);
        final long key0 = 5;
        final long key1 = 6;
        final int[] values = ArrayGenerator.randomInts(rng, 3);
        final int expected0_0 = values[0];
        final int expected0_1 = values[1];
        final int expected1 = values[2];
        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        final DeltaAwareColumnSource<Integer> source = new DeltaAwareColumnSource<>(int.class);
        source.ensureCapacity(10);
        source.set(key0, expected0_0);
        UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();

        source.startTrackingPrevValues();

        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        source.set(key0, expected0_1);
        source.set(key1, expected1);

        final int actual0_0 = source.getPrevInt(key0);
        final int actual0_1 = source.getInt(key0);
        final int actual1_0 = source.getPrevInt(key1);
        final int actual1_1 = source.getInt(key1);

        assertEquals(expected0_0, actual0_0);
        assertEquals(expected0_1, actual0_1);
        assertEquals(NULL_INT, actual1_0);
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
        final int[] valuesPhase1 = ArrayGenerator.randomInts(rng, length);
        final int[] valuesPhase2 = ArrayGenerator.randomInts(rng, length);
        final HashMap<Long, Integer> expectedPrev = new HashMap<>();
        final HashMap<Long, Integer> expectedCurrent = new HashMap<>();
        UpdateGraphProcessor.DEFAULT.startCycleForUnitTests();
        final DeltaAwareColumnSource<Integer> source = new DeltaAwareColumnSource<>(int.class);
        source.ensureCapacity(length);
        for (long ii = 0; ii < length; ++ii) {
            final int value = valuesPhase1[(int)ii];
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
            final int value = valuesPhase2[(int)ii];
            source.set(ii, value);
            expectedCurrent.put(ii, value);
        }
        for (long ii = 60; ii < 80; ++ii) {
            final int value = valuesPhase2[(int)ii];
            source.set(ii, value);
            expectedCurrent.put(ii, value);
        }
        checkUsingGet(source, expectedCurrent, expectedPrev, 0, length);
        checkUsingChunk(source, expectedCurrent, expectedPrev, singleRange);
        checkUsingChunk(source, expectedCurrent, expectedPrev, threeRanges);
        UpdateGraphProcessor.DEFAULT.completeCycleForUnitTests();
    }

    private static void checkUsingGet(DeltaAwareColumnSource<Integer> source, Map<Long, Integer> expectedCurrent,
                                      Map<Long, Integer> expectedPrev, int begin, int end) {
        // Check the whole thing by using individual get calls: current and prev.
        // current...
        for (long ii = begin; ii < end; ++ii) {
            final int expectedValue = expectedCurrent.get(ii);
            final int actualValue = source.getInt(ii);
            assertEquals(expectedValue, actualValue);
        }
        // prev...
        for (long ii = begin; ii < end; ++ii) {
            final int expectedValue = expectedPrev.get(ii);
            final int actualValue = source.getPrevInt(ii);
            assertEquals(expectedValue, actualValue);
        }
    }

    private static void checkUsingChunk(DeltaAwareColumnSource<Integer> dacs, Map<Long, Integer> expectedCurrent,
                                        Map<Long, Integer> expectedPrev, long[] ranges) {
        final RowSet rowSet = rangesToIndex(ranges);
        assertEquals(rowSet.size() % 2, 0);

        // We're going to get creative and pull down the data as two slices
        final int chunkSize = (int) (rowSet.size() / 2);

        // So we'll also split the RowSet in half
        final RowSet rowSet0 = rowSet.subSetByPositionRange(0, chunkSize);
        final RowSet rowSet1 = rowSet.subSetByPositionRange(chunkSize, rowSet.size());

        // Current...
        try (ChunkSource.GetContext context = dacs.makeGetContext(chunkSize)) {
            IntChunk<? extends Values> chunk;

            chunk = dacs.getChunk(context, rowSet0).asIntChunk();
            checkChunk(chunk, expectedCurrent, rowSet0);
            chunk = dacs.getChunk(context, rowSet1).asIntChunk();
            checkChunk(chunk, expectedCurrent, rowSet1);

            chunk = dacs.getPrevChunk(context, rowSet0).asIntChunk();
            checkChunk(chunk, expectedPrev, rowSet0);
            chunk = dacs.getPrevChunk(context, rowSet1).asIntChunk();
            checkChunk(chunk, expectedPrev, rowSet1);
        }
    }

    private static void checkChunk(IntChunk<? extends Values> values, Map<Long, Integer> expected, RowSet keys) {
        int sliceOffset = 0;
        for (final RowSet.Iterator it = keys.iterator(); it.hasNext(); ) {
            final long key = it.nextLong();
            final int expectedValue = expected.get(key);
            final int actualValue = values.get(sliceOffset++);
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

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestCharacterSparseArraySource and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.sources;

import io.deephaven.chunk.ObjectChunk;

import io.deephaven.chunk.WritableObjectChunk;

import io.deephaven.util.BooleanUtils;

import io.deephaven.chunk.ArrayGenerator;
import io.deephaven.chunk.Chunk;
import io.deephaven.engine.context.ExecutionContext;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.ChunkSink;
import io.deephaven.engine.table.ChunkSource;
import io.deephaven.engine.table.WritableColumnSource;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.testutil.ControlledUpdateGraph;
import io.deephaven.util.mutable.MutableInt;
import org.jetbrains.annotations.NotNull;
import org.junit.Test;

import java.util.Arrays;
import java.util.Random;


import static org.junit.Assert.*;


public class TestBooleanSparseArraySource extends AbstractBooleanColumnSourceTest {
    @NotNull
    @Override
    BooleanSparseArraySource makeTestSource() {
        return new BooleanSparseArraySource();
    }

    @Test
    public void confirmAliasingForbidden() {
        final Random rng = new Random(438269476);
        final int arraySize = 100;
        final int rangeStart = 20;
        final int rangeEnd = 80;
        final BooleanSparseArraySource source = makeTestSource();
        source.ensureCapacity(arraySize);

        final byte[] data = ArrayGenerator.randomBooleans(rng, arraySize);
        for (int ii = 0; ii < data.length; ++ii) {
            source.set(ii, data[ii]);
        }
        // super hack
        final byte[] peekedBlock = source.ensureBlock(0, 0, 0);

        try (RowSet srcKeys = RowSetFactory.fromRange(rangeStart, rangeEnd)) {
            try (RowSet destKeys = RowSetFactory.fromRange(rangeStart + 1, rangeEnd + 1)) {
                try (ChunkSource.GetContext srcContext = source.makeGetContext(arraySize)) {
                    try (ChunkSink.FillFromContext destContext = source.makeFillFromContext(arraySize)) {
                        Chunk chunk = source.getChunk(srcContext, srcKeys);
                        if (chunk.isAlias(peekedBlock)) {
                            // If the ArraySource gives out aliases of its blocks, then it should throw when we try to
                            // fill from that aliased chunk
                            boolean testFailed;
                            try {
                                source.fillFromChunk(destContext, chunk, destKeys);
                                testFailed = true;
                            } catch (UnsupportedOperationException uoe) {
                                testFailed = false;
                            }
                            assertFalse(testFailed);
                        }
                    }
                }
            }
        }
    }

    /**
     * Shifts a mix of scattered row keys and a run, more than one chunk of them, up and down by deltas both smaller and
     * larger than a chunk, checking within each cycle that every row key near them holds its shifted value and its
     * value before the shift as its previous value, and that row keys outside the shifted rows are untouched.
     */
    @Test
    public void testShiftRecordsPreviousValues() {
        final Random random = new Random(0);
        final int span = 40_000;
        final long windowStart = 1L << 20;
        final long shiftedStart = windowStart + span;
        final int windowSize = 3 * span;

        final RowSetBuilderRandom keysBuilder = RowSetFactory.builderRandom();
        for (int ii = 0; ii < span; ++ii) {
            if (random.nextInt(4) == 0 || (ii >= 9_000 && ii < 12_000)) {
                keysBuilder.addKey(shiftedStart + ii);
            }
        }
        WritableRowSet keys = keysBuilder.build();
        assertTrue(keys.size() > 4096);

        final BooleanSparseArraySource source = makeTestSource();
        final byte[] values = ArrayGenerator.randomBooleans(random, keys.intSize());
        final MutableInt valueIndex = new MutableInt(0);
        keys.forAllRowKeys(key -> source.set(key, values[valueIndex.getAndIncrement()]));
        final long beforeWindow = windowStart - 5;
        final long afterWindow = windowStart + windowSize + 5;
        final byte[] untouched = ArrayGenerator.randomBooleans(random, 2);
        source.set(beforeWindow, untouched[0]);
        source.set(afterWindow, untouched[1]);
        // the expected values are read back boxed, so that every element type compares them the same way
        Object[] expected = new Object[windowSize];
        for (int ii = 0; ii < windowSize; ++ii) {
            expected[ii] = source.get(windowStart + ii);
        }
        final Object expectedBefore = source.get(beforeWindow);
        final Object expectedAfter = source.get(afterWindow);

        source.startTrackingPrevValues();
        final ControlledUpdateGraph updateGraph = ExecutionContext.getContext().getUpdateGraph().cast();
        for (final long shiftDelta : new long[] {1, -1, 3, 4096 + 7, -(4096 + 7), -2}) {
            final Object[] previous = expected;
            final Object[] current = Arrays.copyOf(previous, windowSize);
            keys.forAllRowKeys(key -> current[(int) (key - windowStart)] = null);
            keys.forAllRowKeys(key -> current[(int) (key + shiftDelta - windowStart)] =
                    previous[(int) (key - windowStart)]);

            updateGraph.startCycleForUnitTests();
            source.shift(keys, shiftDelta);
            for (int ii = 0; ii < windowSize; ++ii) {
                assertEquals(current[ii], source.get(windowStart + ii));
                assertEquals(previous[ii], source.getPrev(windowStart + ii));
            }
            assertEquals(expectedBefore, source.get(beforeWindow));
            assertEquals(expectedAfter, source.get(afterWindow));
            updateGraph.completeCycleForUnitTests();

            final WritableRowSet shiftedKeys = keys.shift(shiftDelta);
            keys.close();
            keys = shiftedKeys;
            expected = current;
        }
        keys.close();
    }
}

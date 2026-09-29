//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import io.deephaven.chunk.WritableBooleanChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ChunkFilter#TRUE_FILTER_INSTANCE} and {@link ChunkFilter#FALSE_FILTER_INSTANCE}: like every other
 * chunk filter, both {@code filter} and {@code filterAnd} return the number of values left true (DH-23750, PD-053).
 */
public class ConstantChunkFilterTest {

    private static final int CHUNK_SIZE = 10;
    /** How many leading entries are true in the incoming results chunk, as a prior filter would have left it. */
    private static final int INITIALLY_TRUE = 6;

    private static WritableBooleanChunk<Values> resultsChunk() {
        final WritableBooleanChunk<Values> results = WritableBooleanChunk.makeWritableChunk(CHUNK_SIZE);
        for (int ii = 0; ii < CHUNK_SIZE; ++ii) {
            results.set(ii, ii < INITIALLY_TRUE);
        }
        return results;
    }

    /** Values are irrelevant to the constant filters; any chunk of the right size will do. */
    private static WritableIntChunk<Values> valuesChunk() {
        final WritableIntChunk<Values> values = WritableIntChunk.makeWritableChunk(CHUNK_SIZE);
        values.fillWithValue(0, CHUNK_SIZE, 0);
        return values;
    }

    private static void assertBits(final WritableBooleanChunk<Values> results, final int trueCount) {
        for (int ii = 0; ii < CHUNK_SIZE; ++ii) {
            assertEquals("result " + ii, ii < trueCount, results.get(ii));
        }
    }

    @Test
    public void falseFilter() {
        try (final WritableIntChunk<Values> values = valuesChunk();
                final WritableBooleanChunk<Values> results = resultsChunk()) {
            assertEquals(0, ChunkFilter.FALSE_FILTER_INSTANCE.filter(values, results));
            assertBits(results, 0);
        }
    }

    @Test
    public void falseFilterAnd() {
        try (final WritableIntChunk<Values> values = valuesChunk();
                final WritableBooleanChunk<Values> results = resultsChunk()) {
            assertEquals(0, ChunkFilter.FALSE_FILTER_INSTANCE.filterAnd(values, results));
            assertBits(results, 0);
        }
    }

    @Test
    public void trueFilter() {
        try (final WritableIntChunk<Values> values = valuesChunk();
                final WritableBooleanChunk<Values> results = resultsChunk()) {
            assertEquals(CHUNK_SIZE, ChunkFilter.TRUE_FILTER_INSTANCE.filter(values, results));
            assertBits(results, CHUNK_SIZE);
        }
    }

    @Test
    public void trueFilterAnd() {
        try (final WritableIntChunk<Values> values = valuesChunk();
                final WritableBooleanChunk<Values> results = resultsChunk()) {
            assertEquals(INITIALLY_TRUE, ChunkFilter.TRUE_FILTER_INSTANCE.filterAnd(values, results));
            assertBits(results, INITIALLY_TRUE);
        }
    }
}

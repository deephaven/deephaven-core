//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.chunkfilter;

import org.junit.Test;

import static io.deephaven.engine.table.impl.chunkfilter.ChunkFilter.FILTER_CHUNK_SIZE;
import static io.deephaven.engine.table.impl.chunkfilter.ChunkFilter.INTERRUPTION_GOAL_MILLIS;
import static io.deephaven.engine.table.impl.chunkfilter.ChunkFilter.MAX_INTERRUPTION_SIZE;
import static io.deephaven.engine.table.impl.chunkfilter.ChunkFilter.nextChunksBetweenChecks;
import static org.junit.Assert.assertEquals;

public class ChunkFilterInterruptionTest {

    private static final long MAX_CHUNKS = Math.max(1, MAX_INTERRUPTION_SIZE / FILTER_CHUNK_SIZE);

    @Test
    public void onTargetIntervalIsUnchanged() {
        assertEquals(64, nextChunksBetweenChecks(64, INTERRUPTION_GOAL_MILLIS));
    }

    @Test
    public void slowFilteringChecksMoreOften() {
        assertEquals(16, nextChunksBetweenChecks(64, INTERRUPTION_GOAL_MILLIS * 4));
        // never less than one chunk
        assertEquals(1, nextChunksBetweenChecks(1, INTERRUPTION_GOAL_MILLIS * 1000));
    }

    @Test
    public void fastFilteringChecksLessOften() {
        assertEquals(128, nextChunksBetweenChecks(32, INTERRUPTION_GOAL_MILLIS / 4));
        // an interval too short to measure doubles
        assertEquals(128, nextChunksBetweenChecks(64, 0));
    }

    @Test
    public void intervalIsCapped() {
        assertEquals(MAX_CHUNKS, nextChunksBetweenChecks(MAX_CHUNKS, 0));
        assertEquals(MAX_CHUNKS, nextChunksBetweenChecks(MAX_CHUNKS, 1));
    }
}

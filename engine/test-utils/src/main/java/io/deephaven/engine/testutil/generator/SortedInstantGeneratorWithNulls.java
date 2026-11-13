//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;

import java.time.Instant;
import java.util.Random;

/**
 * Generates sorted instants with a random fraction replaced by null; the non-null values remain sorted.
 */
public class SortedInstantGeneratorWithNulls extends SortedInstantGenerator {
    private final double nullFrac;

    public SortedInstantGeneratorWithNulls(Instant minTime, Instant maxTime, double nullFrac) {
        super(minTime, maxTime);
        this.nullFrac = nullFrac;
    }

    @Override
    public Chunk<Values> populateChunk(RowSet toAdd, Random random) {
        final ObjectChunk<Instant, Values> srcChunk = super.populateChunk(toAdd, random).asObjectChunk();
        final Object[] dateArr = new Object[srcChunk.size()];
        srcChunk.copyToArray(0, dateArr, 0, dateArr.length);
        for (int ii = 0; ii < dateArr.length; ii++) {
            if (random.nextDouble() < nullFrac) {
                dateArr[ii] = null;
            }
        }
        return ObjectChunk.chunkWrap(dateArr);
    }
}

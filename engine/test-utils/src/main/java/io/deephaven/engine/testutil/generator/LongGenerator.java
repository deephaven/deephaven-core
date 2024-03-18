//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharGenerator and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.QueryConstants;

import java.util.Random;

/**
 * Generates columns of random longacters.
 */
public class LongGenerator extends AbstractGenerator<Long> {

    private final long to, from;
    private final double nullFraction;

    public LongGenerator() {
        this(PrimitiveGeneratorFunctions.minLong(), PrimitiveGeneratorFunctions.maxLong());
    }

    public LongGenerator(long from, long to) {
        this.from = from;
        this.to = to;
        nullFraction = 0.0;
    }

    public LongGenerator(long from, long to, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
    }

    @Override
    public Long nextValue(Random random) {
        return nextLong(random);
    }

    private long nextLong(Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return QueryConstants.NULL_LONG;
            }
        }
        return PrimitiveGeneratorFunctions.generateLong(random, from, to);
    }

    @Override
    public WritableLongChunk<Values> populateChunk(RowSet toAdd, Random random) {
        final long[] result = new long[toAdd.intSize()];
        for (int ii = 0; ii < result.length; ++ii) {
            result[ii] = nextLong(random);
        }
        return WritableLongChunk.writableChunkWrap(result);
    }

    @Override
    public Class<Long> getType() {
        return Long.class;
    }
}

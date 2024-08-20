//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharGenerator and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.QueryConstants;

import java.util.Random;

/**
 * Generates columns of random intacters.
 */
public class IntGenerator extends AbstractGenerator<Integer> {

    private final int to, from;
    private final double nullFraction;

    public IntGenerator() {
        this(PrimitiveGeneratorFunctions.minInt(), PrimitiveGeneratorFunctions.maxInt());
    }

    public IntGenerator(int from, int to) {
        this.from = from;
        this.to = to;
        nullFraction = 0.0;
    }

    public IntGenerator(int from, int to, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
    }

    @Override
    public Integer nextValue(Random random) {
        return nextInt(random);
    }

    private int nextInt(Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return QueryConstants.NULL_INT;
            }
        }
        return PrimitiveGeneratorFunctions.generateInt(random, from, to);
    }

    @Override
    public WritableIntChunk<Values> populateChunk(RowSet toAdd, Random random) {
        final int[] result = new int[toAdd.intSize()];
        for (int ii = 0; ii < result.length; ++ii) {
            result[ii] = nextInt(random);
        }
        return WritableIntChunk.writableChunkWrap(result);
    }

    @Override
    public Class<Integer> getType() {
        return Integer.class;
    }
}

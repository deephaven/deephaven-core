//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit CharGenerator and run "./gradlew replicateSourceAndChunkTests" to regenerate
//
// @formatter:off
package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.WritableShortChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.QueryConstants;

import java.util.Random;

/**
 * Generates columns of random shortacters.
 */
public class ShortGenerator extends AbstractGenerator<Short> {

    private final short to, from;
    private final double nullFraction;

    public ShortGenerator() {
        this(PrimitiveGeneratorFunctions.minShort(), PrimitiveGeneratorFunctions.maxShort());
    }

    public ShortGenerator(short from, short to) {
        this.from = from;
        this.to = to;
        nullFraction = 0.0;
    }

    public ShortGenerator(short from, short to, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
    }

    @Override
    public Short nextValue(Random random) {
        return nextShort(random);
    }

    private short nextShort(Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return QueryConstants.NULL_SHORT;
            }
        }
        return PrimitiveGeneratorFunctions.generateShort(random, from, to);
    }

    @Override
    public WritableShortChunk<Values> populateChunk(RowSet toAdd, Random random) {
        final short[] result = new short[toAdd.intSize()];
        for (int ii = 0; ii < result.length; ++ii) {
            result[ii] = nextShort(random);
        }
        return WritableShortChunk.writableChunkWrap(result);
    }

    @Override
    public Class<Short> getType() {
        return Short.class;
    }
}

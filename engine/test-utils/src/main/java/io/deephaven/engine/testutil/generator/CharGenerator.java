package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.WritableCharChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.QueryConstants;

import java.util.Random;

/**
 * Generates columns of random characters.
 */
public class CharGenerator extends AbstractGenerator<Character> {

    private final char to, from;
    private final double nullFraction;

    public CharGenerator() {
        this(PrimitiveGeneratorFunctions.minChar(), PrimitiveGeneratorFunctions.maxChar());
    }

    public CharGenerator(char from, char to) {
        this.from = from;
        this.to = to;
        nullFraction = 0.0;
    }

    public CharGenerator(char from, char to, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
    }

    @Override
    public Character nextValue(Random random) {
        return nextChar(random);
    }

    private char nextChar(Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return QueryConstants.NULL_CHAR;
            }
        }
        return PrimitiveGeneratorFunctions.generateChar(random, from, to);
    }

    @Override
    public WritableCharChunk<Values> populateChunk(RowSet toAdd, Random random) {
        final char[] result = new char[toAdd.intSize()];
        for (int ii = 0; ii < result.length; ++ii) {
            result[ii] = nextChar(random);
        }
        return WritableCharChunk.writableChunkWrap(result);
    }

    @Override
    public Class<Character> getType() {
        return Character.class;
    }
}

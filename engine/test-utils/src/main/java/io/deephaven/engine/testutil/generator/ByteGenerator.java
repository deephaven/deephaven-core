/*
 * ---------------------------------------------------------------------------------------------------------------------
 * AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY - for any changes edit CharGenerator and regenerate
 * ---------------------------------------------------------------------------------------------------------------------
 */
package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.util.QueryConstants;

import java.util.Random;

/**
 * Generates columns of random byteacters.
 */
public class ByteGenerator extends AbstractGenerator<Byte> {

    private final byte to, from;
    private final double nullFraction;

    public ByteGenerator() {
        this(PrimitiveGeneratorFunctions.minByte(), PrimitiveGeneratorFunctions.maxByte());
    }

    public ByteGenerator(byte from, byte to) {
        this.from = from;
        this.to = to;
        nullFraction = 0.0;
    }

    public ByteGenerator(byte from, byte to, double nullFraction) {
        this.from = from;
        this.to = to;
        this.nullFraction = nullFraction;
    }

    @Override
    public Byte nextValue(Random random) {
        return nextByte(random);
    }

    private byte nextByte(Random random) {
        if (nullFraction > 0) {
            if (random.nextDouble() < nullFraction) {
                return QueryConstants.NULL_BYTE;
            }
        }
        return PrimitiveGeneratorFunctions.generateByte(random, from, to);
    }

    @Override
    public WritableByteChunk<Values> populateChunk(RowSet toAdd, Random random) {
        final byte[] result = new byte[toAdd.intSize()];
        for (int ii = 0; ii < result.length; ++ii) {
            result[ii] = nextByte(random);
        }
        return WritableByteChunk.writableChunkWrap(result);
    }

    @Override
    public Class<Byte> getType() {
        return Byte.class;
    }
}

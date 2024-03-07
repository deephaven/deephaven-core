//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;

import java.util.Random;

/**
 * A generator that produces values of type U for use in a column of type T.
 *
 * @param <T> the output column type
 * @param <U> the generated value type
 */
public abstract class AbstractReinterpretedGenerator<T, U> implements TestDataGenerator<T, U> {
    @Override
    public Chunk<Values> populateChunk(final RowSet toAdd, final Random random) {
        // noinspection unchecked
        final U[] data = (U[]) new Object[toAdd.intSize()];
        for (int ii = 0; ii < data.length; ++ii) {
            data[ii] = nextValue(random);
        }
        return WritableObjectChunk.chunkWrap(data);
    }

    /**
     * Generate a random value.
     *
     * @param random the random number generator to use
     * @return a random value
     */
    abstract U nextValue(Random random);
}

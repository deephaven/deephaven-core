//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.testutil.ColumnInfo;

import java.util.Random;

/**
 * A generator for randomized columns within a unit test.
 *
 * A {@link ColumnInfo} provides information about a column such as its name and the generator that determines how to
 * create the column.
 *
 * There are two type parameters. T is the type of our column, U is the type of values we generate. Often these are
 * identical, but for DateTimes we can generate longs (U) and have present the DateTime to the user (T).
 *
 * @param <T> the outward facing type of the column source
 * @param <U> the type of values that will be generated
 */
public interface TestDataGenerator<T, U> {
    /**
     * Create a chunk for the given index using random for any desired entropy.
     *
     * @param toAdd the index to add values for
     * @param random the random number generator used for this test
     * @return a chunk of primitives or boxed objects of type U
     */
    Chunk<Values> populateChunk(RowSet toAdd, Random random);

    /**
     * @return the type of the chunk that we populate
     */
    Class<U> getType();

    /**
     * @return the type of the generated column
     */
    Class<T> getColumnType();

    /**
     * @return the component type of the generated column
     */
    default Class<?> getColumnComponentType() {
        return getColumnType().getComponentType();
    }

    /**
     * Called after a set of rows have been removed from the map.
     *
     * @param toRemove the row set that was removed
     */
    default void onRemove(RowSet toRemove) {}

    /**
     * Called when a range is shifted.
     *
     * @param start the start of the range to shift
     * @param end the end of the range to shift
     * @param delta the delta for the shift
     */
    default void shift(long start, long end, long delta) {}
}

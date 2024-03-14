//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.testutil.generator;

import java.util.Random;

/**
 * Generates unique values for a column.
 *
 * Some operations require unique values (e.g., the right hand side of a naturalJoin). This generator will provide
 * unique values for those operations.
 *
 * The hasValues and getRandomValue functions are used to retrieve one of the already generated active values from our
 * set.
 *
 * @param <T> the column type
 * @param <U> the generated data type
 */
public interface UniqueTestDataGenerator<T, U> extends TestDataGenerator<T, U> {
    /**
     * Are there any values that have been generated?
     * 
     * @return if there are any internally stored values?
     */
    boolean hasValues();

    /**
     * Get one random value from the generated set of values.
     *
     * hasValues must be true before calling this function.
     *
     * @param random the random number generator to use
     * @return a random value that has already been generated
     */
    T getRandomValue(final Random random);
}

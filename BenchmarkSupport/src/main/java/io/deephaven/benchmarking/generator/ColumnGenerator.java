package io.deephaven.benchmarking.generator;

import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.benchmarking.generator.random.ExtendedRandom;

/**
 * An interface that defines a class which will create Columns for a {@link io.deephaven.benchmarking.BenchmarkTable}
 * including {@link ColumnDefinition} creation and a method to create
 * {@link io.deephaven.db.tables.Table#update(String...)} strings.
 *
 * @param <T> The column type
 */
public interface ColumnGenerator<T> {
    /**
     * @return The correctly typed column Definition for this column
     */
    ColumnDefinition<T> getDefinition();

    /**
     * Initialize any internal state with the specified RNG
     * 
     * @param random the RNG to use.
     */
    void init(ExtendedRandom random);

    /**
     * Create a string suitable for use with {@link io.deephaven.db.tables.Table#update(String...)} calls to generate
     * data.
     *
     * @param varName The name of this instance's variable within the {@link io.deephaven.db.tables.select.QueryScope}
     * @return A string for use with update()
     */
    String getUpdateString(String varName);

    String getName();
}

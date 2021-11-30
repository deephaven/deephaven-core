package io.deephaven.benchmarking;

import io.deephaven.engine.table.Table;
import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.util.annotations.TestUseOnly;

import java.io.File;

/**
 * A Factory class to create tables suitable for use with a Benchmark.
 */
@ScriptApi
public interface BenchmarkTable {
    /**
     * Reset any internal state of the factory, ie. RNGs
     */
    @ScriptApi
    void reset();

    /**
     * Clean up any existing data. Do NOT change any RNG states.
     */
    void cleanup();


    /**
     * Generates the table
     * 
     * @param resetRNG when true resets the random number generator
     */
    @ScriptApi
    Table getTable(boolean resetRNG);

    /**
     * Generates the table after reseting the random number generators
     */
    @ScriptApi
    Table getTable();

    /**
     * Get the name of this table.
     */
    @ScriptApi
    String getName();

    /**
     * Get the target size of the table.
     */
    long getSize();

    @TestUseOnly
    File getLocation();
}

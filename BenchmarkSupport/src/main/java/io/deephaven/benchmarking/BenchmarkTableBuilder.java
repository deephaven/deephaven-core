package io.deephaven.benchmarking;

import io.deephaven.util.annotations.ScriptApi;
import io.deephaven.benchmarking.generator.ColumnGenerator;

/**
 * A Builder that creates tables specifically used for benchmarking.
 */
@ScriptApi
public interface BenchmarkTableBuilder {

    /**
     * Set the RNG seed to be used to populate the table.
     * 
     * @param seed The seed.
     */
    @ScriptApi
    BenchmarkTableBuilder setSeed(long seed);

    @ScriptApi
    BenchmarkTableBuilder addColumn(ColumnGenerator generator);

    /**
     * Create the table based on the current builder.
     */
    @ScriptApi
    BenchmarkTable build();
}

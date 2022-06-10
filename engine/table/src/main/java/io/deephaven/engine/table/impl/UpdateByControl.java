package io.deephaven.engine.table.impl;

import io.deephaven.configuration.Configuration;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.updateby.hashing.IncrementalUpdateByStateManager;
import org.jetbrains.annotations.NotNull;

import java.math.MathContext;

/**
 * An interface to control the behavior of an {@link Table#updateBy}
 */
public interface UpdateByControl {
    int DEFAULT_CHUNK_CAPACITY =
            Configuration.getInstance().getIntegerWithDefault("UpdateByControl.chunkCapacity", 4096);
    boolean DEFAULT_USE_REDIRECTION =
            Configuration.getInstance().getBooleanWithDefault("UpdateByControl.useRedirectionOutput", false);
    double DEFAULT_MAXIMUM_STATIC_SPARSE_MEMORY_OVERHEAD =
            Configuration.getInstance().getDoubleWithDefault("UpdateByControl.maximumStaticMemoryOverhead", 1.1);

    UpdateByControl DEFAULT = new UpdateByControl() {};

    /**
     * If redirections should be used for output sources instead of sparse array sources.
     *
     * @return true if redirections should be used.
     */
    default boolean useRedirection() {
        return DEFAULT_USE_REDIRECTION;
    }

    /**
     * Get the default maximum chunk capacity.
     *
     * @return the maximum chunk capacity.
     */
    default int getChunkCapacity() {
        return DEFAULT_CHUNK_CAPACITY;
    }

    /**
     * The maximum fractional memory overhead allowable for sparse redirections.
     * 
     * @return the maximum fractional memory overhead.
     */
    default double getMaxStaticSparseMemoryOverhead() {
        return DEFAULT_MAXIMUM_STATIC_SPARSE_MEMORY_OVERHEAD;
    }

    /**
     * Get the initial hash table size for the specified input table.
     *
     * @param source the input table
     * @return the initial hash table size
     */
    default int initialHashTableSize(@NotNull final Table source) {
        return IncrementalUpdateByStateManager.MINIMUM_INITIAL_HASH_SIZE;
    }

    /**
     * Get the maximum load factor for the hash table.
     * 
     * @return the maximum load factor
     */
    default double getMaximumLoadFactor() {
        return IncrementalUpdateByStateManager.DEFAULT_MAX_LOAD_FACTOR;
    }

    /**
     * Get the target load factor for the hash table.
     * 
     * @return the target load factor
     */
    default double getTargetLoadFactor() {
        return IncrementalUpdateByStateManager.DEFAULT_MAX_LOAD_FACTOR;
    }

    /**
     * Get if the operation should use grouping data.
     *
     * @param source the source table
     * @param keySources the ke sources
     * @return true if the operation should use groupings.
     */
    default boolean considerGrouping(@NotNull final Table source, @NotNull final ColumnSource<?>[] keySources) {
        return !source.isRefreshing() && keySources.length == 1;
    }

    default MathContext getDefaultMathContext() {
        return MathContext.DECIMAL64;
    }
}

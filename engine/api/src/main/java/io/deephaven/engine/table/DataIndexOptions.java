//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table;

import io.deephaven.annotations.BuildableStyle;
import io.deephaven.api.filter.Filter;
import org.immutables.value.Value;

/**
 * Options for controlling the function of a {@link DataIndex}.
 *
 * <p>
 * Presently, this is used for the {@link Table#where(Filter)} operation to more efficiently handle data index matches,
 * without necessarily reading all RowSet information from disk across partitions.
 * </p>
 */
@Value.Immutable
@BuildableStyle
public interface DataIndexOptions {
    DataIndexOptions DEFAULT = DataIndexOptions.builder().build();

    /**
     * Does this operation use only a subset of the DataIndex?
     *
     * <p>
     * The DataIndex implementation may use this hint to defer work for some row sets.
     * </p>
     *
     * @return if this operation is only going to use a subset of this data index
     */
    @Value.Default
    default boolean operationUsesPartialTable() {
        return false;
    }

    /**
     * Create a new builder for a {@link DataIndexOptions}.
     * 
     * @return
     */
    static Builder builder() {
        return ImmutableDataIndexOptions.builder();
    }

    /**
     * The builder interface to construct a {@link DataIndexOptions}.
     */
    interface Builder {
        /**
         * Set whether this operation only uses a subset of the data index.
         *
         * @param usesPartialTable true if this operation only uses a partial table
         * @return this builder
         */
        Builder operationUsesPartialTable(boolean usesPartialTable);

        /**
         * Build the {@link DataIndexOptions}.
         * 
         * @return an immutable DataIndexOptions structure.
         */
        DataIndexOptions build();
    }
}

/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.locations;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.dataindex.DiskBackedDeferredGroupingProvider;
import io.deephaven.engine.table.impl.dataindex.PartitionColumnGroupingProvider;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

/**
 * Implementations of this interface are able to compute groupings.
 */
public interface GroupingProvider {
    static final String INDEX_DIR_PREFIX = "Index-";
    static final String INDEX_COL_NAME = "Index";

    /**
     * Make a new {@link GroupingProvider} for the specified {@link ColumnDefinition} and current global configuration.
     *
     * @param columnDefinition The column definition
     * @param log a logger
     * @return A new {@link GroupingProvider}
     */
    @NotNull
    static GroupingProvider makeGroupingProvider(@NotNull final ColumnDefinition<?> columnDefinition, @NotNull final ColumnSource<?> source, @NotNull final Logger log) {
        return columnDefinition.isPartitioning()
                ? PartitionColumnGroupingProvider.make(columnDefinition.getName(), source)
                : new DiskBackedDeferredGroupingProvider<>(columnDefinition, log);
    }

    /**
     * Get a {@link GroupingBuilder} suitable for creating groups with specific properties.
     * @return a {@link GroupingBuilder}
     */
    @NotNull
    GroupingBuilder getGroupingBuilder();

    /**
     * Check if this provider is able to create a grouping or not.
     * @return true if this provider can create a grouping.
     */
    boolean hasGrouping();
}

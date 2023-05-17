/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.locations;

import io.deephaven.engine.table.ColumnDefinition;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.GroupingProvider;
import io.deephaven.engine.table.impl.dataindex.DiskBackedDeferredGroupingProvider;
import io.deephaven.engine.table.impl.dataindex.PartitionColumnGroupingProvider;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;

/**
 * Implementations of this interface are able to compute groupings.
 */
public interface GroupingProviderFactory {
    /**
     * Make a new {@link GroupingProviderFactory} for the specified {@link ColumnDefinition} and current global
     * configuration.
     *
     * @param columnDefinition The column definition
     * @param log a logger
     * @return A new {@link GroupingProviderFactory}
     */
    @NotNull
    static GroupingProvider makeGroupingProvider(@NotNull final ColumnDefinition<?> columnDefinition,
            @NotNull final ColumnSource<?> source, @NotNull final Logger log) {
        return columnDefinition.isPartitioning()
                ? PartitionColumnGroupingProvider.make(columnDefinition.getName(), source)
                : new DiskBackedDeferredGroupingProvider<>(columnDefinition, log);
    }
}

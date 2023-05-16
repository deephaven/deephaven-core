/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.locations.GroupingBuilder;
import io.deephaven.engine.table.impl.locations.GroupingProvider;
import org.jetbrains.annotations.Nullable;

/**
 * A column source that optionally makes available a provider for grouping metadata.
 */
public interface DeferredGroupingColumnSource<DATA_TYPE> extends ColumnSource<DATA_TYPE> {
    /**
     * Get a {@link GroupingBuilder} to construct grouping tables for this column.  Use {@link #hasGrouping()}
     * to determine if this column has a grouping.
     *
     * @return a {@link GroupingBuilder} if this column supports grouping.
     */
    @Nullable
    GroupingBuilder getGroupingBuilder();

    /**
     * Get the grouping provider associated with this ColumnSource.
     * @return the grouping provider associated with this ColumnSource.
     */
    GroupingProvider getGroupingProvider();

    /**
     * Supply a provider that will lazily construct the group-to-range map.
     * @param groupingProvider The provider
     */
    void setGroupingProvider(@Nullable GroupingProvider groupingProvider);
}

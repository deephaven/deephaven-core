/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.db.v2.locations.GroupingProvider;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * A column source that optionally makes available a provider for grouping metadata.
 */
public interface DeferredGroupingColumnSource<DATA_TYPE> extends ColumnSource<DATA_TYPE> {

    /**
     * Set the map returned by getGroupToRange().
     * 
     * @param groupToRange The map
     */
    void setGroupToRange(@Nullable Map<DATA_TYPE, Index> groupToRange);

    /**
     * @return A provider previously set by
     *         {@link DeferredGroupingColumnSource#setGroupingProvider(io.deephaven.db.v2.locations.GroupingProvider)}
     */
    GroupingProvider<DATA_TYPE> getGroupingProvider();

    /**
     * Supply a provider that will lazily construct the group-to-range map.
     * 
     * @param groupingProvider The provider
     */
    void setGroupingProvider(@Nullable GroupingProvider<DATA_TYPE> groupingProvider);
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.sources;

import io.deephaven.base.Pair;
import io.deephaven.db.v2.locations.GroupingProvider;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.Nullable;

import java.util.Map;

/**
 * Adds deferred grouping support to {@link AbstractColumnSource}.
 */
public abstract class AbstractDeferredGroupingColumnSource<T> extends AbstractColumnSource<T>
    implements DeferredGroupingColumnSource<T> {

    private transient volatile GroupingProvider<T> groupingProvider;

    protected AbstractDeferredGroupingColumnSource(Class<T> type) {
        super(type, null);
    }

    protected AbstractDeferredGroupingColumnSource(Class<T> type, Class componentType) {
        super(type, componentType);
    }

    @Override
    public GroupingProvider<T> getGroupingProvider() {
        return groupingProvider;
    }

    /**
     * Set a grouping provider for use in lazily-constructing groupings.
     *
     * @param groupingProvider The {@link GroupingProvider} to use
     */
    @Override
    public final void setGroupingProvider(@Nullable GroupingProvider<T> groupingProvider) {
        this.groupingProvider = groupingProvider;
    }

    @Override
    public final Map<T, Index> getGroupToRange() {
        if (groupToRange == null && groupingProvider != null) {
            groupToRange = groupingProvider.getGroupToRange();
            groupingProvider = null;
        }
        return groupToRange;
    }

    @Override
    public final Map<T, Index> getGroupToRange(Index index) {
        if (groupToRange == null && groupingProvider != null) {
            Pair<Map<T, Index>, Boolean> result = groupingProvider.getGroupToRange(index);
            if (result == null) {
                return null;
            }
            if (result.second) {
                groupToRange = result.first;
                groupingProvider = null;
            }
            return result.first;
        }
        return groupToRange;
    }
}

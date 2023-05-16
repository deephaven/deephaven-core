/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl.sources;

import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.AbstractColumnSource;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.chunkfilter.ChunkMatchFilterFactory;
import io.deephaven.engine.table.impl.locations.GroupingBuilder;
import io.deephaven.engine.table.impl.locations.GroupingProvider;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Adds deferred grouping support to {@link AbstractColumnSource}.
 */
public abstract class AbstractDeferredGroupingColumnSource<T> extends AbstractColumnSource<T>
        implements DeferredGroupingColumnSource<T> {

    private transient GroupingProvider groupingProvider;

    protected AbstractDeferredGroupingColumnSource(Class<T> type) {
        super(type, null);
    }

    protected AbstractDeferredGroupingColumnSource(Class<T> type, Class<?> componentType) {
        super(type, componentType);
    }

    @Override
    public GroupingBuilder getGroupingBuilder() {
        return groupingProvider == null ? null : groupingProvider.getGroupingBuilder();
    }

    @Override
    public boolean hasGrouping() {
        return groupingProvider != null && groupingProvider.hasGrouping();
    }

    public GroupingProvider getGroupingProvider() {
        return groupingProvider;
    }

    /**
     * Set a grouping provider for use in lazily-constructing groupings.
     *
     * @param groupingProvider The {@link GroupingProvider} to use
     */
    public final void setGroupingProvider(@Nullable GroupingProvider groupingProvider) {
        this.groupingProvider = groupingProvider;
    }
}

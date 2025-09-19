//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.api.Strings;
import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.TrackingWritableRowSet;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.MustBeInvokedByOverriders;

import java.util.List;
import java.util.Map;

/**
 * Base class for {@link PushdownFilterContext} to help with execution cost tracking.
 */
public class BasePushdownFilterContext implements PushdownFilterContext {
    protected final WhereFilter filter;
    private final List<ColumnSource<?>> columnSources;

    private final boolean isRangeFilter;
    private final boolean isMatchFilter;
    private final boolean supportsChunkFilter;
    private final boolean filterIncludesNulls;

    private long executedFilterCost;

    /**
     * A dummy table to use for initializing {@link ConditionFilter}.
     */
    private volatile QueryTable condFilterInitTable = null;

    /**
     * Interface for a unified chunk filter that can be used to apply a filter to a chunk of data, whether the
     * underlying filter is a {@link ExposesChunkFilter} or a {@link ConditionFilter}.
     */
    public interface UnifiedChunkFilter extends SafeCloseable {
        LongChunk<OrderedRowKeys> filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys);
    }

    public BasePushdownFilterContext(
            final WhereFilter filter,
            final List<ColumnSource<?>> columnSources) {
        this.filter = filter;
        this.columnSources = columnSources;

        executedFilterCost = 0;

        // TODO (DH-19666): Multi column filters are not supported yet
        final boolean isSingleColumn = columnSources.size() == 1;
        if (isSingleColumn) {
            isRangeFilter = filter instanceof RangeFilter
                    && ((RangeFilter) filter).getRealFilter() instanceof AbstractRangeFilter;
            isMatchFilter = filter instanceof MatchFilter &&
                    ((MatchFilter) filter).getFailoverFilterIfCached() == null;
            supportsChunkFilter =
                    (filter instanceof ExposesChunkFilter && ((ExposesChunkFilter) filter).chunkFilter().isPresent())
                            || filter instanceof ConditionFilter;

            // Create a dummy table with a single row and column, and `null` entry, and apply the filter to see if
            // the filter includes nulls.
            final ColumnSource<?> columnSource = columnSources.get(0);
            final NullValueColumnSource<?> nullValueColumnSource =
                    NullValueColumnSource.getInstance(columnSource.getType(), columnSource.getComponentType());
            final Map<String, ColumnSource<?>> columnSourceMap =
                    Map.of(filter.getColumns().get(0), nullValueColumnSource);
            try (final SafeCloseable ignored = LivenessScopeStack.open();
                    final TrackingWritableRowSet rowSet = RowSetFactory.flat(1).toTracking()) {
                final Table nullTestDummyTable = new QueryTable(rowSet, columnSourceMap);
                try (final RowSet result = filter.filter(rowSet, rowSet, nullTestDummyTable, false)) {
                    filterIncludesNulls = !result.isEmpty();
                }
            }
        } else {
            isRangeFilter = false;
            isMatchFilter = false;
            supportsChunkFilter = false;
            filterIncludesNulls = false;
        }
    }

    /**
     * Get the column sources this filter will use.
     */
    public List<ColumnSource<?>> columnSources() {
        return columnSources;
    }

    /**
     * Whether this is a simple range filter, not implemented by a ConditionFilter.
     */
    public boolean isRangeFilter() {
        return isRangeFilter;
    }

    /**
     * Whether this is a MatchFilter.
     */
    public boolean isMatchFilter() {
        return isMatchFilter;
    }

    /**
     * Whether this filter supports direct chunk filtering, i.e., it can be applied to a chunk of data rather than a
     * table. This includes any filter that implements {#@link ExposesChunkFilter} or {@link ConditionFilter} with
     * exactly one column.
     */
    public boolean supportsChunkFilter() {
        return supportsChunkFilter;
    }

    /**
     * Whether this filter includes nulls in its results.
     */
    public boolean filterIncludesNulls() {
        return filterIncludesNulls;
    }

    /**
     * Create a {@link UnifiedChunkFilter} for the {@link WhereFilter} that efficiently filters chunks of data. Every
     * thread that uses this should create its own instance and must close it after use.
     *
     * @param maxChunkSize the maximum size of the chunk that will be filtered
     * @return the initialized {@link UnifiedChunkFilter}
     */
    public final UnifiedChunkFilter createChunkFilter(final int maxChunkSize) {
        if (!supportsChunkFilter) {
            throw new UnsupportedOperationException("Filter does not support chunk filtering: " + Strings.of(filter));
        }
        final UnifiedChunkFilter unifiedChunkFilter;
        if (filter instanceof ExposesChunkFilter) {
            final ChunkFilter chunkFilter = ((ExposesChunkFilter) filter).chunkFilter()
                    .orElseThrow(() -> new IllegalStateException("ExposesChunkFilter#chunkFilter() returned null."));
            unifiedChunkFilter = new UnifiedChunkFilter() {
                // We need to create a WritableLongChunk to hold the results of the chunk filter.
                private final WritableLongChunk<OrderedRowKeys> resultChunk =
                        WritableLongChunk.makeWritableChunk(maxChunkSize);

                @Override
                public LongChunk<OrderedRowKeys> filter(
                        Chunk<? extends Values> values,
                        LongChunk<OrderedRowKeys> keys) {
                    chunkFilter.filter(values, keys, resultChunk);
                    return resultChunk;
                }

                @Override
                public void close() {
                    resultChunk.close();
                }
            };
        } else if (filter instanceof ConditionFilter) {
            // Create a dummy table with no rows and single column of the correct type and name as the filter. This is
            // used to extract a chunk filter kernel from the conditional filter and bind it to the correct name and
            // type without capturing references to the actual table or its column sources.
            // That is why it can be reused across threads.
            if (condFilterInitTable == null) {
                synchronized (this) {
                    if (condFilterInitTable == null) {
                        final Map<String, ColumnSource<?>> columnSourceMap = Map.of(filter.getColumns().get(0),
                                NullValueColumnSource.getInstance(
                                        columnSources.get(0).getType(),
                                        columnSources.get(0).getComponentType()));
                        condFilterInitTable = new QueryTable(RowSetFactory.empty().toTracking(), columnSourceMap);
                    }
                }
            }
            try {
                final ConditionFilter conditionFilter = (ConditionFilter) filter;
                final AbstractConditionFilter.Filter acfFilter =
                        conditionFilter.getFilter(condFilterInitTable, condFilterInitTable.getRowSet());

                unifiedChunkFilter = new UnifiedChunkFilter() {
                    // Create the context for the ConditionFilter, which will be used to filter chunks.
                    private final ConditionFilter.FilterKernel.Context conditionFilterContext =
                            acfFilter.getContext(maxChunkSize);

                    @Override
                    public LongChunk<OrderedRowKeys> filter(Chunk<? extends Values> values,
                            LongChunk<OrderedRowKeys> keys) {
                        // noinspection unchecked
                        return (LongChunk<OrderedRowKeys>) acfFilter.filter(conditionFilterContext, keys,
                                new Chunk[] {values});
                    }

                    @Override
                    public void close() {
                        conditionFilterContext.close();
                    }
                };

            } catch (final Exception e) {
                throw new IllegalArgumentException("Error creating condition filter in BasePushdownFilterContext", e);
            }
        } else {
            throw new UnsupportedOperationException(
                    "Filter does not support chunk filtering: " + Strings.of(filter));
        }
        return unifiedChunkFilter;
    }

    @Override
    public long executedFilterCost() {
        return executedFilterCost;
    }

    @Override
    public void updateExecutedFilterCost(long executedFilterCost) {
        this.executedFilterCost = executedFilterCost;
    }

    @MustBeInvokedByOverriders
    @Override
    public void close() {
        condFilterInitTable = null;
    }
}

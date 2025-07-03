//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.LongChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.chunkattributes.OrderedRowKeys;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.table.impl.chunkfilter.ChunkFilter;
import io.deephaven.engine.table.impl.select.*;
import io.deephaven.engine.table.impl.sources.NullValueColumnSource;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableList;
import org.jetbrains.annotations.MustBeInvokedByOverriders;

import java.util.List;
import java.util.Map;

/**
 * Base class for {@link PushdownFilterContext} to help with execution cost tracking.
 */
public class BasePushdownFilterContext implements PushdownFilterContext {
    private final WhereFilter filter;
    private final List<ColumnSource<?>> columnSources;

    private final boolean isRangeFilter;
    private final boolean isMatchFilter;
    private final boolean supportsChunkFilter;
    private final Boolean filterIncludesNulls;

    protected final SafeCloseableList closeList = new SafeCloseableList();

    private long executedFilterCost;

    private volatile QueryTable dummyTable = null;

    /**
     * Interface for a unified chunk filter that can be used to apply a filter to a chunk of data, whether the
     * underlying filter is a {@link ExposesChunkFilter} or a {@link ConditionFilter}.
     */
    public interface UnifiedChunkFilter extends SafeCloseable {
        LongChunk<OrderedRowKeys> filter(Chunk<? extends Values> values, LongChunk<OrderedRowKeys> keys);
    }

    public BasePushdownFilterContext(final WhereFilter filter, final List<ColumnSource<?>> columnSources) {
        this.filter = filter;
        this.columnSources = columnSources;

        executedFilterCost = 0;

        // Compute useful properties of the filter
        isRangeFilter = filter instanceof RangeFilter
                && ((RangeFilter) filter).getRealFilter() instanceof AbstractRangeFilter;
        isMatchFilter = filter instanceof MatchFilter;
        if (columnSources.size() == 1) {
            supportsChunkFilter =
                    (filter instanceof ExposesChunkFilter && ((ExposesChunkFilter) filter).chunkFilter().isPresent())
                            || filter instanceof ConditionFilter;
            if (supportsChunkFilter) {
                // Run the filter over a chunk containing only null values and check if nulls pass the filter.
                final ChunkType chunkType = columnSources.get(0).getChunkType();
                if (chunkType == ChunkType.Boolean) {
                    // BooleanChunk does not support nulls, so we can skip this check.
                    filterIncludesNulls = false;
                } else {
                    final Chunk<? extends Values> nullChunk = chunkType.getNullChunk();
                    try (final UnifiedChunkFilter unifiedChunkFilter = createChunkFilter(1);
                            final WritableLongChunk<OrderedRowKeys> nullChunkKeys =
                                    WritableLongChunk.makeWritableChunk(1)) {
                        nullChunkKeys.set(0, 0);
                        final LongChunk<OrderedRowKeys> matchingKeys =
                                unifiedChunkFilter.filter(nullChunk, nullChunkKeys);
                        filterIncludesNulls = matchingKeys.size() > 0;
                    }
                }
            } else {
                // If we don't support chunk filtering, we can't easily determine if filter includes nulls.
                filterIncludesNulls = null;
            }
        } else {
            supportsChunkFilter = false;
            filterIncludesNulls = null; // Unknown for multi-column filters
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
     * Whether this filter supports direct chunk filtering, i.e. it can be applied to a chunk of data rather than a
     * table. This includes any filter that implements {#@link ExposesChunkFilter} or
     * {@link io.deephaven.engine.table.impl.select.ConditionFilter} with exactly one column.
     */
    public boolean supportsChunkFilter() {
        return supportsChunkFilter;
    }

    /**
     * Whether this filter includes nulls in its results. Using boxed Boolean to allow tri-state where {@code null}
     * implies "unknown".
     */
    public Boolean filterIncludesNulls() {
        return filterIncludesNulls;
    }

    /**
     * Create a {@link UnifiedChunkFilter} for this filter that efficiently filters chunks of data. Every thread that
     * uses this filter should create its own instance of the filter and must close it after use.
     *
     * @param maxChunkSize the maximum size of the chunk that will be filtered
     * @return the initialized {@link UnifiedChunkFilter}
     */
    public final UnifiedChunkFilter createChunkFilter(int maxChunkSize) {
        if (!supportsChunkFilter) {
            return null;
        }
        final UnifiedChunkFilter unifiedChunkFilter;
        if (filter instanceof ExposesChunkFilter) {
            final WritableLongChunk<OrderedRowKeys> resultChunk = WritableLongChunk.makeWritableChunk(maxChunkSize);
            final ChunkFilter chunkFilter = ((ExposesChunkFilter) filter).chunkFilter()
                    .orElseThrow(() -> new IllegalStateException("ExposesChunkFilter#chunkFilter() returned null."));
            unifiedChunkFilter = new UnifiedChunkFilter() {
                @Override
                public LongChunk<OrderedRowKeys> filter(Chunk<? extends Values> values,
                        LongChunk<OrderedRowKeys> keys) {
                    chunkFilter.filter(values, keys, resultChunk);
                    return resultChunk;
                }

                @Override
                public void close() {
                    resultChunk.close();
                }
            };
        } else {
            // Create and store a dummy table to use for initializing the ConditionFilter.
            if (dummyTable == null) {
                synchronized (this) {
                    if (dummyTable == null) {
                        final Map<String, ColumnSource<?>> columnSourceMap = Map.of(filter.getColumns().get(0),
                                NullValueColumnSource.getInstance(
                                        columnSources.get(0).getType(),
                                        columnSources.get(0).getComponentType()));
                        dummyTable = new QueryTable(RowSetFactory.empty().toTracking(), columnSourceMap);
                    }
                }
            }
            try {
                final ConditionFilter conditionFilter = (ConditionFilter) filter;
                final AbstractConditionFilter.Filter acfFilter =
                        conditionFilter.getFilter(dummyTable, dummyTable.getRowSet());
                final ConditionFilter.FilterKernel.Context conditionFilterContext = acfFilter.getContext(maxChunkSize);

                unifiedChunkFilter = new UnifiedChunkFilter() {
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
        closeList.close();
        dummyTable = null;
    }
}

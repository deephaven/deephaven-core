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

    /**
     * Enum for the behavior of a filter when applied to null values.
     */
    public enum FilterNullBehavior {
        /**
         * The filter includes nulls in its results, like {@code x == null}.
         */
        INCLUDES_NULLS,

        /**
         * The filter does not include nulls in its results, like {@code x > 5}.
         */
        EXCLUDES_NULLS,

        /**
         * The filter throws an exception when applied to nulls, like {@code x.beginsWith("A")}.
         */
        FAILS_ON_NULLS
    }

    protected final WhereFilter filter;
    private final List<ColumnSource<?>> columnSources;

    private final boolean isRangeFilter;
    private final boolean isMatchFilter;
    private final boolean supportsChunkFilter;
    private final boolean filterSupportsPushdown;

    private long executedFilterCost;

    private volatile FilterNullBehavior filterNullBehavior;

    /**
     * A dummy table to use for initializing {@link ConditionFilter}.
     */
    private volatile QueryTable conditionalFilterInitTable;

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

        isRangeFilter = filter instanceof RangeFilter
                && ((RangeFilter) filter).getRealFilter() instanceof AbstractRangeFilter;
        isMatchFilter = filter instanceof MatchFilter &&
                ((MatchFilter) filter).getFailoverFilterIfCached() == null;
        final boolean isConditionFilter = filter instanceof ConditionFilter;

        // TODO (DH-19666): Multi column filters are not supported yet
        filterSupportsPushdown = isRangeFilter || isMatchFilter ||
                (isConditionFilter && ((ConditionFilter) filter).getNumInputsUsed() == 1);
        // Do not use columnSources.size(), multiple logical columns may alias (rename) the same physical column,
        // yielding a single entry.

        supportsChunkFilter = filterSupportsPushdown &&
                ((filter instanceof ExposesChunkFilter && ((ExposesChunkFilter) filter).chunkFilter().isPresent())
                        || isConditionFilter);

        filterNullBehavior = null; // lazily initialized
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
     * Whether this filter supports pushdown-based filtering. This includes simple range filters, match filters, and
     * ConditionFilters with exactly one column.
     */
    public boolean filterSupportsPushdown() {
        return filterSupportsPushdown;
    }

    /**
     * Whether this filter supports direct chunk filtering, i.e., it can be applied to a chunk of data rather than a
     * table. This includes any filter that implements {#@link ExposesChunkFilter} or {@link ConditionFilter} with
     * exactly one column.
     */
    public boolean supportsChunkFilter() {
        return supportsChunkFilter;
    }

    public FilterNullBehavior filterNullBehavior() {
        if (filterNullBehavior == null) {
            synchronized (this) {
                if (filterNullBehavior == null) {
                    FilterNullBehavior temp;
                    // Create a dummy table with a single row and column, and `null` entry, and apply the filter to see
                    // if the filter includes nulls.
                    final ColumnSource<?> columnSource = columnSources.get(0);
                    final NullValueColumnSource<?> nullValueColumnSource =
                            NullValueColumnSource.getInstance(columnSource.getType(), columnSource.getComponentType());
                    final Map<String, ColumnSource<?>> columnSourceMap =
                            Map.of(filter.getColumns().get(0), nullValueColumnSource);
                    try (final SafeCloseable ignored = LivenessScopeStack.open();
                            final TrackingWritableRowSet rowSet = RowSetFactory.flat(1).toTracking()) {
                        final Table nullTestDummyTable = new QueryTable(rowSet, columnSourceMap);
                        try (final RowSet result = filter.filter(rowSet, rowSet, nullTestDummyTable, false)) {
                            temp = result.isEmpty()
                                    ? FilterNullBehavior.EXCLUDES_NULLS
                                    : FilterNullBehavior.INCLUDES_NULLS;
                        } catch (final Exception e) {
                            temp = FilterNullBehavior.FAILS_ON_NULLS;
                        }
                    }
                    filterNullBehavior = temp;
                }
            }
        }
        return filterNullBehavior;
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
            if (conditionalFilterInitTable == null) {
                synchronized (this) {
                    if (conditionalFilterInitTable == null) {
                        final Map<String, ColumnSource<?>> columnSourceMap = Map.of(filter.getColumns().get(0),
                                NullValueColumnSource.getInstance(
                                        columnSources.get(0).getType(),
                                        columnSources.get(0).getComponentType()));
                        conditionalFilterInitTable =
                                new QueryTable(RowSetFactory.empty().toTracking(), columnSourceMap);
                    }
                }
            }
            try {
                final ConditionFilter conditionFilter = (ConditionFilter) filter;
                final AbstractConditionFilter.Filter acfFilter =
                        conditionFilter.getFilter(conditionalFilterInitTable, conditionalFilterInitTable.getRowSet());

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
        conditionalFilterInitTable = null;
    }
}

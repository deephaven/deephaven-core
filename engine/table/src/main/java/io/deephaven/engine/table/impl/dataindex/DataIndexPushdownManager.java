//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.dataindex;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Pair;
import io.deephaven.api.Strings;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.exceptions.TableInitializationException;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.filter.ExtractFilterWithoutBarriers;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;

/**
 * A {@link PushdownPredicateManager} that supports pushdown against a {@link DataIndex}. This manager can wrap an
 * existing {@link PushdownFilterMatcher} to allow efficient fallback if the DataIndex pushdown is not effective or when
 * the cost ceiling is too low to use the DataIndex.
 */
public class DataIndexPushdownManager implements PushdownPredicateManager {
    private final DataIndex dataIndex;
    private final PushdownFilterMatcher wrappedMatcher;

    private final long selectionThreshold;

    private DataIndexPushdownManager(
            @NotNull final DataIndex dataIndex,
            final PushdownFilterMatcher wrappedMatcher) {
        this.dataIndex = dataIndex;
        this.wrappedMatcher = wrappedMatcher;

        selectionThreshold = (long) (dataIndex.table().size() / QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD);
    }

    @Override
    public void estimatePushdownFilterCost(
            @NotNull final WhereFilter filter,
            @NotNull final RowSet selection,
            final boolean usePrev,
            @NotNull final PushdownFilterContext context,
            @NotNull final JobScheduler jobScheduler,
            @NotNull final LongConsumer onComplete,
            @NotNull final Consumer<Exception> onError) {

        final DataIndexPushdownContext ctx = (DataIndexPushdownContext) context;

        final long dataIndexCost = selection.size() < selectionThreshold
                ? Long.MAX_VALUE
                : PushdownResult.IN_MEMORY_DATA_INDEX_COST;

        if (wrappedMatcher != null) {
            // Retrieve the wrapped cost and return the minimum of it and the data index cost.
            wrappedMatcher.estimatePushdownFilterCost(
                    filter, selection, usePrev, ctx.wrappedContext, jobScheduler,
                    cost -> onComplete.accept(Math.min(cost, dataIndexCost)),
                    onError);
            return;
        }

        onComplete.accept(dataIndexCost);
    }

    @Override
    public void pushdownFilter(
            @NotNull final WhereFilter filter,
            @NotNull final RowSet selection,
            final boolean usePrev,
            @NotNull final PushdownFilterContext context,
            final long costCeiling,
            @NotNull final JobScheduler jobScheduler,
            @NotNull final Consumer<PushdownResult> onComplete,
            @NotNull final Consumer<Exception> onError) {

        final DataIndexPushdownContext ctx = (DataIndexPushdownContext) context;

        if (costCeiling < PushdownResult.IN_MEMORY_DATA_INDEX_COST) {
            // Run the wrapped matcher if we have one.
            if (wrappedMatcher != null) {
                wrappedMatcher.pushdownFilter(filter, selection, usePrev, ctx.wrappedContext, costCeiling, jobScheduler,
                        onComplete, onError);
                return;
            }
            onComplete.accept(PushdownResult.allMaybeMatch(selection));
        } else {
            // If we have a wrapped matcher, run it up to the data index cost.
            if (wrappedMatcher != null) {
                wrappedMatcher.pushdownFilter(
                        filter,
                        selection,
                        usePrev,
                        ctx.wrappedContext,
                        PushdownResult.IN_MEMORY_DATA_INDEX_COST - 1,
                        jobScheduler,
                        result -> {
                            // Run the data index filter?
                            if (result.maybeMatch().size() > selectionThreshold) {
                                onComplete.accept(pushdownDataIndex(
                                        selection,
                                        filter,
                                        ctx.renameMap,
                                        dataIndex,
                                        result));
                            } else {
                                // We need to run the wrapped filter up to the cost ceiling.
                                wrappedMatcher.pushdownFilter(
                                        filter,
                                        result.maybeMatch(),
                                        usePrev,
                                        ctx.wrappedContext,
                                        costCeiling,
                                        jobScheduler,
                                        nextResult -> {
                                            // Combine the match results from earlier pushdown
                                            nextResult.match().insert(result.match());
                                            onComplete.accept(nextResult);
                                        },
                                        onError);
                            }
                        },
                        onError);
            } else {
                // Should we run the filter against the index?
                if (selection.size() > selectionThreshold) {
                    onComplete.accept(pushdownDataIndex(
                            selection,
                            filter,
                            ctx.renameMap,
                            dataIndex,
                            PushdownResult.allMaybeMatch(selection)));
                } else {
                    onComplete.accept(PushdownResult.allMaybeMatch(selection));
                }
            }
        }
    }

    public static class DataIndexPushdownContext extends BasePushdownFilterContext {
        private final Map<String, String> renameMap;
        private final PushdownFilterContext wrappedContext;

        public DataIndexPushdownContext(
                final DataIndexPushdownManager manager,
                final WhereFilter filter,
                final List<ColumnSource<?>> columnSources,
                final PushdownFilterContext wrappedContext) {
            super(filter, columnSources);
            this.wrappedContext = wrappedContext;

            final List<String> filterColumns = filter.getColumns();
            Require.eq(filterColumns.size(), "filterColumns.size()",
                    columnSources.size(), "columnSources.size()");

            renameMap = new HashMap<>();

            // Map the incoming column sources to the index table column name.
            final Map<ColumnSource<?>, String> indexTableMap = manager.dataIndex.keyColumnNamesByIndexedColumn();
            for (int i = 0; i < columnSources.size(); i++) {
                final String filterColumnName = filterColumns.get(i);
                final ColumnSource<?> filterSource = columnSources.get(i);

                final String indexColumnName = indexTableMap.get(filterSource);
                if (indexColumnName == null) {
                    throw new IllegalStateException("No associated source for '" + filterColumnName
                            + "' found in the index table column sources");
                }
                if (!filterColumnName.equals(indexColumnName)) {
                    renameMap.put(filterColumnName, indexColumnName);
                }
            }
        }

        @Override
        public void close() {
            super.close();
        }
    }

    @Override
    public PushdownFilterContext makePushdownFilterContext(
            @NotNull final WhereFilter filter,
            @NotNull final List<ColumnSource<?>> filterSources) {
        final PushdownFilterContext wrappedContext = wrappedMatcher != null
                ? wrappedMatcher.makePushdownFilterContext(filter, filterSources)
                : null;
        return new DataIndexPushdownContext(this, filter, filterSources, wrappedContext);
    }

    /**
     * Apply the filter to the data index table and return the result.
     */
    @NotNull
    private PushdownResult pushdownDataIndex(
            final RowSet selection,
            final WhereFilter filter,
            final Map<String, String> renameMap,
            final BasicDataIndex dataIndex,
            final PushdownResult result) {
        final RowSetBuilderRandom matchingBuilder = RowSetFactory.builderRandom();
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            // Extract the fundamental filter, ignoring barriers.
            final WhereFilter copiedFilter = ExtractFilterWithoutBarriers.of(filter).copy();
            final Table toFilter;
            if (!renameMap.isEmpty()) {
                final Collection<Pair> renamePairs = renameMap.entrySet().stream()
                        .map(entry -> io.deephaven.api.Pair.of(ColumnName.of(entry.getValue()),
                                ColumnName.of(entry.getKey())))
                        .collect(Collectors.toList());
                toFilter = dataIndex.table().renameColumns(renamePairs);
            } else {
                toFilter = dataIndex.table();
            }
            copiedFilter.init(toFilter.getDefinition());
            try {
                final Table filteredTable = toFilter.where(copiedFilter);
                try (final CloseableIterator<RowSet> it =
                        ColumnVectors.ofObject(filteredTable, dataIndex.rowSetColumnName(), RowSet.class).iterator()) {
                    it.forEachRemaining(rowSet -> {
                        try (final RowSet matching = rowSet.intersect(result.maybeMatch())) {
                            matchingBuilder.addRowSet(matching);
                        }
                    });
                }
            } catch (final Exception e) {
                throw new TableInitializationException(
                        "Error applying filter " + Strings.of(copiedFilter) + " to data index table", e);
            }
        }
        // Retain only the maybe rows and add the previously found matches.
        try (
                final WritableRowSet matching = matchingBuilder.build();
                final WritableRowSet empty = RowSetFactory.empty()) {
            matching.insert(result.match());
            return PushdownResult.of(selection, matching, empty);
        }
    }

    /**
     * Create new {@link DataIndexPushdownManager} for a {@link DataIndex}, wrapping a {@link PushdownFilterMatcher} to
     * be used when the index would not be efficient.
     *
     * @param dataIndex the {@link DataIndex} to use for pushdown
     * @param wrapped the wrapped {@link PushdownFilterMatcher}
     *
     * @return a new {@link PushdownFilterMatcher} supporting DataIndex pushdown, or {@code null} if pushdown is not
     *         supported
     */
    public static PushdownFilterMatcher wrap(
            @NotNull final DataIndex dataIndex,
            final PushdownFilterMatcher wrapped) {
        return new DataIndexPushdownManager(dataIndex, wrapped);
    }
}

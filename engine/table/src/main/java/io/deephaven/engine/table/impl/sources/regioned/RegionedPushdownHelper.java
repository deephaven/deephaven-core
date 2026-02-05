//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Pair;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.LivenessScopeStack;
import io.deephaven.engine.primitive.iterator.CloseableIterator;
import io.deephaven.engine.rowset.*;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.BasePushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownFilterContext;
import io.deephaven.engine.table.impl.PushdownResult;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.impl.filter.ExtractFilterWithoutBarriers;
import io.deephaven.engine.table.impl.locations.TableLocation;
import io.deephaven.engine.table.impl.locations.impl.AbstractTableLocation;
import io.deephaven.engine.table.impl.select.WhereFilter;
import io.deephaven.engine.table.impl.util.JobScheduler;
import io.deephaven.engine.table.vectors.ColumnVectors;
import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;

import java.util.*;
import java.util.function.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class RegionedPushdownHelper {
    public static class PushdownMode {
        // Note: the disabled static configuration values are _not_ currently final. While ill-advised to change them in
        // this way, it is better that we retain the current behavior until a better pattern can be established.
        private final BooleanSupplier disabled;
        private final long filterCost;
        private final Predicate<BasePushdownFilterContext> contextAllows;
        private final Predicate<AbstractTableLocation> locationAllows;

        public PushdownMode(
                final BooleanSupplier disabled,
                final long filterCost,
                final Predicate<BasePushdownFilterContext> contextAllows,
                final Predicate<AbstractTableLocation> locationAllows) {
            this.disabled = Objects.requireNonNull(disabled);
            this.filterCost = filterCost;
            this.contextAllows = Objects.requireNonNull(contextAllows);
            this.locationAllows = Objects.requireNonNull(locationAllows);
        }

        public long filterCost() {
            return filterCost;
        }

        public boolean allows(
                final AbstractTableLocation location,
                final BasePushdownFilterContext context) {
            return !disabled.getAsBoolean()
                    && contextAllows(context)
                    && locationAllows(location);
        }

        public boolean allows(
                final AbstractTableLocation location,
                final BasePushdownFilterContext context,
                final long costCeiling) {
            return allows(location, context) && ceilingAllows(costCeiling);
        }

        private boolean ceilingAllows(final long costCeiling) {
            return filterCost <= costCeiling;
        }

        private boolean contextAllows(final BasePushdownFilterContext context) {
            return context.executedFilterCost() < filterCost && contextAllows.test(context);
        }

        private boolean locationAllows(final AbstractTableLocation location) {
            return locationAllows.test(location);
        }
    }

    /**
     * The context for regioned pushdown parallel JobScheduler operations.
     */
    public static class RegionThreadContext implements JobScheduler.JobThreadContext {
        RowSet shiftedRowSet;

        @Override
        public void close() {
            try (RowSet ignored = shiftedRowSet) {
            }
        }

        public void reset() {
            if (shiftedRowSet != null) {
                shiftedRowSet.close();
                shiftedRowSet = null;
            }
        }
    }

    /**
     * A PushdownFilterContext that delegates to another PushdownFilterContext while storing and providing a
     * TableLocation.
     */
    public static class LocalRegionPushdownContext implements PushdownFilterContext {
        private final TableLocation tableLocation;
        private final PushdownFilterContext context;

        private LocalRegionPushdownContext(
                final PushdownFilterContext context,
                final TableLocation tableLocation) {
            this.context = context;
            this.tableLocation = tableLocation;
        }

        public static LocalRegionPushdownContext of(
                final PushdownFilterContext context,
                final TableLocation tableLocation) {
            return new LocalRegionPushdownContext(context, tableLocation);
        }

        public TableLocation tableLocation() {
            return tableLocation;
        }

        public PushdownFilterContext context() {
            return context;
        }

        @Override
        public long executedFilterCost() {
            return context.executedFilterCost();
        }

        @Override
        public void updateExecutedFilterCost(long executedFilterCost) {
            context.updateExecutedFilterCost(executedFilterCost);
        }

        @Override
        public void close() {
            throw new IllegalStateException("LocalRegionPushdownContext is a wrapper class and should not be closed");
        }
    }

    /**
     * Combine the results from multiple regioned pushdown operations into a unified result.
     */
    public static PushdownResult buildResults(
            final WritableRowSet[] matches,
            final WritableRowSet[] maybeMatches,
            final RowSet selection) {
        final long totalMatchSize = Stream.of(matches).mapToLong(RowSet::size).sum();
        final long totalMaybeMatchSize = Stream.of(maybeMatches).mapToLong(RowSet::size).sum();
        final long selectionSize = selection.size();
        if (totalMatchSize == selectionSize) {
            Assert.eqZero(totalMaybeMatchSize, "totalMaybeMatchSize");
            return PushdownResult.allMatch(selection);
        }
        if (totalMaybeMatchSize == selectionSize) {
            Assert.eqZero(totalMatchSize, "totalMatchSize");
            return PushdownResult.allMaybeMatch(selection);
        }
        if (totalMatchSize == 0 && totalMaybeMatchSize == 0) {
            return PushdownResult.allNoMatch(selection);
        }
        // Note: it's not obvious what the best approach for building these RowSets is; that is, sequential
        // insertion vs sequential builder. We know that the individual results are ordered and non-overlapping.
        // If this becomes important, we can do more benchmarking.
        try (
                final WritableRowSet match = RowSetFactory.unionInsert(Arrays.asList(matches));
                final WritableRowSet maybeMatch = RowSetFactory.unionInsert(Arrays.asList(maybeMatches))) {
            return PushdownResult.of(selection, match, maybeMatch);
        }
    }

    /**
     * Apply the filter to the data index table and return the result.
     */
    @NotNull
    public static PushdownResult pushdownDataIndex(
            final RowSet selection,
            final WhereFilter filter,
            final Map<String, String> renameMap,
            final BasicDataIndex dataIndex,
            final PushdownResult result) {
        final RowSetBuilderRandom matchingBuilder = RowSetFactory.builderRandom();
        try (final SafeCloseable ignored = LivenessScopeStack.open()) {
            final long threshold = (long) (dataIndex.table().size() / QueryTable.DATA_INDEX_FOR_WHERE_THRESHOLD);
            if (result.maybeMatch().size() <= threshold) {
                return result.copy();
            }
            // Extract the fundamental filter, ignoring barriers and serial wrappers.
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
            // Apply the filter to the data index table
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
                // TODO: Exception occurs here if we have a data type mismatch between the index and the filter.
                // When https://deephaven.atlassian.net/browse/DH-19443 is implemented, we should be able
                // to remove the catch block and let any exception propagate. For now, just swallow the exception
                // and return a copy of the original input, skipping pushdown filtering.
                return result.copy();
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
}

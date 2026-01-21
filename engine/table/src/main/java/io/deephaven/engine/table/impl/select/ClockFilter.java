//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.select;

import io.deephaven.base.clock.Clock;
import io.deephaven.base.verify.Require;
import io.deephaven.engine.rowset.WritableRowSet;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.engine.rowset.RowSetBuilderRandom;
import io.deephaven.engine.rowset.RowSetFactory;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.TableDefinition;
import io.deephaven.engine.table.impl.lang.QueryLanguageFunctionUtils;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.ColumnSource;
import io.deephaven.engine.updategraph.NotificationQueue;
import io.deephaven.engine.updategraph.UpdateGraph;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.time.Instant;
import java.util.Collections;
import java.util.List;

/**
 * Boilerplate super-class for various clock-oriented filters.
 */
public abstract class ClockFilter extends WhereFilterLivenessArtifactImpl
        implements ReindexingFilter, Runnable, NotificationQueue.Dependency {

    protected final String columnName;
    protected final Clock clock;
    private final boolean refreshing;

    ColumnSource<Long> nanosColumnSource;
    private QueryTable resultTable;

    @SuppressWarnings("WeakerAccess")
    public ClockFilter(@NotNull final String columnName, @NotNull final Clock clock, final boolean refreshing) {
        this.columnName = columnName;
        this.clock = clock;
        this.refreshing = refreshing;
    }

    @Override
    public final void init(@NotNull final TableDefinition tableDefinition) {}

    @Override
    public final List<String> getColumns() {
        return Collections.singletonList(columnName);
    }

    @Override
    public final List<String> getColumnArrays() {
        return Collections.emptyList();
    }

    @NotNull
    @Override
    public final WritableRowSet filter(
            @NotNull final RowSet selection,
            @NotNull final RowSet fullSet,
            @NotNull final Table table,
            final boolean usePrev) {
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }

        // We have no support for refreshing tables, nor any known use cases for that support.
        Require.requirement(DynamicNode.notDynamicOrNotRefreshing(table),
                "DynamicNode.notDynamicOrNotRefreshing(table)");

        nanosColumnSource = ReinterpretUtils.instantToLongSource(table.getColumnSource(columnName, Instant.class));

        final WritableRowSet initial = initializeAndGetInitialIndex(selection, fullSet, table);
        return initial == null ? RowSetFactory.empty() : initial;
    }

    /**
     * This method is called from the {@link ClockFilter#filter(RowSet, RowSet, Table, boolean)} method to initialize
     * the filter's internal data structures.
     *
     * <p>
     * The filter permits parallelization, so it may be called from many initialization threads concurrently.
     * </p>
     *
     * @param selection the selection to be filtered
     * @param fullSet the full set of rows in the source table
     * @param table the table that is being filtered
     * @return the rowset that matches the filter at initialization time
     */
    @Nullable
    protected abstract WritableRowSet initializeAndGetInitialIndex(@NotNull final RowSet selection,
            @NotNull final RowSet fullSet, @NotNull final Table table);

    @Override
    public final boolean isSimpleFilter() {
        // This doesn't execute any user code, so it should be safe to execute it before ACL filters are applied.
        return true;
    }

    @Override
    public boolean isRefreshing() {
        return refreshing;
    }

    @Override
    public boolean satisfied(long step) {
        return updateGraph.satisfied(step);
    }

    @Override
    public UpdateGraph getUpdateGraph() {
        return updateGraph;
    }

    @Override
    public final void setRecomputeListener(@NotNull final RecomputeListener listener) {
        if (!refreshing) {
            return;
        }
        updateGraph.addSource(this);
        this.resultTable = listener.getTable();
        listener.setIsRefreshing(true);
    }

    @OverridingMethodsMustInvokeSuper
    @Override
    protected void destroy() {
        super.destroy();
        updateGraph.removeSource(this);
    }

    @Override
    public final void run() {
        final RowSet added = updateAndGetAddedIndex();
        if (added != null && !added.isEmpty()) {
            resultTable.getRowSet().writableCast().insert(added);
            resultTable.notifyListeners(added, RowSetFactory.empty(), RowSetFactory.empty());
        }
    }

    /**
     * This method is called from the {@link ClockFilter#run()} method as part of the update graph source refresh.
     *
     * <p>
     * This method is not called concurrently, because there are no updates from the source table.
     * </p>
     */
    @Nullable
    protected abstract WritableRowSet updateAndGetAddedIndex();

    /**
     * Representation of a contiguous key range with monotonically nondecreasing timestamp values.
     */
    protected final static class Range {

        long nextKey;
        private long lastKey;

        protected Range(final long firstKey, final long lastKey) {
            nextKey = Require.leq(firstKey, "firstRowKey", lastKey, "lastRowKey");
            this.lastKey = lastKey;
        }

        protected boolean isEmpty() {
            return nextKey > lastKey;
        }

        @Nullable
        RowSetBuilderRandom consumeKeysAndAppendAdded(final ColumnSource<Long> nanosColumnSource,
                final long nowNanos,
                @Nullable RowSetBuilderRandom addedBuilder) {
            final long firstKeyAdded = nextKey;
            long lastKeyAdded = -1L;
            while (nextKey <= lastKey
                    && QueryLanguageFunctionUtils.lessEquals(nanosColumnSource.getLong(nextKey), nowNanos)) {
                lastKeyAdded = nextKey++;
            }
            if (lastKeyAdded == -1L) {
                return null;
            }
            if (addedBuilder == null) {
                addedBuilder = RowSetFactory.builderRandom();
            }
            addedBuilder.addRange(firstKeyAdded, lastKeyAdded);
            return addedBuilder;
        }

        /**
         * Returns true if this range is before the given key.
         */
        boolean isBefore(final long key) {
            return lastKey < key;
        }

        /**
         * Merges range if contiguous; returns true if the range was merged into this range
         */
        boolean merge(final long firstKey, final long lastKey) {
            if (isEmpty()) {
                this.nextKey = firstKey;
                this.lastKey = lastKey;
                return true;
            }
            if (firstKey == this.lastKey + 1) {
                this.lastKey = lastKey;
                return true;
            }
            if (lastKey == this.nextKey - 1) {
                this.nextKey = firstKey;
                return true;
            }
            return false;
        }

        /**
         * @return the first key in this range
         */
        long firstKey() {
            return nextKey;
        }
    }
}

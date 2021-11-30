/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

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
import io.deephaven.engine.updategraph.UpdateGraphProcessor;
import io.deephaven.time.DateTime;
import io.deephaven.engine.updategraph.DynamicNode;
import io.deephaven.engine.table.impl.QueryTable;
import io.deephaven.engine.table.ColumnSource;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * Boilerplate super-class for various clock-oriented filters.
 */
public abstract class ClockFilter extends WhereFilterLivenessArtifactImpl implements ReindexingFilter, Runnable {

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

    @Override
    public final WritableRowSet filter(@NotNull final RowSet selection, @NotNull final RowSet fullSet,
            @NotNull final Table table, boolean usePrev) {
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }

        // We have no support for refreshing tables, nor any known use cases for that support.
        Require.requirement(DynamicNode.notDynamicOrNotRefreshing(table),
                "DynamicNode.notDynamicOrNotRefreshing(table)");

        final ColumnSource<DateTime> dateTimeColumnSource = table.getColumnSource(columnName);
        // Obviously, column needs to be of date-time values.
        Require.requirement(DateTime.class.isAssignableFrom(dateTimeColumnSource.getType()),
                "DateTime.class.isAssignableFrom(dateTimeColumnSource.getType())");

        nanosColumnSource = dateTimeColumnSource.allowsReinterpret(long.class)
                ? table.dateTimeColumnAsNanos(columnName).getColumnSource(columnName)
                : table.view(columnName + " = isNull(" + columnName + ") ? NULL_LONG : " + columnName + ".getNanos()")
                        .getColumnSource(columnName);

        final WritableRowSet initial = initializeAndGetInitialIndex(selection, fullSet, table);
        return initial == null ? RowSetFactory.empty() : initial;
    }

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
    public final void setRecomputeListener(@NotNull final RecomputeListener listener) {
        if (!refreshing) {
            return;
        }
        UpdateGraphProcessor.DEFAULT.addSource(this);
        this.resultTable = listener.getTable();
        listener.setIsRefreshing(true);
    }

    @Override
    protected void destroy() {
        super.destroy();
        UpdateGraphProcessor.DEFAULT.removeSource(this);
    }

    @Override
    public final void run() {
        final RowSet added = updateAndGetAddedIndex();
        if (added != null && !added.isEmpty()) {
            resultTable.getRowSet().writableCast().insert(added);
            resultTable.notifyListeners(added, RowSetFactory.empty(),
                    RowSetFactory.empty());
        }
    }

    @Nullable
    protected abstract WritableRowSet updateAndGetAddedIndex();

    /**
     * Representation of a contiguous key range with monotonically nondecreasing timestamp values.
     */
    protected final static class Range {

        long nextKey;
        private final long lastKey;

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
    }
}

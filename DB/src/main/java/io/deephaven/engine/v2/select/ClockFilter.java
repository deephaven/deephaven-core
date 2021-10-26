/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.v2.select;

import io.deephaven.base.verify.Require;
import io.deephaven.base.clock.Clock;
import io.deephaven.engine.tables.Table;
import io.deephaven.engine.tables.TableDefinition;
import io.deephaven.engine.tables.lang.DBLanguageFunctionUtil;
import io.deephaven.engine.tables.live.LiveTable;
import io.deephaven.engine.tables.live.LiveTableMonitor;
import io.deephaven.engine.tables.utils.DBDateTime;
import io.deephaven.engine.v2.DynamicNode;
import io.deephaven.engine.v2.QueryTable;
import io.deephaven.engine.v2.sources.ColumnSource;
import io.deephaven.engine.v2.utils.*;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collections;
import java.util.List;

/**
 * Boilerplate super-class for various clock-oriented filters.
 */
public abstract class ClockFilter extends SelectFilterLivenessArtifactImpl implements ReindexingFilter, LiveTable {

    protected final String columnName;
    protected final Clock clock;
    private final boolean live;

    ColumnSource<Long> nanosColumnSource;
    private QueryTable resultTable;

    @SuppressWarnings("WeakerAccess")
    public ClockFilter(@NotNull final String columnName, @NotNull final Clock clock, final boolean live) {
        this.columnName = columnName;
        this.clock = clock;
        this.live = live;
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
    public final TrackingMutableRowSet filter(@NotNull final TrackingMutableRowSet selection, @NotNull final RowSet fullSet, @NotNull final Table table,
                                              boolean usePrev) {
        if (usePrev) {
            throw new PreviousFilteringNotSupported();
        }

        // We have no support for refreshing tables, nor any known use cases for that support.
        Require.requirement(DynamicNode.notDynamicOrNotRefreshing(table),
                "DynamicNode.notDynamicOrNotRefreshing(table)");

        // noinspection unchecked
        final ColumnSource<DBDateTime> dateTimeColumnSource = table.getColumnSource(columnName);
        // Obviously, column needs to be of date-time values.
        Require.requirement(DBDateTime.class.isAssignableFrom(dateTimeColumnSource.getType()),
                "DBDateTime.class.isAssignableFrom(dateTimeColumnSource.getType())");

        // noinspection unchecked
        nanosColumnSource = dateTimeColumnSource.allowsReinterpret(long.class)
                ? table.dateTimeColumnAsNanos(columnName).getColumnSource(columnName)
                : table.view(columnName + " = isNull(" + columnName + ") ? NULL_LONG : " + columnName + ".getNanos()")
                        .getColumnSource(columnName);

        final RowSet initial = initializeAndGetInitialIndex(selection, fullSet, table);
        return initial == null ? RowSetFactoryImpl.INSTANCE.getEmptyRowSet() : initial;
    }

    protected abstract @Nullable
    RowSet initializeAndGetInitialIndex(@NotNull final RowSet selection,
                                        @NotNull final RowSet fullSet, @NotNull final Table table);

    @Override
    public final boolean isSimpleFilter() {
        // This doesn't execute any user code, so it should be safe to execute it before ACL filters are applied.
        return true;
    }

    @Override
    public boolean isRefreshing() {
        return true;
    }

    @Override
    public final void setRecomputeListener(@NotNull final RecomputeListener listener) {
        if (!live) {
            return;
        }
        LiveTableMonitor.DEFAULT.addTable(this);
        this.resultTable = listener.getTable();
        listener.setIsRefreshing(true);
    }

    @Override
    protected void destroy() {
        super.destroy();
        LiveTableMonitor.DEFAULT.removeTable(this);
    }

    @Override
    public final void refresh() {
        final RowSet added = updateAndGetAddedIndex();
        if (added != null && !added.isEmpty()) {
            resultTable.getRowSet().insert(added);
            resultTable.notifyListeners(added, RowSetFactoryImpl.INSTANCE.getEmptyRowSet(), RowSetFactoryImpl.INSTANCE.getEmptyRowSet());
        }
    }

    protected abstract @Nullable
    RowSet updateAndGetAddedIndex();

    boolean isLive() {
        return live;
    }

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
                    && DBLanguageFunctionUtil.lessEquals(nanosColumnSource.getLong(nextKey), nowNanos)) {
                lastKeyAdded = nextKey++;
            }
            if (lastKeyAdded == -1L) {
                return null;
            }
            if (addedBuilder == null) {
                addedBuilder = RowSetFactoryImpl.INSTANCE.getRandomBuilder();
            }
            addedBuilder.addRange(firstKeyAdded, lastKeyAdded);
            return addedBuilder;
        }
    }
}

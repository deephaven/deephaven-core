/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2;

import io.deephaven.base.verify.Assert;
import io.deephaven.db.tables.*;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.select.WouldMatchPair;
import io.deephaven.util.QueryConstants;
import io.deephaven.db.util.liveness.Liveness;
import io.deephaven.db.v2.by.AggregationStateFactory;
import io.deephaven.db.v2.by.ComboAggregateFactory;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.db.v2.select.SelectFilter;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Map;

/**
 * Abstract class for uncoalesced tables. These tables have deferred work that must be done before
 * data can be operated on.
 */
public abstract class UncoalescedTable extends BaseTable implements Table {

    private final Object coalescingLock = new Object();

    private volatile DynamicTable coalesced;

    public UncoalescedTable(@NotNull final TableDefinition definition,
        @NotNull final String description) {
        super(definition, description);
    }

    /**
     * Produce the actual coalesced result table, suitable for caching.
     * <p>
     * Note that if this table must have listeners registered, etc, setting these up is the
     * implementation's responsibility.
     * <p>
     * Also note that the implementation should copy attributes, as in
     * {@code copyAttributes(resultTable, CopyAttributeOperation.Coalesce)}.
     *
     * @return The coalesced result table, suitable for caching
     */
    protected abstract DynamicTable doCoalesce();

    public final Table coalesce() {
        DynamicTable localCoalesced;
        if (Liveness.verifyCachedObjectForReuse(localCoalesced = coalesced)) {
            return localCoalesced;
        }
        synchronized (coalescingLock) {
            if (Liveness.verifyCachedObjectForReuse(localCoalesced = coalesced)) {
                return localCoalesced;
            }
            return coalesced = doCoalesce();
        }
    }

    /**
     * Proactively set the coalesced result table. See {@link #doCoalesce()} for the caller's
     * responsibilities. Note that it is an error to call this more than once with a non-null input.
     *
     * @param coalesced The coalesced result table, suitable for caching
     */
    protected final void setCoalesced(final DynamicTable coalesced) {
        synchronized (coalescingLock) {
            Assert.eqNull(this.coalesced, "this.coalesced");
            this.coalesced = coalesced;
        }
    }

    protected @Nullable final Table getCoalesced() {
        return coalesced;
    }

    @Override
    public void listenForUpdates(Listener listener) {
        ((DynamicTable) coalesce()).listenForUpdates(listener);
    }

    @Override
    public void listenForUpdates(Listener listener, boolean replayInitialImage) {
        ((DynamicTable) coalesce()).listenForUpdates(listener, replayInitialImage);
    }

    @Override
    public void listenForUpdates(ShiftAwareListener listener) {
        ((DynamicTable) coalesce()).listenForUpdates(listener);
    }

    protected final void listenForUpdatesUncoalesced(@NotNull final ShiftAwareListener listener) {
        super.listenForUpdates(listener);
    }

    @Override
    public void removeUpdateListener(Listener listener) {
        ((DynamicTable) coalesce()).removeUpdateListener(listener);
    }

    @Override
    public void removeUpdateListener(ShiftAwareListener listener) {
        ((DynamicTable) coalesce()).removeUpdateListener(listener);
    }

    protected final void removeUpdateListenerUncoalesced(
        @NotNull final ShiftAwareListener listener) {
        super.removeUpdateListener(listener);
    }

    @Override
    public void listenForDirectUpdates(Listener listener) {
        ((DynamicTable) coalesce()).listenForDirectUpdates(listener);
    }

    @Override
    public void removeDirectUpdateListener(Listener listener) {
        ((DynamicTable) coalesce()).removeUpdateListener(listener);
    }

    @Override
    public Index getIndex() {
        return coalesce().getIndex();
    }

    @Override
    public long size() {
        return coalesce().size();
    }

    @Override
    public long sizeForInstrumentation() {
        return QueryConstants.NULL_LONG;
    }

    @Override
    public ColumnSource getColumnSource(String sourceName) {
        return coalesce().getColumnSource(sourceName);
    }

    @Override
    public Map<String, ? extends ColumnSource> getColumnSourceMap() {
        return coalesce().getColumnSourceMap();
    }

    @Override
    public Collection<? extends ColumnSource> getColumnSources() {
        return coalesce().getColumnSources();
    }

    @Override
    public DataColumn getColumn(String columnName) {
        return coalesce().getColumn(columnName);
    }

    @Override
    public Object[] getRecord(long rowNo, String... columnNames) {
        return coalesce().getRecord(rowNo, columnNames);
    }

    @Override
    public Table where(SelectFilter... filters) {
        return coalesce().where(filters);
    }

    @Override
    public Table whereIn(GroupStrategy groupStrategy, Table rightTable, boolean inclusion,
        MatchPair... columnsToMatch) {
        return coalesce().whereIn(groupStrategy, rightTable, inclusion, columnsToMatch);
    }

    @Override
    public Table getSubTable(Index index) {
        return coalesce().getSubTable(index);
    }

    @Override
    public Table select(SelectColumn... columns) {
        return coalesce().select(columns);
    }

    @Override
    public Table selectDistinct(SelectColumn... columns) {
        return coalesce().selectDistinct(columns);
    }

    @Override
    public Table update(SelectColumn... newColumns) {
        return coalesce().update(newColumns);
    }

    @Override
    public Table view(SelectColumn... columns) {
        return coalesce().view(columns);
    }

    @Override
    public Table updateView(SelectColumn... newColumns) {
        return coalesce().updateView(newColumns);
    }

    @Override
    public SelectValidationResult validateSelect(SelectColumn... columns) {
        return coalesce().validateSelect(columns);
    }

    @Override
    public Table lazyUpdate(SelectColumn... newColumns) {
        return coalesce().lazyUpdate(newColumns);
    }

    @Override
    public Table dropColumns(String... columnNames) {
        return coalesce().dropColumns(columnNames);
    }

    @Override
    public Table renameColumns(MatchPair... pairs) {
        return coalesce().renameColumns(pairs);
    }

    @Override
    public Table slice(long firstRowInclusive, long lastRowExclusive) {
        return coalesce().slice(firstRowInclusive, lastRowExclusive);
    }

    @Override
    public Table head(long size) {
        return coalesce().head(size);
    }

    @Override
    public Table tail(long size) {
        return coalesce().tail(size);
    }

    @Override
    public Table headPct(double percent) {
        return coalesce().headPct(percent);
    }

    @Override
    public Table tailPct(double percent) {
        return coalesce().tailPct(percent);
    }

    @Override
    public Table leftJoin(Table table, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return coalesce().leftJoin(table, columnsToMatch, columnsToAdd);
    }

    @Override
    public Table exactJoin(Table table, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return coalesce().exactJoin(table, columnsToMatch, columnsToAdd);
    }

    @Override
    public Table aj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
        AsOfMatchRule asOfMatchRule) {
        return coalesce().aj(rightTable, columnsToMatch, columnsToAdd, asOfMatchRule);
    }

    @Override
    public Table raj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
        AsOfMatchRule asOfMatchRule) {
        return coalesce().raj(rightTable, columnsToMatch, columnsToAdd, asOfMatchRule);
    }

    @Override
    public Table naturalJoin(Table rightTable, MatchPair[] columnsToMatch,
        MatchPair[] columnsToAdd) {
        return coalesce().naturalJoin(rightTable, columnsToMatch, columnsToAdd);
    }

    @Override
    public Table join(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
        int numRightBitsToReserve) {
        return coalesce().join(rightTable, columnsToMatch, columnsToAdd, numRightBitsToReserve);
    }

    @Override
    public Table by(AggregationStateFactory aggregationStateFactory,
        SelectColumn... groupByColumns) {
        return coalesce().by(aggregationStateFactory, groupByColumns);
    }

    @Override
    public Table headBy(long nRows, String... groupByColumns) {
        return coalesce().headBy(nRows, groupByColumns);
    }

    @Override
    public Table tailBy(long nRows, String... groupByColumns) {
        return coalesce().tailBy(nRows, groupByColumns);
    }

    @Override
    public Table applyToAllBy(String formulaColumn, String columnParamName,
        SelectColumn... groupByColumns) {
        return coalesce().applyToAllBy(formulaColumn, columnParamName, groupByColumns);
    }

    @Override
    public Table sumBy(SelectColumn... groupByColumns) {
        return coalesce().sumBy(groupByColumns);
    }

    @Override
    public Table absSumBy(SelectColumn... groupByColumns) {
        return coalesce().absSumBy(groupByColumns);
    }

    @Override
    public Table avgBy(SelectColumn... groupByColumns) {
        return coalesce().avgBy(groupByColumns);
    }

    @Override
    public Table wavgBy(String weightColumn, SelectColumn... groupByColumns) {
        return coalesce().wavgBy(weightColumn, groupByColumns);
    }

    @Override
    public Table wsumBy(String weightColumn, SelectColumn... groupByColumns) {
        return coalesce().wsumBy(weightColumn, groupByColumns);
    }

    @Override
    public Table stdBy(SelectColumn... groupByColumns) {
        return coalesce().stdBy(groupByColumns);
    }

    @Override
    public Table varBy(SelectColumn... groupByColumns) {
        return coalesce().varBy(groupByColumns);
    }

    @Override
    public Table lastBy(SelectColumn... groupByColumns) {
        return coalesce().lastBy(groupByColumns);
    }

    @Override
    public Table firstBy(SelectColumn... groupByColumns) {
        return coalesce().firstBy(groupByColumns);
    }

    @Override
    public Table minBy(SelectColumn... groupByColumns) {
        return coalesce().minBy(groupByColumns);
    }

    @Override
    public Table maxBy(SelectColumn... groupByColumns) {
        return coalesce().maxBy(groupByColumns);
    }

    @Override
    public Table medianBy(SelectColumn... groupByColumns) {
        return coalesce().medianBy(groupByColumns);
    }

    @Override
    public Table countBy(String countColumnName, SelectColumn... groupByColumns) {
        return coalesce().countBy(countColumnName, groupByColumns);
    }

    @Override
    public Table ungroup(boolean nullFill, String... columnsToUngroup) {
        return coalesce().ungroup(nullFill, columnsToUngroup);
    }

    @Override
    public TableMap byExternal(boolean dropKeys, String... keyColumnNames) {
        return coalesce().byExternal(dropKeys, keyColumnNames);
    }

    @Override
    public Table rollup(ComboAggregateFactory comboAggregateFactory, boolean includeConstituents,
        SelectColumn... columns) {
        return coalesce().rollup(comboAggregateFactory, includeConstituents, columns);
    }

    @Override
    public Table treeTable(String idColumn, String parentColumn) {
        return coalesce().treeTable(idColumn, parentColumn);
    }

    @Override
    public Table sort(SortPair... columnsToSortBy) {
        return coalesce().sort(columnsToSortBy);
    }

    @Override
    public Table reverse() {
        return coalesce().reverse();
    }

    @Override
    public Table snapshot(Table baseTable, boolean doInitialSnapshot, String... stampColumns) {
        return coalesce().snapshot(baseTable, doInitialSnapshot, stampColumns);
    }

    @Override
    public Table snapshotIncremental(Table rightTable, boolean doInitialSnapshot,
        String... stampColumns) {
        return coalesce().snapshotIncremental(rightTable, doInitialSnapshot, stampColumns);
    }

    @Override
    public Table snapshotHistory(Table rightTable) {
        return coalesce().snapshotHistory(rightTable);
    }

    @Override
    public boolean isFlat() {
        return false;
    }

    @Override
    public Table flatten() {
        return coalesce().flatten();
    }

    @Override
    @Deprecated
    public void addColumnGrouping(String columnName) {
        coalesce().addColumnGrouping(columnName);
    }

    @Override
    public Table wouldMatch(WouldMatchPair... matchers) {
        return coalesce().wouldMatch(matchers);
    }
}

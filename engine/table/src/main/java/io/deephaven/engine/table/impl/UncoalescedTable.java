/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.base.verify.Assert;
import io.deephaven.engine.liveness.Liveness;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.iterators.*;
import io.deephaven.api.util.ConcurrentMethod;
import io.deephaven.util.QueryConstants;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Function;

/**
 * Abstract class for uncoalesced tables. These tables have deferred work that must be done before data can be operated
 * on.
 */
public abstract class UncoalescedTable<IMPL_TYPE extends UncoalescedTable<IMPL_TYPE>> extends BaseTable<IMPL_TYPE> {

    private final Object coalescingLock = new Object();

    private volatile Table coalesced;

    public UncoalescedTable(@NotNull final TableDefinition definition, @NotNull final String description) {
        super(definition, description, null);
    }

    // region coalesce support

    /**
     * Produce the actual coalesced result table, suitable for caching.
     * <p>
     * Note that if this table must have listeners registered, etc, setting these up is the implementation's
     * responsibility.
     * <p>
     * Also note that the implementation should copy attributes, as in
     * {@code copyAttributes(resultTable, CopyAttributeOperation.Coalesce)}.
     *
     * @return The coalesced result table, suitable for caching
     */
    protected abstract Table doCoalesce();

    public final Table coalesce() {
        Table localCoalesced;
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
     * Proactively set the coalesced result table. See {@link #doCoalesce()} for the caller's responsibilities. Note
     * that it is an error to call this more than once with a non-null input.
     *
     * @param coalesced The coalesced result table, suitable for caching
     */
    protected final void setCoalesced(final Table coalesced) {
        synchronized (coalescingLock) {
            Assert.eqNull(this.coalesced, "this.coalesced");
            this.coalesced = coalesced;
        }
    }

    protected @Nullable final Table getCoalesced() {
        return coalesced;
    }

    // endregion coalesce support

    // region uncoalesced listeners

    protected final void addUpdateListenerUncoalesced(@NotNull final TableUpdateListener listener) {
        super.addUpdateListener(listener);
    }

    protected final void removeUpdateListenerUncoalesced(@NotNull final TableUpdateListener listener) {
        super.removeUpdateListener(listener);
    }

    // endregion uncoalesced listeners

    // region non-delegated overrides

    @Override
    public long sizeForInstrumentation() {
        return QueryConstants.NULL_LONG;
    }

    @Override
    public boolean isFlat() {
        return false;
    }

    // endregion non-delegated methods

    // region delegated methods

    @Override
    public long size() {
        return coalesce().size();
    }

    @Override
    public TrackingRowSet getRowSet() {
        return coalesce().getRowSet();
    }

    @Override
    public <T> ColumnSource<T> getColumnSource(String sourceName) {
        return coalesce().getColumnSource(sourceName);
    }

    @Override
    public Map<String, ? extends ColumnSource<?>> getColumnSourceMap() {
        return coalesce().getColumnSourceMap();
    }

    @Override
    public Collection<? extends ColumnSource<?>> getColumnSources() {
        return coalesce().getColumnSources();
    }

    @Override
    public DataColumn[] getColumns() {
        return coalesce().getColumns();
    }

    @Override
    public DataColumn getColumn(String columnName) {
        return coalesce().getColumn(columnName);
    }

    @Override
    public <TYPE> Iterator<TYPE> columnIterator(@NotNull String columnName) {
        return coalesce().columnIterator(columnName);
    }

    @Override
    public CharacterColumnIterator characterColumnIterator(@NotNull String columnName) {
        return coalesce().characterColumnIterator(columnName);
    }

    @Override
    public ByteColumnIterator byteColumnIterator(@NotNull String columnName) {
        return coalesce().byteColumnIterator(columnName);
    }

    @Override
    public ShortColumnIterator shortColumnIterator(@NotNull String columnName) {
        return coalesce().shortColumnIterator(columnName);
    }

    @Override
    public IntegerColumnIterator integerColumnIterator(@NotNull String columnName) {
        return coalesce().integerColumnIterator(columnName);
    }

    @Override
    public LongColumnIterator longColumnIterator(@NotNull String columnName) {
        return coalesce().longColumnIterator(columnName);
    }

    @Override
    public FloatColumnIterator floatColumnIterator(@NotNull String columnName) {
        return coalesce().floatColumnIterator(columnName);
    }

    @Override
    public DoubleColumnIterator doubleColumnIterator(@NotNull String columnName) {
        return coalesce().doubleColumnIterator(columnName);
    }

    @Override
    public <DATA_TYPE> ObjectColumnIterator<DATA_TYPE> objectColumnIterator(@NotNull String columnName) {
        return coalesce().objectColumnIterator(columnName);
    }

    @Override
    public Object[] getRecord(long rowNo, String... columnNames) {
        return coalesce().getRecord(rowNo, columnNames);
    }

    @Override
    @ConcurrentMethod
    public Table where(Collection<? extends Filter> filters) {
        return coalesce().where(filters);
    }

    @Override
    @ConcurrentMethod
    public Table wouldMatch(WouldMatchPair... matchers) {
        return coalesce().wouldMatch(matchers);
    }

    @Override
    public Table whereIn(Table rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return coalesce().whereIn(rightTable, columnsToMatch);
    }

    @Override
    public Table whereNotIn(Table rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return coalesce().whereNotIn(rightTable, columnsToMatch);
    }

    @Override
    public Table select(Collection<? extends Selectable> columns) {
        return coalesce().select(columns);
    }

    @Override
    @ConcurrentMethod
    public Table selectDistinct(Collection<? extends Selectable> columns) {
        return coalesce().selectDistinct(columns);
    }

    @Override
    public Table update(Collection<? extends Selectable> columns) {
        return coalesce().update(columns);
    }

    @Override
    public Table lazyUpdate(Collection<? extends Selectable> newColumns) {
        return coalesce().lazyUpdate(newColumns);
    }

    @Override
    @ConcurrentMethod
    public Table view(Collection<? extends Selectable> columns) {
        return coalesce().view(columns);
    }

    @Override
    @ConcurrentMethod
    public Table updateView(Collection<? extends Selectable> columns) {
        return coalesce().updateView(columns);
    }

    @Override
    @ConcurrentMethod
    public Table dropColumns(String... columnNames) {
        return coalesce().dropColumns(columnNames);
    }

    @Override
    public Table renameColumns(MatchPair... pairs) {
        return coalesce().renameColumns(pairs);
    }

    @Override
    @ConcurrentMethod
    public Table moveColumns(int index, boolean moveToEnd, String... columnsToMove) {
        return coalesce().moveColumns(index, moveToEnd, columnsToMove);
    }

    @Override
    @ConcurrentMethod
    public Table dateTimeColumnAsNanos(String dateTimeColumnName, String nanosColumnName) {
        return coalesce().dateTimeColumnAsNanos(dateTimeColumnName, nanosColumnName);
    }

    @Override
    @ConcurrentMethod
    public Table head(long size) {
        return coalesce().head(size);
    }

    @Override
    @ConcurrentMethod
    public Table tail(long size) {
        return coalesce().tail(size);
    }

    @Override
    @ConcurrentMethod
    public Table slice(long firstPositionInclusive, long lastPositionExclusive) {
        return coalesce().slice(firstPositionInclusive, lastPositionExclusive);
    }

    @Override
    @ConcurrentMethod
    public Table headPct(double percent) {
        return coalesce().headPct(percent);
    }

    @Override
    @ConcurrentMethod
    public Table tailPct(double percent) {
        return coalesce().tailPct(percent);
    }

    @Override
    public Table exactJoin(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return coalesce().exactJoin(rightTable, columnsToMatch, columnsToAdd);
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
    public Table naturalJoin(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return coalesce().naturalJoin(rightTable, columnsToMatch, columnsToAdd);
    }

    @Override
    public Table join(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
            int numRightBitsToReserve) {
        return coalesce().join(rightTable, columnsToMatch, columnsToAdd, numRightBitsToReserve);
    }

    @Override
    @ConcurrentMethod
    public Table aggAllBy(AggSpec spec, ColumnName... groupByColumns) {
        return coalesce().aggAllBy(spec, groupByColumns);
    }

    @Override
    @ConcurrentMethod
    public Table aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty, Table initialGroups,
            Collection<? extends ColumnName> groupByColumns) {
        return coalesce().aggBy(aggregations, preserveEmpty, initialGroups, groupByColumns);
    }

    @Override
    public Table headBy(long nRows, String... groupByColumnNames) {
        return coalesce().headBy(nRows, groupByColumnNames);
    }

    @Override
    public Table tailBy(long nRows, String... groupByColumnNames) {
        return coalesce().tailBy(nRows, groupByColumnNames);
    }

    @Override
    public Table ungroup(boolean nullFill, Collection<? extends ColumnName> columnsToUngroup) {
        return coalesce().ungroup(nullFill, columnsToUngroup);
    }

    @Override
    @ConcurrentMethod
    public PartitionedTable partitionBy(boolean dropKeys, String... keyColumnNames) {
        return coalesce().partitionBy(dropKeys, keyColumnNames);
    }

    @Override
    @ConcurrentMethod
    public PartitionedTable partitionedAggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty,
            Table initialGroups, String... keyColumnNames) {
        return coalesce().partitionedAggBy(aggregations, preserveEmpty, initialGroups, keyColumnNames);
    }

    @Override
    @ConcurrentMethod
    public RollupTable rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents,
            Collection<? extends ColumnName> groupByColumns) {
        return coalesce().rollup(aggregations, includeConstituents, groupByColumns);
    }

    @Override
    @ConcurrentMethod
    public TreeTable tree(String idColumn, String parentColumn) {
        return coalesce().tree(idColumn, parentColumn);
    }

    @Override
    @ConcurrentMethod
    public Table updateBy(@NotNull final UpdateByControl control,
            @NotNull final Collection<? extends UpdateByOperation> ops,
            @NotNull final Collection<? extends ColumnName> byColumns) {
        return UpdateBy.updateBy((QueryTable) this.coalesce(), ops, byColumns, control);
    }

    @Override
    @ConcurrentMethod
    public Table sort(Collection<SortColumn> columnsToSortBy) {
        return coalesce().sort(columnsToSortBy);
    }

    @Override
    @ConcurrentMethod
    public Table reverse() {
        return coalesce().reverse();
    }

    @Override
    public Table snapshot(Table baseTable, boolean doInitialSnapshot, String... stampColumns) {
        return coalesce().snapshot(baseTable, doInitialSnapshot, stampColumns);
    }

    @Override
    public Table snapshotIncremental(Table rightTable, boolean doInitialSnapshot, String... stampColumns) {
        return coalesce().snapshotIncremental(rightTable, doInitialSnapshot, stampColumns);
    }

    @Override
    public Table snapshotHistory(Table rightTable) {
        return coalesce().snapshotHistory(rightTable);
    }

    @Override
    public Table getSubTable(TrackingRowSet rowSet) {
        return coalesce().getSubTable(rowSet);
    }

    @Override
    public <R> R apply(Function<Table, R> function) {
        return coalesce().apply(function);
    }

    @Override
    @ConcurrentMethod
    public Table flatten() {
        return coalesce().flatten();
    }

    @Override
    public void awaitUpdate() throws InterruptedException {
        coalesce().awaitUpdate();
    }

    @Override
    public boolean awaitUpdate(long timeout) throws InterruptedException {
        return coalesce().awaitUpdate(timeout);
    }

    @Override
    public void addUpdateListener(ShiftObliviousListener listener, boolean replayInitialImage) {
        coalesce().addUpdateListener(listener, replayInitialImage);
    }

    @Override
    public void addUpdateListener(TableUpdateListener listener) {
        coalesce().addUpdateListener(listener);
    }

    @Override
    public void removeUpdateListener(ShiftObliviousListener listener) {
        coalesce().removeUpdateListener(listener);
    }

    @Override
    public void removeUpdateListener(TableUpdateListener listener) {
        coalesce().removeUpdateListener(listener);
    }

    // endregion delegated methods
}

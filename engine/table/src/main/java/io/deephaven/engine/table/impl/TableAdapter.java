package io.deephaven.engine.table.impl;

import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.base.log.LogOutput;
import io.deephaven.engine.liveness.LivenessReferent;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.hierarchical.RollupTable;
import io.deephaven.engine.table.hierarchical.TreeTable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.lang.ref.WeakReference;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Adapter sub-interface of {@link TableDefaults} that allows implementors to selectively support an opt-in subset of
 * the typical concrete methods for a {@link io.deephaven.engine.table.Table} implementation.
 */
public interface TableAdapter extends TableDefaults {

    private <T> T throwUnsupported() {
        final StackTraceElement caller = Thread.currentThread().getStackTrace()[1];
        throw new UnsupportedOperationException(getClass().getName() + " does not support " + caller.getMethodName());
    }

    @Override
    default LogOutput append(LogOutput logOutput) {
        return throwUnsupported();
    }

    @Override
    default long size() {
        return throwUnsupported();
    }

    @Override
    default TableDefinition getDefinition() {
        return throwUnsupported();
    }

    @Override
    default String getDescription() {
        return throwUnsupported();
    }

    @Override
    default boolean isRefreshing() {
        return throwUnsupported();
    }

    @Override
    default boolean setRefreshing(boolean refreshing) {
        return throwUnsupported();
    }

    @Override
    default void addParentReference(Object parent) {
        throwUnsupported();
    }

    @Override
    default TrackingRowSet getRowSet() {
        return throwUnsupported();
    }

    @Override
    default boolean isFlat() {
        return throwUnsupported();
    }

    @Override
    default Table withAttributes(@NotNull Map<String, Object> toAdd, @NotNull Collection<String> toRemove) {
        return throwUnsupported();
    }

    @Override
    default Table withAttributes(@NotNull Map<String, Object> toAdd) {
        return throwUnsupported();
    }

    @Override
    default Table withoutAttributes(@NotNull Collection<String> toRemove) {
        return throwUnsupported();
    }

    @Override
    default Table retainingAttributes(@NotNull Collection<String> toRetain) {
        return throwUnsupported();
    }

    @Override
    @Nullable
    default Object getAttribute(@NotNull String key) {
        return throwUnsupported();
    }

    @Override
    @NotNull
    default Set<String> getAttributeKeys() {
        return throwUnsupported();
    }

    @Override
    default boolean hasAttribute(@NotNull String name) {
        return throwUnsupported();
    }

    @Override
    @NotNull
    default Map<String, Object> getAttributes() {
        return throwUnsupported();
    }

    @Override
    @NotNull
    default Map<String, Object> getAttributes(@Nullable Predicate<String> included) {
        return throwUnsupported();
    }

    @Override
    default <T> ColumnSource<T> getColumnSource(String sourceName) {
        return throwUnsupported();
    }

    @Override
    default Map<String, ? extends ColumnSource<?>> getColumnSourceMap() {
        return throwUnsupported();
    }

    @Override
    default Collection<? extends ColumnSource<?>> getColumnSources() {
        return throwUnsupported();
    }

    @Override
    default DataColumn getColumn(String columnName) {
        return throwUnsupported();
    }

    @Override
    default Object[] getRecord(long rowNo, String... columnNames) {
        return throwUnsupported();
    }

    @Override
    default Table wouldMatch(WouldMatchPair... matchers) {
        return throwUnsupported();
    }

    @Override
    default Table dropColumns(String... columnNames) {
        return throwUnsupported();
    }

    @Override
    default Table renameColumns(MatchPair... pairs) {
        return throwUnsupported();
    }

    @Override
    default Table moveColumns(int index, boolean moveToEnd, String... columnsToMove) {
        return throwUnsupported();
    }

    @Override
    default Table dateTimeColumnAsNanos(String dateTimeColumnName, String nanosColumnName) {
        return throwUnsupported();
    }

    @Override
    default Table slice(long firstPositionInclusive, long lastPositionExclusive) {
        return throwUnsupported();
    }

    @Override
    default Table headPct(double percent) {
        return throwUnsupported();
    }

    @Override
    default Table tailPct(double percent) {
        return throwUnsupported();
    }

    @Override
    default Table exactJoin(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return throwUnsupported();
    }

    @Override
    default Table aj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
            AsOfMatchRule asOfMatchRule) {
        return throwUnsupported();
    }

    @Override
    default Table raj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
            AsOfMatchRule asOfMatchRule) {
        return throwUnsupported();
    }

    @Override
    default Table naturalJoin(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return throwUnsupported();
    }

    @Override
    default Table join(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
            int numRightBitsToReserve) {
        return throwUnsupported();
    }

    @Override
    default Table headBy(long nRows, String... groupByColumnNames) {
        return throwUnsupported();
    }

    @Override
    default Table tailBy(long nRows, String... groupByColumnNames) {
        return throwUnsupported();
    }

    @Override
    default Table dropStream() {
        return throwUnsupported();
    }

    @Override
    default PartitionedTable partitionBy(boolean dropKeys, String... keyColumnNames) {
        return throwUnsupported();
    }

    @Override
    default PartitionedTable partitionedAggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty,
            Table initialGroups, String... keyColumnNames) {
        return throwUnsupported();
    }

    @Override
    default RollupTable rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents,
                               Collection<? extends ColumnName> groupByColumns) {
        return throwUnsupported();
    }

    @Override
    default TreeTable tree(String idColumn, String parentColumn) {
        return throwUnsupported();
    }

    @Override
    default Table snapshotIncremental(Table rightTable, boolean doInitialSnapshot, String... stampColumns) {
        return throwUnsupported();
    }

    @Override
    default Table snapshotHistory(Table rightTable) {
        return throwUnsupported();
    }

    @Override
    default Table getSubTable(TrackingRowSet rowSet) {
        return throwUnsupported();
    }

    @Override
    default Table flatten() {
        return throwUnsupported();
    }

    @Override
    default Table withKeys(String... columns) {
        return throwUnsupported();
    }

    @Override
    default Table withUniqueKeys(String... columns) {
        return throwUnsupported();
    }

    @Override
    default Table setTotalsTable(String directive) {
        return throwUnsupported();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    default void awaitUpdate() throws InterruptedException {
        throwUnsupported();
    }

    @SuppressWarnings("RedundantThrows")
    @Override
    default boolean awaitUpdate(long timeout) throws InterruptedException {
        return throwUnsupported();
    }

    @Override
    default void addUpdateListener(ShiftObliviousListener listener, boolean replayInitialImage) {
        throwUnsupported();
    }

    @Override
    default void addUpdateListener(TableUpdateListener listener) {
        throwUnsupported();
    }

    @Override
    default void removeUpdateListener(ShiftObliviousListener listener) {
        throwUnsupported();
    }

    @Override
    default void removeUpdateListener(TableUpdateListener listener) {
        throwUnsupported();
    }

    @Override
    default boolean tryManage(@NotNull LivenessReferent referent) {
        return throwUnsupported();
    }

    @Override
    default boolean tryUnmanage(@NotNull LivenessReferent referent) {
        return throwUnsupported();
    }

    @Override
    default boolean tryUnmanage(@NotNull Stream<? extends LivenessReferent> referents) {
        return throwUnsupported();
    }

    @Override
    default boolean tryRetainReference() {
        return throwUnsupported();
    }

    @Override
    default void dropReference() {
        throwUnsupported();
    }

    @Override
    default WeakReference<? extends LivenessReferent> getWeakReference() {
        return throwUnsupported();
    }

    @Override
    default boolean satisfied(long step) {
        return throwUnsupported();
    }

    @Override
    default Table head(long size) {
        return throwUnsupported();
    }

    @Override
    default Table tail(long size) {
        return throwUnsupported();
    }

    @Override
    default Table reverse() {
        return throwUnsupported();
    }

    @Override
    default Table snapshot(Table baseTable, boolean doInitialSnapshot, String... stampColumns) {
        return throwUnsupported();
    }

    @Override
    default Table sort(Collection<SortColumn> columnsToSortBy) {
        return throwUnsupported();
    }

    @Override
    default Table where(Collection<? extends Filter> filters) {
        return throwUnsupported();
    }

    @Override
    default Table whereIn(Table rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return throwUnsupported();
    }

    @Override
    default Table whereNotIn(Table rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return throwUnsupported();
    }

    @Override
    default Table view(Collection<? extends Selectable> columns) {
        return throwUnsupported();
    }

    @Override
    default Table updateView(Collection<? extends Selectable> columns) {
        return throwUnsupported();
    }

    @Override
    default Table update(Collection<? extends Selectable> columns) {
        return throwUnsupported();
    }

    @Override
    default Table lazyUpdate(Collection<? extends Selectable> columns) {
        return throwUnsupported();
    }

    @Override
    default Table select(Collection<? extends Selectable> columns) {
        return throwUnsupported();
    }

    @Override
    default Table aggAllBy(AggSpec spec, ColumnName... groupByColumns) {
        return throwUnsupported();
    }

    @Override
    default Table aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty, Table initialGroups,
            Collection<? extends ColumnName> groupByColumns) {
        return throwUnsupported();
    }

    @Override
    default Table updateBy(UpdateByControl control, Collection<? extends UpdateByOperation> operations,
            Collection<? extends ColumnName> byColumns) {
        return throwUnsupported();
    }

    @Override
    default Table selectDistinct(Collection<? extends Selectable> columns) {
        return throwUnsupported();
    }

    @Override
    default Table ungroup(boolean nullFill, Collection<? extends ColumnName> columnsToUngroup) {
        return throwUnsupported();
    }

    @Override
    default Table restrictSortTo(@NotNull String... allowedSortingColumns) {
        return throwUnsupported();
    }

    @Override
    default Table clearSortingRestrictions() {
        return throwUnsupported();
    }

    @Override
    default Table withDescription(@NotNull String description) {
        return throwUnsupported();
    }

    @Override
    default Table withColumnDescription(@NotNull String column, @NotNull String description) {
        return throwUnsupported();
    }

    @Override
    default Table withColumnDescriptions(@NotNull Map<String, String> descriptions) {
        return throwUnsupported();
    }

    @Override
    default Table setLayoutHints(@NotNull String hints) {
        return throwUnsupported();
    }
}

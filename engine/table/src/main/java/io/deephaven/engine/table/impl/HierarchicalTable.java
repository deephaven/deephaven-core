package io.deephaven.engine.table.impl;

import io.deephaven.api.JoinMatch;
import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.filter.Filter;
import io.deephaven.engine.rowset.TrackingRowSet;
import io.deephaven.engine.table.MatchPair;
import io.deephaven.engine.table.Table;
import io.deephaven.engine.table.impl.perf.QueryPerformanceRecorder;
import io.deephaven.engine.table.impl.select.SelectColumnFactory;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;
import java.util.Collection;

/**
 * This class is an extension of QueryTable that overrides many methods from {@link Table} which are not valid to
 * perform on Hierarchical tables (treeTables() and rollups()).
 */
public class HierarchicalTable extends QueryTable {
    private final QueryTable rootTable;
    private final HierarchicalTableInfo info;

    private HierarchicalTable(@NotNull QueryTable rootTable, @NotNull HierarchicalTableInfo info) {
        super(rootTable.getDefinition(), rootTable.getRowSet(), rootTable.getColumnSourceMap());
        this.rootTable = rootTable;
        this.info = info;
        setAttribute(Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE, info);
    }

    /**
     * Get the {@link HierarchicalTableInfo} associated with this table.
     *
     * @return the info for this table
     */
    public HierarchicalTableInfo getInfo() {
        return info;
    }

    /**
     * Get the table on which this hierarchical table was created from.
     *
     * @return the source table
     */
    public Table getSourceTable() {
        return (Table) getAttribute(Table.HIERARCHICAL_SOURCE_TABLE_ATTRIBUTE);
    }

    @Override
    public Table copy(boolean copyAttributes) {
        return QueryPerformanceRecorder.withNugget("hierarchicalTable-copy()", sizeForInstrumentation(), () -> {
            final HierarchicalTable result = createFrom((QueryTable) rootTable.copy(), info);
            copyAttributes(result, a -> true);
            return result;
        });
    }

    /**
     * Get the table that is the root of the hierarchy
     *
     * @return the root of the hierarchy
     */
    public Table getRawRootTable() {
        return rootTable;
    }

    @Override
    public Table formatColumns(String... columnFormats) {
        final HierarchicalTableInfo hierarchicalTableInfo =
                (HierarchicalTableInfo) getAttribute(HIERARCHICAL_SOURCE_INFO_ATTRIBUTE);
        final String[] originalColumnFormats = hierarchicalTableInfo.getColumnFormats();

        final String[] newColumnFormats;
        if (originalColumnFormats != null && originalColumnFormats.length > 0) {
            newColumnFormats =
                    Arrays.copyOf(originalColumnFormats, originalColumnFormats.length + columnFormats.length);
            System.arraycopy(columnFormats, 0, newColumnFormats, originalColumnFormats.length, columnFormats.length);
        } else {
            newColumnFormats = columnFormats;
        }

        // Note that we are not updating the root with the 'newColumnFormats' because the original set of formats
        // are already there.
        final Table updatedRoot = rootTable.updateView(SelectColumnFactory.getFormatExpressions(columnFormats));
        final ReverseLookup maybeRll = (ReverseLookup) rootTable.getAttribute(REVERSE_LOOKUP_ATTRIBUTE);

        // Explicitly need to copy this in case we are a rollup, in which case the RLL needs to be at root level
        if (maybeRll != null) {
            updatedRoot.setAttribute(REVERSE_LOOKUP_ATTRIBUTE, maybeRll);
        }

        final HierarchicalTable result =
                createFrom((QueryTable) updatedRoot, hierarchicalTableInfo.withColumnFormats(newColumnFormats));
        copyAttributes(result, a -> !Table.HIERARCHICAL_SOURCE_INFO_ATTRIBUTE.equals(a));

        return result;
    }

    @Override
    public Table slice(long firstRowInclusive, long lastRowExclusive) {
        return throwUnsupported("pct()");
    }

    @Override
    public Table head(long size) {
        return throwUnsupported("head()");
    }

    @Override
    public Table tail(long size) {
        return throwUnsupported("tail()");
    }

    @Override
    public Table leftJoin(Table table, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return throwUnsupported("leftJoin()");
    }

    @Override
    public Table exactJoin(Table table, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return throwUnsupported("exactJoin()");
    }

    @Override
    public Table dropColumns(String... columnNames) {
        return throwUnsupported("dropColumns()");
    }

    @Override
    public Table renameColumns(MatchPair... pairs) {
        return throwUnsupported("renameColumns()");
    }

    @Override
    public Table aj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
            AsOfMatchRule asOfMatchRule) {
        return throwUnsupported("aj()");
    }

    @Override
    public Table raj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
            AsOfMatchRule asOfMatchRule) {
        return throwUnsupported("raj()");
    }

    @Override
    public Table naturalJoin(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return throwUnsupported("naturalJoin()");
    }

    @Override
    public Table join(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd,
            int numRightBitsToReserve) {
        return throwUnsupported("join()");
    }

    @Override
    public Table lastBy(Selectable... groupByColumns) {
        return throwUnsupported("lastBy()");
    }

    @Override
    public Table firstBy(Selectable... groupByColumns) {
        return throwUnsupported("firstBy()");
    }

    @Override
    public Table minBy(Selectable... groupByColumns) {
        return throwUnsupported("minBy()");
    }

    @Override
    public Table maxBy(Selectable... groupByColumns) {
        return throwUnsupported("maxBy()");
    }

    @Override
    public Table medianBy(Selectable... groupByColumns) {
        return throwUnsupported("medianBy()");
    }

    @Override
    public Table countBy(String countColumnName, Selectable... groupByColumns) {
        return throwUnsupported("countBy()");
    }

    @Override
    public Table ungroup(boolean nullFill, String... columnsToUngroup) {
        return throwUnsupported("ungroup()");
    }

    @Override
    public Table headPct(double percent) {
        return throwUnsupported("headPct()");
    }

    @Override
    public Table tailPct(double percent) {
        return throwUnsupported("tailPct()");
    }

    @Override
    public Table groupBy(Collection<? extends Selectable> groupByColumns) {
        return throwUnsupported("groupBy()");
    }

    @Override
    public Table aggBy(Collection<? extends Aggregation> aggregations,
            Collection<? extends Selectable> groupByColumns) {
        return throwUnsupported("aggBy()");
    }

    @Override
    public Table headBy(long nRows, String... groupByColumns) {
        return throwUnsupported("headBy()");
    }

    @Override
    public Table tailBy(long nRows, String... groupByColumns) {
        return throwUnsupported("tailBy()");
    }

    @Override
    public Table applyToAllBy(String formulaColumn, String columnParamName,
            Collection<? extends Selectable> groupByColumns) {
        return throwUnsupported("applyToAllBy()");
    }

    @Override
    public Table sumBy(Selectable... groupByColumns) {
        return throwUnsupported("sumBy()");
    }

    @Override
    public Table absSumBy(Selectable... groupByColumns) {
        return throwUnsupported("absSumBy()");
    }

    @Override
    public Table avgBy(Selectable... groupByColumns) {
        return throwUnsupported("avgBy()");
    }

    @Override
    public Table wavgBy(String weightColumn, Selectable... groupByColumns) {
        return throwUnsupported("wavgBy()");
    }

    @Override
    public Table stdBy(Selectable... groupByColumns) {
        return throwUnsupported("stdBy()");
    }

    @Override
    public Table varBy(Selectable... groupByColumns) {
        return throwUnsupported("varBy()");
    }

    @Override
    public Table where(Collection<? extends Filter> filters) {
        return throwUnsupported("where()");
    }

    @Override
    public Table whereIn(Table rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return throwUnsupported("whereIn()");
    }

    @Override
    public Table whereNotIn(Table rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return throwUnsupported("whereIn()");
    }

    @Override
    public Table select(Collection<? extends Selectable> selectColumns) {
        return throwUnsupported("select()");
    }

    @Override
    public Table selectDistinct(Collection<? extends Selectable> columns) {
        return throwUnsupported("selectDistinct()");
    }

    @Override
    public Table update(Collection<? extends Selectable> columns) {
        return throwUnsupported("update()");
    }

    @Override
    public Table view(Collection<? extends Selectable> columns) {
        return throwUnsupported("view()");
    }

    @Override
    public Table updateView(Collection<? extends Selectable> columns) {
        return throwUnsupported("updateView()");
    }

    @Override
    public Table lazyUpdate(Collection<? extends Selectable> columns) {
        return throwUnsupported("lazyUpdate()");
    }

    @Override
    public Table flatten() {
        return throwUnsupported("flatten()");
    }

    @Override
    public LocalTableMap partitionBy(boolean dropKeys, String... keyColumnNames) {
        return throwUnsupported("partitionBy()");
    }

    @Override
    public Table rollup(Collection<? extends Aggregation> aggregations, boolean includeConstituents,
            Selectable... columns) {
        return throwUnsupported("rollup()");
    }

    @Override
    public Table treeTable(String idColumn, String parentColumn) {
        return throwUnsupported("treeTable()");
    }

    @Override
    public Table sort(Collection<SortColumn> columnsToSortBy) {
        return throwUnsupported("sort()");
    }

    @Override
    public Table reverse() {
        return throwUnsupported("reverse()");
    }

    @Override
    public Table snapshot(Table baseTable, boolean doInitialSnapshot, String... stampColumns) {
        return throwUnsupported("snapshot()");
    }

    @Override
    public Table snapshotIncremental(Table rightTable, boolean doInitialSnapshot, String... stampColumns) {
        return throwUnsupported("snapshotIncremental()");
    }

    @Override
    public Table snapshotHistory(Table rightTable) {
        return throwUnsupported("snapshotHistory()");
    }

    @Override
    public QueryTable getSubTable(TrackingRowSet rowSet) {
        return throwUnsupported("getSubTable()");
    }

    private <T> T throwUnsupported(String opName) {
        throw new UnsupportedOperationException("Operation " + opName
                + " may not be performed on hierarchical tables. Instead, apply it to table before treeTable() or rollup()");
    }

    /**
     * Create a HierarchicalTable from the specified root (top level) table and {@link HierarchicalTableInfo info} that
     * describes the hierarchy type.
     *
     * @param rootTable the root table of the hierarchy
     * @param info the info that describes the hierarchy type
     *
     * @return A new Hierarchical table. The table itself is a view of the root of the hierarchy.
     */
    static @NotNull HierarchicalTable createFrom(@NotNull QueryTable rootTable, @NotNull HierarchicalTableInfo info) {
        final Mutable<HierarchicalTable> resultHolder = new MutableObject<>();

        // Create a copy of the root partitionBy table as a HierarchicalTable, and wire it up for listeners.
        final SwapListener swapListener =
                rootTable.createSwapListenerIfRefreshing(SwapListener::new);
        rootTable.initializeWithSnapshot("-hierarchicalTable", swapListener, (usePrev, beforeClockValue) -> {
            final HierarchicalTable table = new HierarchicalTable(rootTable, info);
            rootTable.copyAttributes(table, a -> true);

            if (swapListener != null) {
                final ListenerImpl listener =
                        new ListenerImpl("hierarchicalTable()", rootTable, table);
                swapListener.setListenerAndResult(listener, table);
                table.addParentReference(swapListener);
            }

            resultHolder.setValue(table);
            return true;
        });

        return resultHolder.getValue();
    }
}

package io.deephaven.db.v2;

import io.deephaven.db.tables.SortPair;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.select.SelectColumnFactory;
import io.deephaven.db.tables.utils.QueryPerformanceRecorder;
import io.deephaven.db.v2.by.AggregationStateFactory;
import io.deephaven.db.v2.by.ComboAggregateFactory;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.db.v2.select.SelectFilter;
import io.deephaven.db.v2.utils.Index;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
import org.jetbrains.annotations.NotNull;

import java.util.Arrays;

/**
 * This class is an extension of QueryTable that overrides many methods from {@link Table} which are not valid to perform
 * on Hierarchical tables (treeTables() and rollups()).
 */
public class HierarchicalTable extends QueryTable {
    private final QueryTable rootTable;
    private final HierarchicalTableInfo info;

    private HierarchicalTable(@NotNull QueryTable rootTable, @NotNull HierarchicalTableInfo info) {
        super(rootTable.getDefinition(), rootTable.getIndex(), rootTable.getColumnSourceMap());
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
        return (Table)getAttribute(Table.HIERARCHICAL_SOURCE_TABLE_ATTRIBUTE);
    }

    @Override
    public Table copy(boolean copyAttributes) {
        return QueryPerformanceRecorder.withNugget("hierarchicalTable-copy()", sizeForInstrumentation(), () -> {
            final HierarchicalTable result = createFrom((QueryTable)rootTable.copy(), info);
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
        final HierarchicalTableInfo hierarchicalTableInfo = (HierarchicalTableInfo) getAttribute(HIERARCHICAL_SOURCE_INFO_ATTRIBUTE);
        final String[] originalColumnFormats = hierarchicalTableInfo.getColumnFormats();

        final String[] newColumnFormats;
        if(originalColumnFormats != null && originalColumnFormats.length > 0) {
            newColumnFormats = Arrays.copyOf(originalColumnFormats, originalColumnFormats.length + columnFormats.length);
            System.arraycopy(columnFormats, 0, newColumnFormats, originalColumnFormats.length, columnFormats.length);
        } else {
            newColumnFormats = columnFormats;
        }

        // Note that we are not updating the root with the 'newColumnFormats' because the original set of formats
        // are already there.
        final Table updatedRoot = rootTable.updateView(SelectColumnFactory.getFormatExpressions(columnFormats));
        final ReverseLookup maybeRll = (ReverseLookup)rootTable.getAttribute(REVERSE_LOOKUP_ATTRIBUTE);

        // Explicitly need to copy this in case we are a rollup, in which case the RLL needs to be at root level
        if(maybeRll != null) {
            updatedRoot.setAttribute(REVERSE_LOOKUP_ATTRIBUTE, maybeRll);
        }

        final HierarchicalTable result = createFrom((QueryTable)updatedRoot, hierarchicalTableInfo.withColumnFormats(newColumnFormats));
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
    public Table aj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, AsOfMatchRule asOfMatchRule) {
        return throwUnsupported("aj()");
    }

    @Override
    public Table raj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, AsOfMatchRule asOfMatchRule) {
        return throwUnsupported("raj()");
    }

    @Override
    public Table naturalJoin(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return throwUnsupported("naturalJoin()");
    }

    @Override
    public Table join(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, int numRightBitsToReserve) {
        return throwUnsupported("join()");
    }

    @Override
    public Table lastBy(SelectColumn... groupByColumns) {
        return throwUnsupported("lastBy()");
    }

    @Override
    public Table firstBy(SelectColumn... groupByColumns) {
        return throwUnsupported("firstBy()");
    }

    @Override
    public Table minBy(SelectColumn[] selectColumns) {
        return throwUnsupported("minBy()");
    }

    @Override
    public Table maxBy(SelectColumn[] selectColumns) {
        return throwUnsupported("maxBy()");
    }

    @Override
    public Table medianBy(SelectColumn[] selectColumns) {
        return throwUnsupported("medianBy()");
    }

    @Override
    public Table countBy(String countColumnName, SelectColumn... groupByColumns) {
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
    public Table by(@SuppressWarnings("rawtypes") AggregationStateFactory aggregationStateFactory, SelectColumn... groupByColumns) {
        return throwUnsupported("by()");
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
    public Table applyToAllBy(String formulaColumn, String columnParamName, SelectColumn... groupByColumns) {
        return throwUnsupported("applyToAllBy()");
    }

    @Override
    public Table sumBy(SelectColumn... groupByColumns) {
        return throwUnsupported("sumBy()");
    }

    @Override
    public Table absSumBy(SelectColumn... groupByColumns) {
        return throwUnsupported("absSumBy()");
    }

    @Override
    public Table avgBy(SelectColumn... groupByColumns) {
        return throwUnsupported("avgBy()");
    }

    @Override
    public Table wavgBy(String weightColumn, SelectColumn... groupByColumns) {
        return throwUnsupported("wavgBy()");
    }

    @Override
    public Table stdBy(SelectColumn... groupByColumns) {
        return throwUnsupported("stdBy()");
    }

    @Override
    public Table varBy(SelectColumn... groupByColumns) {
        return throwUnsupported("varBy()");
    }

    @Override
    public Table where(SelectFilter... filters) {
        return throwUnsupported("where()");
    }

    @Override
    public Table whereIn(GroupStrategy groupStrategy, Table rightTable, boolean inclusion, MatchPair... columnsToMatch) {
        return throwUnsupported("whereIn()");
    }

    @Override
    public Table select(SelectColumn... selectColumns) {
        return throwUnsupported("select()");
    }

    @Override
    public Table selectDistinct(SelectColumn... columns) {
        return throwUnsupported("selectDistinct()");
    }

    @Override
    public Table update(SelectColumn... columns) {
        return throwUnsupported("update()");
    }

    @Override
    public Table view(SelectColumn... columns) {
        return throwUnsupported("view()");
    }

    @Override
    public Table updateView(SelectColumn... columns) {
        return throwUnsupported("updateView()");
    }

    @Override
    public Table lazyUpdate(SelectColumn... columns) {
        return throwUnsupported("lazyUpdate()");
    }

    @Override
    public Table flatten() {
        return throwUnsupported("flatten()");
    }

    @SuppressWarnings("deprecation")
    @Override
    public void addColumnGrouping(String columnName) {
        throwUnsupported("addColumnGrouping()");
    }

    @Override
    public LocalTableMap byExternal(boolean dropKeys, String... keyColumnNames) {
        return throwUnsupported("byExternal()");
    }

    @Override
    public Table rollup(ComboAggregateFactory comboAggregateFactory, SelectColumn... columns) {
        return throwUnsupported("rollup()");
    }

    @Override
    public Table treeTable(String idColumn, String parentColumn) {
        return throwUnsupported("treeTable()");
    }

    @Override
    public Table sort(SortPair... columnsToSortBy) {
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
    public QueryTable getSubTable(Index index) {
        return throwUnsupported("getSubTable()");
    }

    private <T> T throwUnsupported(String opName) {
        throw new UnsupportedOperationException("Operation " + opName + " may not be performed on hierarchical tables. Instead, apply it to table before treeTable() or rollup()");
    }

    /**
     * Create a HierarchicalTable from the specified root (top level) table and {@link HierarchicalTableInfo info}
     * that describes the hierarchy type.
     *
     * @param rootTable the root table of the hierarchy
     * @param info the info that describes the hierarchy type
     *
     * @return A new Hierarchical table. The table itself is a view of the root of the hierarchy.
     */
    static @NotNull HierarchicalTable createFrom(@NotNull QueryTable rootTable, @NotNull HierarchicalTableInfo info) {
        final Mutable<HierarchicalTable> resultHolder = new MutableObject<>();

        // Create a copy of the root byExternal table as a HierarchicalTable, and wire it up for listeners.
        final ShiftAwareSwapListener swapListener = rootTable.createSwapListenerIfRefreshing(ShiftAwareSwapListener::new);
        rootTable.initializeWithSnapshot("-hierarchicalTable", swapListener, (usePrev, beforeClockValue) -> {
            final HierarchicalTable table = new HierarchicalTable(rootTable, info);
            rootTable.copyAttributes(table, a -> true);

            if(swapListener != null) {
                final ShiftAwareListenerImpl listener = new ShiftAwareListenerImpl("hierarchicalTable()", rootTable, table);
                swapListener.setListenerAndResult(listener, table);
                table.addParentReference(swapListener);
            }

            resultHolder.setValue(table);
            return true;
        });

        return resultHolder.getValue();
    }
}

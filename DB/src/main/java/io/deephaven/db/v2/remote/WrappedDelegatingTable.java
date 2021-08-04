package io.deephaven.db.v2.remote;

import io.deephaven.db.tables.DataColumn;
import io.deephaven.db.tables.SelectValidationResult;
import io.deephaven.db.tables.SortPair;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.select.WouldMatchPair;
import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.TableMap;
import io.deephaven.db.v2.by.AggregationStateFactory;
import io.deephaven.db.v2.by.ComboAggregateFactory;
import io.deephaven.db.v2.select.SelectColumn;
import io.deephaven.db.v2.select.SelectFilter;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.utils.Index;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;

public abstract class WrappedDelegatingTable extends BaseTable {

    /**
     * Marks a {@link io.deephaven.base.Function.Unary#call(Object)} method as opting out of being re-wrapped, as
     * a WrappedDelegatingTable would normally do.
     */
    @Target(ElementType.METHOD)
    @Retention(RetentionPolicy.RUNTIME)
    public static @interface DoNotWrap {
    }

    protected final Table parent;
    Function<Table, Table> wrapTable;

    public WrappedDelegatingTable(final Table parent, final String wrapperSuffix, final Function<Table, Table> wrapTable) {
        super(parent.getDefinition(), parent.getDescription() + "-" + wrapperSuffix);
        this.parent = parent;
        this.wrapTable = wrapTable;
    }

    @Override
    public <R> R apply(io.deephaven.base.Function.Unary<R, Table> function) {
        final R result = parent.apply(function);
        if (result instanceof Table) {
            try {
                if (function.getClass().getMethod("call", Object.class).getAnnotation(DoNotWrap.class) != null) {
                    return result;
                }
            } catch (final NoSuchMethodException e) {
                // Function.Unary no longer has a call() method?
                throw new IllegalStateException("Function.Unary.call() method is missing?", e);
            }
            // We can't reflectively check if R is Table or is some unexpected subclass that doesn't match what wrapTable
            // returns, so we just have to "cast to R" and let the calling code potentially fail with a ClassCastException.
            return (R) wrapTable.apply((Table) result);
        }
        return result;
    }

    @Override
    public ColumnSource getColumnSource(String sourceName) {
        return parent.getColumnSource(sourceName);
    }

    @Override
    public Map<String, ? extends ColumnSource> getColumnSourceMap() {
        return parent.getColumnSourceMap();
    }

    @Override
    public Collection<? extends ColumnSource> getColumnSources() {
        return parent.getColumnSources();
    }

    @Override
    public DataColumn getColumn(String columnName) {
        return parent.getColumn(columnName);
    }

    @Override
    public Object[] getRecord(long rowNo, String... columnNames) {
        return parent.getRecord(rowNo, columnNames);
    }

    @Override
    public Table where(SelectFilter... filters) {
        return wrapTable.apply(parent.where(filters));
    }

    @Override
    public Table wouldMatch(WouldMatchPair... matchers) {
        return wrapTable.apply(parent.wouldMatch(matchers));
    }

    @Override
    public Table whereIn(GroupStrategy groupStrategy, Table rightTable, boolean inclusion, MatchPair... columnsToMatch) {
        return wrapTable.apply(parent.whereIn(groupStrategy, rightTable, inclusion, columnsToMatch));
    }

    @Override
    public Table getSubTable(Index index) {
        return wrapTable.apply(parent.getSubTable(index));
    }

    @Override
    public Table select(SelectColumn... columns) {
        return wrapTable.apply(parent.select(columns));
    }

    @Override
    public Table selectDistinct(SelectColumn... columns) {
        return wrapTable.apply(parent.selectDistinct(columns));
    }

    @Override
    public Table update(SelectColumn... newColumns) {
        return wrapTable.apply(parent.update(newColumns));
    }

    @Override
    public Table lazyUpdate(SelectColumn... newColumns) {
        return wrapTable.apply(parent.lazyUpdate(newColumns));
    }

    @Override
    public Table view(SelectColumn... columns) {
        return wrapTable.apply(parent.view(columns));
    }

    @Override
    public Table updateView(SelectColumn... newColumns) {
        return wrapTable.apply(parent.updateView(newColumns));
    }

    @Override
    public Table dropColumns(String... columnNames) {
        return wrapTable.apply(parent.dropColumns(columnNames));
    }

    @Override
    public Table renameColumns(MatchPair... pairs) {
        return wrapTable.apply(parent.renameColumns(pairs));
    }

    @Override
    public Table head(long size) {
        return wrapTable.apply(parent.head(size));
    }

    @Override
    public Table tail(long size) {
        return wrapTable.apply(parent.tail(size));
    }

    @Override
    public Table slice(long firstPositionInclusive, long lastPositionExclusive) {
        return wrapTable.apply(parent.slice(firstPositionInclusive, lastPositionExclusive));
    }

    @Override
    public Table headPct(double percent) {
        return wrapTable.apply(parent.headPct(percent));
    }

    @Override
    public Table tailPct(double percent) {
        return wrapTable.apply(parent.tailPct(percent));
    }

    @Override
    public Table leftJoin(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return wrapTable.apply(parent.leftJoin(rightTable, columnsToMatch, columnsToAdd));
    }

    @Override
    public Table exactJoin(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return wrapTable.apply(parent.exactJoin(rightTable, columnsToMatch, columnsToAdd));
    }

    @Override
    public Table aj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, AsOfMatchRule asOfMatchRule) {
        return wrapTable.apply(parent.aj(rightTable, columnsToMatch, columnsToAdd, asOfMatchRule));
    }

    @Override
    public Table raj(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, AsOfMatchRule asOfMatchRule) {
        return wrapTable.apply(parent.raj(rightTable, columnsToMatch, columnsToAdd, asOfMatchRule));
    }

    @Override
    public Table naturalJoin(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd) {
        return wrapTable.apply(parent.naturalJoin(rightTable, columnsToMatch, columnsToAdd));
    }

    @Override
    public Table join(Table rightTable, MatchPair[] columnsToMatch, MatchPair[] columnsToAdd, int numRightBitsToReserve) {
        return wrapTable.apply(parent.join(rightTable, columnsToMatch, columnsToAdd, numRightBitsToReserve));
    }

    @Override
    public Table by(AggregationStateFactory aggregationStateFactory, SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.by(aggregationStateFactory, groupByColumns));
    }

    @Override
    public Table headBy(long nRows, String... groupByColumns) {
        return wrapTable.apply(parent.headBy(nRows, groupByColumns));
    }

    @Override
    public Table tailBy(long nRows, String... groupByColumns) {
        return wrapTable.apply(parent.tailBy(nRows, groupByColumns));
    }

    @Override
    public Table applyToAllBy(String formulaColumn, String columnParamName, SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.applyToAllBy(formulaColumn, columnParamName, groupByColumns));
    }

    @Override
    public Table sumBy(SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.sumBy(groupByColumns));
    }

    @Override
    public Table absSumBy(SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.absSumBy(groupByColumns));
    }

    @Override
    public Table avgBy(SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.avgBy(groupByColumns));
    }

    @Override
    public Table wavgBy(String weightColumn, SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.wavgBy(weightColumn, groupByColumns));
    }

    @Override
    public Table wsumBy(String weightColumn, SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.wsumBy(weightColumn, groupByColumns));
    }

    @Override
    public Table stdBy(SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.stdBy(groupByColumns));
    }

    @Override
    public Table varBy(SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.varBy(groupByColumns));
    }

    @Override
    public Table lastBy(SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.lastBy(groupByColumns));
    }

    @Override
    public Table firstBy(SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.firstBy(groupByColumns));
    }

    @Override
    public Table minBy(SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.minBy(groupByColumns));
    }

    @Override
    public Table maxBy(SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.maxBy(groupByColumns));
    }

    @Override
    public Table medianBy(SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.medianBy(groupByColumns));
    }

    @Override
    public Table countBy(String countColumnName, SelectColumn... groupByColumns) {
        return wrapTable.apply(parent.countBy(countColumnName, groupByColumns));
    }

    @Override
    public Table ungroup(boolean nullFill, String... columnsToUngroup) {
        return wrapTable.apply(parent.ungroup(nullFill, columnsToUngroup));
    }

    @Override
    public TableMap byExternal(boolean dropKeys, String... keyColumnNames) {
        return parent.byExternal(dropKeys, keyColumnNames).transformTables(wrapTable);
    }

    @Override
    public Table rollup(ComboAggregateFactory comboAggregateFactory, boolean includeConstituents, SelectColumn... columns) {
        return wrapTable.apply(parent.rollup(comboAggregateFactory, includeConstituents, columns));
    }

    @Override
    public Table treeTable(String idColumn, String parentColumn) {
        return wrapTable.apply(parent.treeTable(idColumn, parentColumn));
    }

    @Override
    public Table sort(SortPair... sortPairs) {
        return wrapTable.apply(parent.sort(sortPairs));
    }

    @Override
    public Table reverse() {
        return wrapTable.apply(parent.reverse());
    }

    @Override
    public Table snapshot(Table baseTable, boolean doInitialSnapshot, String... stampColumns) {
        return wrapTable.apply(parent.snapshot(baseTable, doInitialSnapshot, stampColumns));
    }

    @Override
    public Table snapshotIncremental(Table rightTable, boolean doInitialSnapshot, String... stampColumns) {
        return wrapTable.apply(parent.snapshotIncremental(rightTable, doInitialSnapshot, stampColumns));
    }

    @Override
    public Table snapshotHistory(Table rightTable) {
        return wrapTable.apply(parent.snapshotHistory(rightTable));
    }

    @Override
    public Table flatten() {
        return wrapTable.apply(parent.flatten());
    }


    @Override
    public SelectValidationResult validateSelect(SelectColumn... columns) {
       return parent.validateSelect(columns);
    }

}

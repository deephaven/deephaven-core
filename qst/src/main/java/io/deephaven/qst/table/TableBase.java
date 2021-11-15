package io.deephaven.qst.table;

import io.deephaven.api.AsOfJoinRule;
import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.RawString;
import io.deephaven.api.ReverseAsOfJoinRule;
import io.deephaven.api.Selectable;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.filter.Filter;
import io.deephaven.qst.TableCreationLogic;
import io.deephaven.qst.table.TableSchema.Visitor;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public abstract class TableBase implements TableSpec {

    // Note: method implementations should use the static constructors, or builder patterns, for the
    // necessary TableSpec instead of delegating to other methods. The default values are the
    // responsibility of the TableSpec.


    @Override
    public final TableCreationLogic logic() {
        return new TableCreationLogicImpl(this);
    }

    @Override
    public final HeadTable head(long size) {
        return HeadTable.of(this, size);
    }

    @Override
    public final TailTable tail(long size) {
        return TailTable.of(this, size);
    }

    @Override
    public final ReverseTable reverse() {
        return ReverseTable.of(this);
    }

    @Override
    public final SnapshotTable snapshot(TableSpec baseTable, String... stampColumns) {
        SnapshotTable.Builder builder = SnapshotTable.builder().trigger(this).base(baseTable);
        for (String stampColumn : stampColumns) {
            builder.addStampColumns(ColumnName.of(stampColumn));
        }
        return builder.build();
    }

    @Override
    public final SnapshotTable snapshot(TableSpec baseTable, boolean doInitialSnapshot,
            String... stampColumns) {
        SnapshotTable.Builder builder = SnapshotTable.builder().trigger(this).base(baseTable)
                .doInitialSnapshot(doInitialSnapshot);
        for (String stampColumn : stampColumns) {
            builder.addStampColumns(ColumnName.of(stampColumn));
        }
        return builder.build();
    }

    @Override
    public final SnapshotTable snapshot(TableSpec baseTable, boolean doInitialSnapshot,
            Collection<ColumnName> stampColumns) {
        return SnapshotTable.builder().trigger(this).base(baseTable)
                .doInitialSnapshot(doInitialSnapshot).addAllStampColumns(stampColumns).build();
    }

    @Override
    public final SortTable sort(String... columnsToSortBy) {
        SortTable.Builder builder = SortTable.builder().parent(this);
        for (String column : columnsToSortBy) {
            builder.addColumns(ColumnName.of(column).asc());
        }
        return builder.build();
    }

    @Override
    public final SortTable sortDescending(String... columnsToSortBy) {
        SortTable.Builder builder = SortTable.builder().parent(this);
        for (String column : columnsToSortBy) {
            builder.addColumns(ColumnName.of(column).desc());
        }
        return builder.build();
    }

    @Override
    public final SortTable sort(Collection<SortColumn> columnsToSortBy) {
        return SortTable.builder().parent(this).addAllColumns(columnsToSortBy).build();
    }

    @Override
    public final WhereTable where(String... filters) {
        WhereTable.Builder builder = WhereTable.builder().parent(this);
        for (String filter : filters) {
            builder.addFilters(RawString.of(filter));
        }
        return builder.build();
    }

    @Override
    public final WhereTable where(Collection<? extends Filter> filters) {
        return WhereTable.builder().parent(this).addAllFilters(filters).build();
    }

    @Override
    public final WhereInTable whereIn(TableSpec rightTable, String... columnsToMatch) {
        WhereInTable.Builder builder = WhereInTable.builder().left(this).right(rightTable);
        for (String toMatch : columnsToMatch) {
            builder.addMatches(JoinMatch.parse(toMatch));
        }
        return builder.build();
    }

    @Override
    public final WhereInTable whereIn(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch) {
        return WhereInTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .build();
    }

    @Override
    public final WhereNotInTable whereNotIn(TableSpec rightTable, String... columnsToMatch) {
        WhereNotInTable.Builder builder = WhereNotInTable.builder().left(this).right(rightTable);
        for (String toMatch : columnsToMatch) {
            builder.addMatches(JoinMatch.parse(toMatch));
        }
        return builder.build();
    }

    @Override
    public final WhereNotInTable whereNotIn(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch) {
        return WhereNotInTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .build();
    }

    @Override
    public final NaturalJoinTable naturalJoin(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return NaturalJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final NaturalJoinTable naturalJoin(TableSpec rightTable, String columnsToMatch) {
        NaturalJoinTable.Builder builder = NaturalJoinTable.builder().left(this).right(rightTable);
        for (String match : split(columnsToMatch)) {
            builder.addMatches(JoinMatch.parse(match));
        }
        return builder.build();
    }

    @Override
    public final NaturalJoinTable naturalJoin(TableSpec rightTable, String columnsToMatch,
            String columnsToAdd) {
        NaturalJoinTable.Builder builder = NaturalJoinTable.builder().left(this).right(rightTable);
        for (String match : split(columnsToMatch)) {
            builder.addMatches(JoinMatch.parse(match));
        }
        for (String addition : split(columnsToAdd)) {
            builder.addAdditions(JoinAddition.parse(addition));
        }
        return builder.build();
    }

    @Override
    public final ExactJoinTable exactJoin(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return ExactJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final ExactJoinTable exactJoin(TableSpec rightTable, String columnsToMatch) {
        ExactJoinTable.Builder builder = ExactJoinTable.builder().left(this).right(rightTable);
        for (String match : split(columnsToMatch)) {
            builder.addMatches(JoinMatch.parse(match));
        }
        return builder.build();
    }

    @Override
    public final ExactJoinTable exactJoin(TableSpec rightTable, String columnsToMatch,
            String columnsToAdd) {
        ExactJoinTable.Builder builder = ExactJoinTable.builder().left(this).right(rightTable);
        for (String match : split(columnsToMatch)) {
            builder.addMatches(JoinMatch.parse(match));
        }
        for (String addition : split(columnsToAdd)) {
            builder.addAdditions(JoinAddition.parse(addition));
        }
        return builder.build();
    }

    @Override
    public JoinTable join(TableSpec rightTable, Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return JoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final JoinTable join(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, int reserveBits) {
        return JoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).reserveBits(reserveBits).build();
    }

    @Override
    public final JoinTable join(TableSpec rightTable, String columnsToMatch) {
        JoinTable.Builder builder = JoinTable.builder().left(this).right(rightTable);
        for (String match : split(columnsToMatch)) {
            builder.addMatches(JoinMatch.parse(match));
        }
        return builder.build();
    }

    @Override
    public final JoinTable join(TableSpec rightTable, String columnsToMatch, String columnsToAdd) {
        JoinTable.Builder builder = JoinTable.builder().left(this).right(rightTable);
        for (String match : split(columnsToMatch)) {
            builder.addMatches(JoinMatch.parse(match));
        }
        for (String addition : split(columnsToAdd)) {
            builder.addAdditions(JoinAddition.parse(addition));
        }
        return builder.build();
    }

    @Override
    public final LeftJoinTable leftJoin(TableSpec rightTable, String columnsToMatch) {
        LeftJoinTable.Builder builder = LeftJoinTable.builder().left(this).right(rightTable);
        for (String match : split(columnsToMatch)) {
            builder.addMatches(JoinMatch.parse(match));
        }
        return builder.build();
    }

    @Override
    public final LeftJoinTable leftJoin(TableSpec rightTable, String columnsToMatch,
            String columnsToAdd) {
        LeftJoinTable.Builder builder = LeftJoinTable.builder().left(this).right(rightTable);
        for (String match : split(columnsToMatch)) {
            builder.addMatches(JoinMatch.parse(match));
        }
        for (String addition : split(columnsToAdd)) {
            builder.addAdditions(JoinAddition.parse(addition));
        }
        return builder.build();
    }

    @Override
    public final LeftJoinTable leftJoin(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return LeftJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final AsOfJoinTable aj(TableSpec rightTable, String columnsToMatch) {
        AsOfJoinTable.Builder builder = AsOfJoinTable.builder().left(this).right(rightTable);
        for (String match : split(columnsToMatch)) {
            builder.addMatches(JoinMatch.parse(match));
        }
        return builder.build();
    }

    @Override
    public final AsOfJoinTable aj(TableSpec rightTable, String columnsToMatch,
            String columnsToAdd) {
        AsOfJoinTable.Builder builder = AsOfJoinTable.builder().left(this).right(rightTable);
        for (String match : split(columnsToMatch)) {
            builder.addMatches(JoinMatch.parse(match));
        }
        for (String addition : split(columnsToAdd)) {
            builder.addAdditions(JoinAddition.parse(addition));
        }
        return builder.build();
    }

    @Override
    public final AsOfJoinTable aj(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return AsOfJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final AsOfJoinTable aj(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule) {
        return AsOfJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
                .addAllAdditions(columnsToAdd).rule(asOfJoinRule).build();
    }

    @Override
    public final ReverseAsOfJoinTable raj(TableSpec rightTable, String columnsToMatch) {
        ReverseAsOfJoinTable.Builder builder =
                ReverseAsOfJoinTable.builder().left(this).right(rightTable);
        for (String match : split(columnsToMatch)) {
            builder.addMatches(JoinMatch.parse(match));
        }
        return builder.build();
    }

    @Override
    public final ReverseAsOfJoinTable raj(TableSpec rightTable, String columnsToMatch,
            String columnsToAdd) {
        ReverseAsOfJoinTable.Builder builder =
                ReverseAsOfJoinTable.builder().left(this).right(rightTable);
        for (String match : split(columnsToMatch)) {
            builder.addMatches(JoinMatch.parse(match));
        }
        for (String addition : split(columnsToAdd)) {
            builder.addAdditions(JoinAddition.parse(addition));
        }
        return builder.build();
    }

    @Override
    public final ReverseAsOfJoinTable raj(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd) {
        return ReverseAsOfJoinTable.builder().left(this).right(rightTable)
                .addAllMatches(columnsToMatch).addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final ReverseAsOfJoinTable raj(TableSpec rightTable,
            Collection<? extends JoinMatch> columnsToMatch,
            Collection<? extends JoinAddition> columnsToAdd, ReverseAsOfJoinRule reverseAsOfJoinRule) {
        return ReverseAsOfJoinTable.builder().left(this).right(rightTable)
                .addAllMatches(columnsToMatch).addAllAdditions(columnsToAdd).rule(reverseAsOfJoinRule)
                .build();
    }

    @Override
    public final ViewTable view(String... columns) {
        final ViewTable.Builder builder = ViewTable.builder().parent(this);
        for (String column : columns) {
            builder.addColumns(Selectable.parse(column));
        }
        return builder.build();
    }

    @Override
    public final ViewTable view(Collection<? extends Selectable> columns) {
        return ViewTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final UpdateViewTable updateView(String... columns) {
        final UpdateViewTable.Builder builder = UpdateViewTable.builder().parent(this);
        for (String column : columns) {
            builder.addColumns(Selectable.parse(column));
        }
        return builder.build();
    }

    @Override
    public final UpdateViewTable updateView(Collection<? extends Selectable> columns) {
        return UpdateViewTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final UpdateTable update(String... columns) {
        final UpdateTable.Builder builder = UpdateTable.builder().parent(this);
        for (String column : columns) {
            builder.addColumns(Selectable.parse(column));
        }
        return builder.build();
    }

    @Override
    public final UpdateTable update(Collection<? extends Selectable> columns) {
        return UpdateTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final SelectTable select(String... columns) {
        final SelectTable.Builder builder = SelectTable.builder().parent(this);
        for (String column : columns) {
            builder.addColumns(Selectable.parse(column));
        }
        return builder.build();
    }

    @Override
    public final SelectTable select(Collection<? extends Selectable> columns) {
        return SelectTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final ByTable by() {
        return ByTable.builder().parent(this).build();
    }

    @Override
    public final ByTable by(String... groupByColumns) {
        ByTable.Builder builder = ByTable.builder().parent(this);
        for (String groupByColumn : groupByColumns) {
            builder.addColumns(Selectable.parse(groupByColumn));
        }
        return builder.build();
    }

    @Override
    public final ByTable by(Collection<? extends Selectable> groupByColumns) {
        return ByTable.builder().parent(this).addAllColumns(groupByColumns).build();
    }

    @Override
    public final AggregationTable by(Collection<? extends Selectable> groupByColumns,
            Collection<? extends Aggregation> aggregations) {
        return AggregationTable.builder().parent(this).addAllColumns(groupByColumns)
                .addAllAggregations(aggregations).build();
    }

    @Override
    public final <V extends TableSchema.Visitor> V walk(V visitor) {
        visitor.visit(this);
        return visitor;
    }

    @Override
    public final String toString() {
        // the auto-generated toString methods aren't very useful; and being recursive, they can
        // cause stack overflow exceptions that hide other errors in unit tests
        return super.toString();
    }

    private static Collection<String> split(String string) {
        return string.trim().isEmpty() ? Collections.emptyList()
                : Arrays.stream(string.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                        .collect(Collectors.toList());
    }
}

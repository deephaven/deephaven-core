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

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public abstract class TableBase implements Table {

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
    public final SnapshotTable snapshot(Table rightTable, String... stampColumns) {
        return snapshot(rightTable, false, stampColumns);
    }

    @Override
    public final SnapshotTable snapshot(Table rightTable, boolean doInitialSnapshot,
        String... stampColumns) {
        SnapshotTable.Builder builder = SnapshotTable.builder().trigger(this).base(rightTable)
            .doInitialSnapshot(doInitialSnapshot);
        for (String stampColumn : stampColumns) {
            builder.addStampColumns(ColumnName.of(stampColumn));
        }
        return builder.build();
    }

    @Override
    public final SnapshotTable snapshot(Table rightTable, boolean doInitialSnapshot,
        Collection<ColumnName> stampColumns) {
        return SnapshotTable.builder().trigger(this).base(rightTable)
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
    public final WhereInTable whereIn(Table rightTable, String... columnsToMatch) {
        WhereInTable.Builder builder = WhereInTable.builder().left(this).right(rightTable);
        for (String toMatch : columnsToMatch) {
            builder.addMatches(JoinMatch.parse(toMatch));
        }
        return builder.build();
    }

    @Override
    public final WhereInTable whereIn(Table rightTable,
        Collection<? extends JoinMatch> columnsToMatch) {
        return WhereInTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
            .build();
    }

    @Override
    public final WhereNotInTable whereNotIn(Table rightTable, String... columnsToMatch) {
        WhereNotInTable.Builder builder = WhereNotInTable.builder().left(this).right(rightTable);
        for (String toMatch : columnsToMatch) {
            builder.addMatches(JoinMatch.parse(toMatch));
        }
        return builder.build();
    }

    @Override
    public final WhereNotInTable whereNotIn(Table rightTable,
        Collection<? extends JoinMatch> columnsToMatch) {
        return WhereNotInTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
            .build();
    }

    @Override
    public final NaturalJoinTable naturalJoin(Table rightTable,
        Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd) {
        return NaturalJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
            .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final NaturalJoinTable naturalJoin(Table rightTable, String columnsToMatch) {
        return naturalJoin(rightTable, JoinMatch.from(split(columnsToMatch)),
            Collections.emptyList());
    }

    @Override
    public final NaturalJoinTable naturalJoin(Table rightTable, String columnsToMatch,
        String columnsToAdd) {
        return naturalJoin(rightTable, JoinMatch.from(split(columnsToMatch)),
            JoinAddition.from(split(columnsToAdd)));
    }

    @Override
    public final ExactJoinTable exactJoin(Table rightTable,
        Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd) {
        return ExactJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
            .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final ExactJoinTable exactJoin(Table rightTable, String columnsToMatch) {
        return exactJoin(rightTable, JoinMatch.from(split(columnsToMatch)),
            Collections.emptyList());
    }

    @Override
    public final ExactJoinTable exactJoin(Table rightTable, String columnsToMatch,
        String columnsToAdd) {
        return exactJoin(rightTable, JoinMatch.from(split(columnsToMatch)),
            JoinAddition.from(split(columnsToAdd)));
    }

    @Override
    public JoinTable join(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd) {
        return JoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
            .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final JoinTable join(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd, int reserveBits) {
        return JoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
            .addAllAdditions(columnsToAdd).reserveBits(reserveBits).build();
    }

    @Override
    public final JoinTable join(Table rightTable, String columnsToMatch) {
        return join(rightTable, JoinMatch.from(split(columnsToMatch)), Collections.emptyList(),
            JoinTable.DEFAULT_RESERVE_BITS);
    }

    @Override
    public final JoinTable join(Table rightTable, String columnsToMatch, String columnsToAdd) {
        return join(rightTable, JoinMatch.from(split(columnsToMatch)),
            JoinAddition.from(split(columnsToAdd)), JoinTable.DEFAULT_RESERVE_BITS);
    }

    @Override
    public final LeftJoinTable leftJoin(Table rightTable, String columnsToMatch) {
        return leftJoin(rightTable, JoinMatch.from(split(columnsToMatch)), Collections.emptyList());
    }

    @Override
    public final LeftJoinTable leftJoin(Table rightTable, String columnsToMatch,
        String columnsToAdd) {
        return leftJoin(rightTable, JoinMatch.from(split(columnsToMatch)),
            JoinAddition.from(split(columnsToAdd)));
    }

    @Override
    public final LeftJoinTable leftJoin(Table rightTable,
        Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd) {
        return LeftJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
            .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final AsOfJoinTable aj(Table rightTable, String columnsToMatch) {
        return aj(rightTable, JoinMatch.from(split(columnsToMatch)), Collections.emptyList());
    }

    @Override
    public final AsOfJoinTable aj(Table rightTable, String columnsToMatch, String columnsToAdd) {
        return aj(rightTable, JoinMatch.from(split(columnsToMatch)),
            JoinAddition.from(split(columnsToAdd)));
    }

    @Override
    public final AsOfJoinTable aj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd) {
        return AsOfJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
            .addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final AsOfJoinTable aj(Table rightTable, Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd, AsOfJoinRule asOfJoinRule) {
        return AsOfJoinTable.builder().left(this).right(rightTable).addAllMatches(columnsToMatch)
            .addAllAdditions(columnsToAdd).rule(asOfJoinRule).build();
    }

    @Override
    public final ReverseAsOfJoinTable raj(Table rightTable, String columnsToMatch) {
        return raj(rightTable, JoinMatch.from(split(columnsToMatch)), Collections.emptyList());
    }

    @Override
    public final ReverseAsOfJoinTable raj(Table rightTable, String columnsToMatch,
        String columnsToAdd) {
        return raj(rightTable, JoinMatch.from(split(columnsToMatch)),
            JoinAddition.from(split(columnsToAdd)));
    }

    @Override
    public final ReverseAsOfJoinTable raj(Table rightTable,
        Collection<? extends JoinMatch> columnsToMatch,
        Collection<? extends JoinAddition> columnsToAdd) {
        return ReverseAsOfJoinTable.builder().left(this).right(rightTable)
            .addAllMatches(columnsToMatch).addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final ReverseAsOfJoinTable raj(Table rightTable,
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

    private static Collection<String> split(String string) {
        return string.trim().isEmpty() ? Collections.emptyList()
            : Arrays.stream(string.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }
}

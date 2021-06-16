package io.deephaven.qst.table;

import io.deephaven.qst.table.agg.Aggregation;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.stream.Collectors;

public abstract class TableBase implements Table {

    @Override
    public final Table toTable() {
        return this;
    }

    @Override
    public final HeadTable head(long size) {
        return ImmutableHeadTable.of(this, size);
    }

    @Override
    public final TailTable tail(long size) {
        return ImmutableTailTable.of(this, size);
    }

    @Override
    public final WhereTable where(String... filters) {
        ImmutableWhereTable.Builder builder = ImmutableWhereTable.builder().parent(this);
        for (String filter : filters) {
            builder.addFilters(Filter.parse(filter));
        }
        return builder.build();
    }

    @Override
    public final WhereTable where(Collection<String> filters) {
        ImmutableWhereTable.Builder builder = ImmutableWhereTable.builder().parent(this);
        for (String filter : filters) {
            builder.addFilters(Filter.parse(filter));
        }
        return builder.build();
    }

    @Override
    public final WhereTable where2(Collection<Filter> filters) {
        return ImmutableWhereTable.builder().parent(this).addAllFilters(filters).build();
    }

    @Override
    public final WhereInTable whereIn(Table rightTable, String... columnsToMatch) {
        ImmutableWhereInTable.Builder builder =
            ImmutableWhereInTable.builder().left(this).right(rightTable);
        for (String toMatch : columnsToMatch) {
            builder.addMatches(JoinMatch.parse(toMatch));
        }
        return builder.build();
    }

    @Override
    public final WhereInTable whereIn(Table rightTable, Collection<JoinMatch> columnsToMatch) {
        return ImmutableWhereInTable.builder().left(this).right(rightTable)
            .addAllMatches(columnsToMatch).build();
    }

    @Override
    public final WhereNotInTable whereNotIn(Table rightTable, String... columnsToMatch) {
        ImmutableWhereNotInTable.Builder builder =
            ImmutableWhereNotInTable.builder().left(this).right(rightTable);
        for (String toMatch : columnsToMatch) {
            builder.addMatches(JoinMatch.parse(toMatch));
        }
        return builder.build();
    }

    @Override
    public final WhereNotInTable whereNotIn(Table rightTable,
        Collection<JoinMatch> columnsToMatch) {
        return ImmutableWhereNotInTable.builder().left(this).right(rightTable)
            .addAllMatches(columnsToMatch).build();
    }

    @Override
    public final NaturalJoinTable naturalJoin2(Table rightTable,
        Collection<JoinMatch> columnsToMatch, Collection<JoinAddition> columnsToAdd) {
        return ImmutableNaturalJoinTable.builder().left(this).right(rightTable)
            .addAllMatches(columnsToMatch).addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final NaturalJoinTable naturalJoin(Table rightTable, Collection<String> columnsToMatch,
        Collection<String> columnsToAdd) {
        ImmutableNaturalJoinTable.Builder builder =
            ImmutableNaturalJoinTable.builder().left(this).right(rightTable);
        for (String toMatch : columnsToMatch) {
            builder.addMatches(JoinMatch.parse(toMatch));
        }
        for (String toAdd : columnsToAdd) {
            builder.addAdditions(JoinAddition.parse(toAdd));
        }
        return builder.build();
    }

    @Override
    public final NaturalJoinTable naturalJoin(Table rightTable, Collection<String> columnsToMatch) {
        return naturalJoin(rightTable, columnsToMatch, Collections.emptyList());
    }

    @Override
    public final NaturalJoinTable naturalJoin(Table rightTable, String columnsToMatch) {
        return naturalJoin(rightTable, split(columnsToMatch));
    }

    @Override
    public final NaturalJoinTable naturalJoin(Table rightTable, String columnsToMatch,
        String columnsToAdd) {
        return naturalJoin(rightTable, split(columnsToMatch), split(columnsToAdd));
    }

    @Override
    public final ExactJoinTable exactJoin2(Table rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd) {
        return ImmutableExactJoinTable.builder().left(this).right(rightTable)
            .addAllMatches(columnsToMatch).addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final ExactJoinTable exactJoin(Table rightTable, Collection<String> columnsToMatch,
        Collection<String> columnsToAdd) {
        ImmutableExactJoinTable.Builder builder =
            ImmutableExactJoinTable.builder().left(this).right(rightTable);
        for (String toMatch : columnsToMatch) {
            builder.addMatches(JoinMatch.parse(toMatch));
        }
        for (String toAdd : columnsToAdd) {
            builder.addAdditions(JoinAddition.parse(toAdd));
        }
        return builder.build();
    }

    @Override
    public final ExactJoinTable exactJoin(Table rightTable, Collection<String> columnsToMatch) {
        return exactJoin(rightTable, columnsToMatch, Collections.emptyList());
    }

    @Override
    public final ExactJoinTable exactJoin(Table rightTable, String columnsToMatch) {
        return exactJoin(rightTable, split(columnsToMatch));
    }

    @Override
    public final ExactJoinTable exactJoin(Table rightTable, String columnsToMatch,
        String columnsToAdd) {
        return exactJoin(rightTable, split(columnsToMatch), split(columnsToAdd));
    }

    @Override
    public final JoinTable join2(Table rightTable, Collection<JoinMatch> columnsToMatch,
        Collection<JoinAddition> columnsToAdd) {
        return ImmutableJoinTable.builder().left(this).right(rightTable)
            .addAllMatches(columnsToMatch).addAllAdditions(columnsToAdd).build();
    }

    @Override
    public final JoinTable join(Table rightTable, Collection<String> columnsToMatch,
        Collection<String> columnsToAdd) {
        ImmutableJoinTable.Builder builder =
            ImmutableJoinTable.builder().left(this).right(rightTable);
        for (String toMatch : columnsToMatch) {
            builder.addMatches(JoinMatch.parse(toMatch));
        }
        for (String toAdd : columnsToAdd) {
            builder.addAdditions(JoinAddition.parse(toAdd));
        }
        return builder.build();
    }

    @Override
    public final JoinTable join(Table rightTable, String columnsToMatch) {
        return join(rightTable, split(columnsToMatch), Collections.emptyList());
    }

    @Override
    public final JoinTable join(Table rightTable, String columnsToMatch, String columnsToAdd) {
        return join(rightTable, split(columnsToMatch), split(columnsToAdd));
    }

    @Override
    public final ViewTable view(String... columns) {
        final ImmutableViewTable.Builder builder = ImmutableViewTable.builder().parent(this);
        for (String column : columns) {
            builder.addColumns(Selectable.parse(column));
        }
        return builder.build();
    }

    @Override
    public final ViewTable view(Collection<String> columns) {
        final ImmutableViewTable.Builder builder = ImmutableViewTable.builder().parent(this);
        for (String column : columns) {
            builder.addColumns(Selectable.parse(column));
        }
        return builder.build();
    }

    @Override
    public final ViewTable view2(Collection<Selectable> columns) {
        return ImmutableViewTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final UpdateViewTable updateView(String... columns) {
        final ImmutableUpdateViewTable.Builder builder =
            ImmutableUpdateViewTable.builder().parent(this);
        for (String column : columns) {
            builder.addColumns(Selectable.parse(column));
        }
        return builder.build();
    }

    @Override
    public final UpdateViewTable updateView(Collection<String> columns) {
        final ImmutableUpdateViewTable.Builder builder =
            ImmutableUpdateViewTable.builder().parent(this);
        for (String column : columns) {
            builder.addColumns(Selectable.parse(column));
        }
        return builder.build();
    }

    @Override
    public final UpdateViewTable updateView2(Collection<Selectable> columns) {
        return ImmutableUpdateViewTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final UpdateTable update(String... columns) {
        final ImmutableUpdateTable.Builder builder = ImmutableUpdateTable.builder().parent(this);
        for (String column : columns) {
            builder.addColumns(Selectable.parse(column));
        }
        return builder.build();
    }

    @Override
    public final UpdateTable update(Collection<String> columns) {
        final ImmutableUpdateTable.Builder builder = ImmutableUpdateTable.builder().parent(this);
        for (String column : columns) {
            builder.addColumns(Selectable.parse(column));
        }
        return builder.build();
    }

    @Override
    public final UpdateTable update2(Collection<Selectable> columns) {
        return ImmutableUpdateTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final SelectTable select() {
        return ImmutableSelectTable.builder().parent(this).build();
    }

    @Override
    public final SelectTable select(String... columns) {
        final ImmutableSelectTable.Builder builder = ImmutableSelectTable.builder().parent(this);
        for (String column : columns) {
            builder.addColumns(Selectable.parse(column));
        }
        return builder.build();
    }

    @Override
    public final SelectTable select(Collection<String> columns) {
        final ImmutableSelectTable.Builder builder = ImmutableSelectTable.builder().parent(this);
        for (String column : columns) {
            builder.addColumns(Selectable.parse(column));
        }
        return builder.build();
    }

    @Override
    public final SelectTable select2(Collection<Selectable> columns) {
        return ImmutableSelectTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final ByTable by() {
        return ImmutableByTable.builder().parent(this).build();
    }

    @Override
    public final ByTable by(String... groupByColumns) {
        return by(Arrays.asList(groupByColumns));
    }

    @Override
    public final ByTable by(Collection<String> groupByColumns) {
        ImmutableByTable.Builder builder = ImmutableByTable.builder().parent(this);
        for (String groupByColumn : groupByColumns) {
            builder.addColumns(Selectable.parse(groupByColumn));
        }
        return builder.build();
    }

    @Override
    public final ByTable by2(Collection<Selectable> groupByColumns) {
        return ImmutableByTable.builder().parent(this).addAllColumns(groupByColumns).build();
    }

    @Override
    public final AggregationTable by(Collection<Selectable> groupByColumns,
        Collection<Aggregation> aggregations) {
        return ImmutableAggregationTable.builder().parent(this).addAllColumns(groupByColumns)
            .addAllAggregations(aggregations).build();
    }

    private static Collection<String> split(String string) {
        return string.trim().isEmpty() ? Collections.emptyList()
            : Arrays.stream(string.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }
}

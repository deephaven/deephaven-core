package io.deephaven.qst.table;

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
    public final Table where(String... filters) {
        return ImmutableWhereTable.builder().parent(this).addFilters(filters).build();
    }

    @Override
    public final WhereTable where(Collection<String> filters) {
        return ImmutableWhereTable.builder().parent(this).addAllFilters(filters).build();
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
        return naturalJoin2(rightTable,
            columnsToMatch.stream().map(JoinMatch::parse).collect(Collectors.toList()),
            columnsToAdd.stream().map(JoinAddition::parse).collect(Collectors.toList()));
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
        return exactJoin2(rightTable,
            columnsToMatch.stream().map(JoinMatch::parse).collect(Collectors.toList()),
            columnsToAdd.stream().map(JoinAddition::parse).collect(Collectors.toList()));
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
    public final ViewTable view(String... columns) {
        return ImmutableViewTable.builder().parent(this).addColumns(columns).build();
    }

    @Override
    public final ViewTable view(Collection<String> columns) {
        return ImmutableViewTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final UpdateViewTable updateView(String... columns) {
        return ImmutableUpdateViewTable.builder().parent(this).addColumns(columns).build();
    }

    @Override
    public final UpdateViewTable updateView(Collection<String> columns) {
        return ImmutableUpdateViewTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final UpdateTable update(String... columns) {
        return ImmutableUpdateTable.builder().parent(this).addColumns(columns).build();
    }

    @Override
    public final UpdateTable update(Collection<String> columns) {
        return ImmutableUpdateTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final SelectTable select() {
        return ImmutableSelectTable.builder().parent(this).build();
    }

    @Override
    public final SelectTable select(String... columns) {
        return ImmutableSelectTable.builder().parent(this).addColumns(columns).build();
    }

    @Override
    public final SelectTable select(Collection<String> columns) {
        return ImmutableSelectTable.builder().parent(this).addAllColumns(columns).build();
    }

    private static Collection<String> split(String string) {
        return string.trim().isEmpty() ? Collections.emptyList()
            : Arrays.stream(string.split(",")).map(String::trim).filter(s -> !s.isEmpty())
                .collect(Collectors.toList());
    }
}

/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
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
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.qst.TableCreationLogic;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
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
    public final LazyUpdateTable lazyUpdate(String... columns) {
        final LazyUpdateTable.Builder builder = LazyUpdateTable.builder().parent(this);
        for (String column : columns) {
            builder.addColumns(Selectable.parse(column));
        }
        return builder.build();
    }

    @Override
    public final LazyUpdateTable lazyUpdate(Collection<? extends Selectable> columns) {
        return LazyUpdateTable.builder().parent(this).addAllColumns(columns).build();
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
    public final AggregateAllByTable groupBy() {
        return aggAllBy(AggSpec.group());
    }

    @Override
    public final AggregateAllByTable groupBy(String... groupByColumns) {
        return aggAllBy(AggSpec.group(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable groupBy(Collection<? extends ColumnName> groupByColumns) {
        return aggAllBy(AggSpec.group(), groupByColumns.toArray(new ColumnName[0]));
    }

    @Override
    public final AggregateAllByTable aggAllBy(AggSpec spec) {
        return AggregateAllByTable.builder().parent(this).spec(spec).build();
    }

    @Override
    public final AggregateAllByTable aggAllBy(AggSpec spec, String... groupByColumns) {
        return aggAllBy(spec, Arrays.asList(groupByColumns));
    }

    @Override
    public final AggregateAllByTable aggAllBy(AggSpec spec, ColumnName... groupByColumns) {
        return AggregateAllByTable.builder().parent(this).spec(spec).addGroupByColumns(groupByColumns).build();
    }

    @Override
    public final AggregateAllByTable aggAllBy(AggSpec spec, Collection<String> groupByColumns) {
        AggregateAllByTable.Builder builder = AggregateAllByTable.builder().parent(this).spec(spec);
        for (String groupByColumn : groupByColumns) {
            builder.addGroupByColumns(ColumnName.of(groupByColumn));
        }
        return builder.build();
    }

    @Override
    public final AggregationTable aggBy(Aggregation aggregation) {
        return AggregationTable.builder().parent(this).addAggregations(aggregation).build();
    }

    @Override
    public final AggregationTable aggBy(Collection<? extends Aggregation> aggregations) {
        return AggregationTable.builder().parent(this).addAllAggregations(aggregations).build();
    }

    @Override
    public TableSpec aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty) {
        return AggregationTable.builder().parent(this)
                .addAllAggregations(aggregations)
                .preserveEmpty(preserveEmpty)
                .build();
    }

    @Override
    public final AggregationTable aggBy(Aggregation aggregation, String... groupByColumns) {
        final AggregationTable.Builder builder = AggregationTable.builder().parent(this);
        for (String groupByColumn : groupByColumns) {
            builder.addGroupByColumns(ColumnName.of(groupByColumn));
        }
        return builder.addAggregations(aggregation).build();
    }

    @Override
    public final AggregationTable aggBy(Aggregation aggregation, Collection<? extends ColumnName> groupByColumns) {
        return AggregationTable.builder().parent(this).addAllGroupByColumns(groupByColumns)
                .addAggregations(aggregation).build();
    }

    @Override
    public final AggregationTable aggBy(Collection<? extends Aggregation> aggregations, String... groupByColumns) {
        final AggregationTable.Builder builder = AggregationTable.builder().parent(this);
        for (String groupByColumn : groupByColumns) {
            builder.addGroupByColumns(ColumnName.of(groupByColumn));
        }
        return builder.addAllAggregations(aggregations).build();
    }

    @Override
    public final AggregationTable aggBy(Collection<? extends Aggregation> aggregations,
            Collection<? extends ColumnName> groupByColumns) {
        return AggregationTable.builder().parent(this).addAllGroupByColumns(groupByColumns)
                .addAllAggregations(aggregations).build();
    }

    @Override
    public TableSpec aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty,
            TableSpec initialGroups, Collection<? extends ColumnName> groupByColumns) {
        return AggregationTable.builder().parent(this)
                .addAllGroupByColumns(groupByColumns)
                .addAllAggregations(aggregations)
                .preserveEmpty(preserveEmpty)
                .initialGroups(Optional.ofNullable(initialGroups))
                .build();
    }

    @Override
    public final UpdateByTable updateBy(UpdateByOperation operation) {
        return UpdateByTable.builder()
                .parent(this)
                .addOperations(operation)
                .build();
    }

    @Override
    public final UpdateByTable updateBy(UpdateByOperation operation, String... byColumns) {
        UpdateByTable.Builder builder = UpdateByTable.builder()
                .parent(this)
                .addOperations(operation);
        for (String byColumn : byColumns) {
            builder.addGroupByColumns(ColumnName.of(byColumn));
        }
        return builder.build();
    }

    @Override
    public final UpdateByTable updateBy(Collection<? extends UpdateByOperation> operations,
            Collection<? extends ColumnName> byColumns) {
        return UpdateByTable.builder()
                .parent(this)
                .addAllOperations(operations)
                .addAllGroupByColumns(byColumns)
                .build();
    }

    @Override
    public final UpdateByTable updateBy(Collection<? extends UpdateByOperation> operations) {
        return UpdateByTable.builder()
                .parent(this)
                .addAllOperations(operations)
                .build();
    }

    @Override
    public final UpdateByTable updateBy(Collection<? extends UpdateByOperation> operations, String... byColumns) {
        UpdateByTable.Builder builder = UpdateByTable.builder()
                .parent(this)
                .addAllOperations(operations);
        for (String byColumn : byColumns) {
            builder.addGroupByColumns(ColumnName.of(byColumn));
        }
        return builder.build();
    }

    @Override
    public final UpdateByTable updateBy(UpdateByControl control, Collection<? extends UpdateByOperation> operations) {
        return UpdateByTable.builder()
                .parent(this)
                .control(control)
                .addAllOperations(operations)
                .build();
    }

    @Override
    public final UpdateByTable updateBy(UpdateByControl control, Collection<? extends UpdateByOperation> operations,
            Collection<? extends ColumnName> byColumns) {
        return UpdateByTable.builder()
                .parent(this)
                .control(control)
                .addAllOperations(operations)
                .addAllGroupByColumns(byColumns)
                .build();
    }

    @Override
    public final SelectDistinctTable selectDistinct() {
        return SelectDistinctTable.builder().parent(this).build();
    }

    @Override
    public final SelectDistinctTable selectDistinct(String... columns) {
        final SelectDistinctTable.Builder builder = SelectDistinctTable.builder().parent(this);
        for (String column : columns) {
            builder.addColumns(Selectable.parse(column));
        }
        return builder.build();
    }

    @Override
    public final SelectDistinctTable selectDistinct(Selectable... columns) {
        return selectDistinct(Arrays.asList(columns));
    }

    @Override
    public final SelectDistinctTable selectDistinct(Collection<? extends Selectable> columns) {
        return SelectDistinctTable.builder().parent(this).addAllColumns(columns).build();
    }

    @Override
    public final CountByTable countBy(String countColumnName) {
        return CountByTable.builder().parent(this).countName(ColumnName.of(countColumnName)).build();
    }

    @Override
    public final CountByTable countBy(String countColumnName, String... groupByColumns) {
        return countBy(countColumnName, Arrays.asList(groupByColumns));
    }

    @Override
    public final CountByTable countBy(String countColumnName, ColumnName... groupByColumns) {
        return CountByTable.builder().parent(this).countName(ColumnName.of(countColumnName))
                .addGroupByColumns(groupByColumns).build();
    }

    @Override
    public final CountByTable countBy(String countColumnName, Collection<String> groupByColumns) {
        CountByTable.Builder builder = CountByTable.builder().parent(this).countName(ColumnName.of(countColumnName));
        for (String groupByColumn : groupByColumns) {
            builder.addGroupByColumns(ColumnName.of(groupByColumn));
        }
        return builder.build();
    }

    @Override
    public final AggregateAllByTable firstBy() {
        return aggAllBy(AggSpec.first());
    }

    @Override
    public final AggregateAllByTable firstBy(String... groupByColumns) {
        return aggAllBy(AggSpec.first(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable firstBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.first(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable firstBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.first(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable lastBy() {
        return aggAllBy(AggSpec.last());
    }

    @Override
    public final AggregateAllByTable lastBy(String... groupByColumns) {
        return aggAllBy(AggSpec.last(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable lastBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.last(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable lastBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.last(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable minBy() {
        return aggAllBy(AggSpec.min());
    }

    @Override
    public final AggregateAllByTable minBy(String... groupByColumns) {
        return aggAllBy(AggSpec.min(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable minBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.min(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable minBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.min(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable maxBy() {
        return aggAllBy(AggSpec.max());
    }

    @Override
    public final AggregateAllByTable maxBy(String... groupByColumns) {
        return aggAllBy(AggSpec.max(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable maxBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.max(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable maxBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.max(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable sumBy() {
        return aggAllBy(AggSpec.sum());
    }

    @Override
    public final AggregateAllByTable sumBy(String... groupByColumns) {
        return aggAllBy(AggSpec.sum(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable sumBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.sum(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable sumBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.sum(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable avgBy() {
        return aggAllBy(AggSpec.avg());
    }

    @Override
    public final AggregateAllByTable avgBy(String... groupByColumns) {
        return aggAllBy(AggSpec.avg(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable avgBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.avg(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable avgBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.avg(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable medianBy() {
        return aggAllBy(AggSpec.median());
    }

    @Override
    public final AggregateAllByTable medianBy(String... groupByColumns) {
        return aggAllBy(AggSpec.median(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable medianBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.median(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable medianBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.median(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable stdBy() {
        return aggAllBy(AggSpec.std());
    }

    @Override
    public final AggregateAllByTable stdBy(String... groupByColumns) {
        return aggAllBy(AggSpec.std(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable stdBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.std(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable stdBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.std(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable varBy() {
        return aggAllBy(AggSpec.var());
    }

    @Override
    public final AggregateAllByTable varBy(String... groupByColumns) {
        return aggAllBy(AggSpec.var(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable varBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.var(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable varBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.var(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable absSumBy() {
        return aggAllBy(AggSpec.absSum());
    }

    @Override
    public final AggregateAllByTable absSumBy(String... groupByColumns) {
        return aggAllBy(AggSpec.absSum(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable absSumBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.absSum(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable absSumBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.absSum(), groupByColumns);
    }

    @Override
    public final AggregateAllByTable wsumBy(String weightColumn) {
        return aggAllBy(AggSpec.wsum(weightColumn));
    }

    @Override
    public final AggregateAllByTable wsumBy(String weightColumn, String... groupByColumns) {
        return aggAllBy(AggSpec.wsum(weightColumn), groupByColumns);
    }

    @Override
    public final AggregateAllByTable wsumBy(String weightColumn, ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.wsum(weightColumn), groupByColumns);
    }

    @Override
    public final AggregateAllByTable wsumBy(String weightColumn, Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.wsum(weightColumn), groupByColumns);
    }

    @Override
    public final AggregateAllByTable wavgBy(String weightColumn) {
        return aggAllBy(AggSpec.wavg(weightColumn));
    }

    @Override
    public final AggregateAllByTable wavgBy(String weightColumn, String... groupByColumns) {
        return aggAllBy(AggSpec.wavg(weightColumn), groupByColumns);
    }

    @Override
    public final AggregateAllByTable wavgBy(String weightColumn, ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.wavg(weightColumn), groupByColumns);
    }

    @Override
    public final AggregateAllByTable wavgBy(String weightColumn, Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.wavg(weightColumn), groupByColumns);
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

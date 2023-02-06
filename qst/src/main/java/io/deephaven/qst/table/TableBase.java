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
import io.deephaven.api.expression.AsOfJoinMatchFactory;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.snapshot.SnapshotWhenOptions;
import io.deephaven.api.snapshot.SnapshotWhenOptions.Flag;
import io.deephaven.api.updateby.UpdateByControl;
import io.deephaven.api.updateby.UpdateByOperation;
import io.deephaven.qst.TableCreationLogic;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

import static io.deephaven.api.TableOperationsDefaults.splitToCollection;

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
    public final SnapshotTable snapshot() {
        return SnapshotTable.of(this);
    }

    @Override
    public final SnapshotWhenTable snapshotWhen(TableSpec trigger, Flag... features) {
        return SnapshotWhenTable.of(this, trigger, SnapshotWhenOptions.of(features));
    }

    @Override
    public final SnapshotWhenTable snapshotWhen(TableSpec trigger, Collection<Flag> features,
            String... stampColumns) {
        return SnapshotWhenTable.of(this, trigger, SnapshotWhenOptions.of(features, stampColumns));
    }

    @Override
    public final SnapshotWhenTable snapshotWhen(TableSpec trigger, SnapshotWhenOptions options) {
        return SnapshotWhenTable.of(this, trigger, options);
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
        return whereIn(rightTable, columnsToMatch, false);
    }

    @Override
    public final WhereInTable whereIn(TableSpec rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return whereIn(rightTable, columnsToMatch, false);
    }

    @Override
    public final WhereInTable whereNotIn(TableSpec rightTable, String... columnsToMatch) {
        return whereIn(rightTable, columnsToMatch, true);
    }

    @Override
    public final WhereInTable whereNotIn(TableSpec rightTable, Collection<? extends JoinMatch> columnsToMatch) {
        return whereIn(rightTable, columnsToMatch, true);
    }

    private WhereInTable whereIn(TableSpec rightTable, String[] columnsToMatch, boolean inverted) {
        WhereInTable.Builder builder = WhereInTable.builder()
                .left(this)
                .right(rightTable)
                .inverted(inverted);
        for (String toMatch : columnsToMatch) {
            builder.addMatches(JoinMatch.parse(toMatch));
        }
        return builder.build();
    }

    private WhereInTable whereIn(TableSpec rightTable, Collection<? extends JoinMatch> columnsToMatch,
            boolean inverted) {
        return WhereInTable.builder()
                .left(this)
                .right(rightTable)
                .addAllMatches(columnsToMatch)
                .inverted(inverted)
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
        AsOfJoinMatchFactory.AsOfJoinResult result =
                AsOfJoinMatchFactory.getAjExpressions(splitToCollection(columnsToMatch));
        builder.addMatches(result.matches);
        builder.rule(result.rule);
        return builder.build();
    }

    @Override
    public final AsOfJoinTable aj(TableSpec rightTable, String columnsToMatch,
            String columnsToAdd) {
        AsOfJoinTable.Builder builder = AsOfJoinTable.builder().left(this).right(rightTable);
        AsOfJoinMatchFactory.AsOfJoinResult result =
                AsOfJoinMatchFactory.getAjExpressions(splitToCollection(columnsToMatch));
        builder.addMatches(result.matches);
        builder.rule(result.rule);
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
        AsOfJoinMatchFactory.ReverseAsOfJoinResult result =
                AsOfJoinMatchFactory.getRajExpressions(splitToCollection(columnsToMatch));
        builder.addMatches(result.matches);
        builder.rule(result.rule);
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
        AsOfJoinMatchFactory.ReverseAsOfJoinResult result =
                AsOfJoinMatchFactory.getRajExpressions(splitToCollection(columnsToMatch));
        builder.addMatches(result.matches);
        builder.rule(result.rule);
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
    public final AggregateAllTable groupBy() {
        return aggAllBy(AggSpec.group());
    }

    @Override
    public final AggregateAllTable groupBy(String... groupByColumns) {
        return aggAllBy(AggSpec.group(), groupByColumns);
    }

    @Override
    public final AggregateAllTable groupBy(Collection<? extends ColumnName> groupByColumns) {
        return aggAllBy(AggSpec.group(), groupByColumns.toArray(new ColumnName[0]));
    }

    @Override
    public final AggregateAllTable aggAllBy(AggSpec spec) {
        return AggregateAllTable.builder().parent(this).spec(spec).build();
    }

    @Override
    public final AggregateAllTable aggAllBy(AggSpec spec, String... groupByColumns) {
        return aggAllBy(spec, Arrays.asList(groupByColumns));
    }

    @Override
    public final AggregateAllTable aggAllBy(AggSpec spec, ColumnName... groupByColumns) {
        return AggregateAllTable.builder()
                .parent(this)
                .spec(spec)
                .addGroupByColumns(groupByColumns)
                .build();
    }

    @Override
    public final AggregateAllTable aggAllBy(AggSpec spec, Collection<String> groupByColumns) {
        AggregateAllTable.Builder builder = AggregateAllTable.builder()
                .parent(this)
                .spec(spec);
        for (String groupByColumn : groupByColumns) {
            builder.addGroupByColumns(ColumnName.of(groupByColumn));
        }
        return builder.build();
    }

    @Override
    public final AggregateTable aggBy(Aggregation aggregation) {
        return AggregateTable.builder()
                .parent(this)
                .addAggregations(aggregation)
                .build();
    }

    @Override
    public final AggregateTable aggBy(Collection<? extends Aggregation> aggregations) {
        return AggregateTable.builder()
                .parent(this)
                .addAllAggregations(aggregations)
                .build();
    }

    @Override
    public TableSpec aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty) {
        return AggregateTable.builder()
                .parent(this)
                .addAllAggregations(aggregations)
                .preserveEmpty(preserveEmpty)
                .build();
    }

    @Override
    public final AggregateTable aggBy(Aggregation aggregation, String... groupByColumns) {
        final AggregateTable.Builder builder = AggregateTable.builder().parent(this);
        for (String groupByColumn : groupByColumns) {
            builder.addGroupByColumns(ColumnName.of(groupByColumn));
        }
        return builder.addAggregations(aggregation).build();
    }

    @Override
    public final AggregateTable aggBy(Aggregation aggregation, Collection<? extends ColumnName> groupByColumns) {
        return AggregateTable.builder()
                .parent(this)
                .addAllGroupByColumns(groupByColumns)
                .addAggregations(aggregation)
                .build();
    }

    @Override
    public final AggregateTable aggBy(Collection<? extends Aggregation> aggregations, String... groupByColumns) {
        final AggregateTable.Builder builder = AggregateTable.builder().parent(this);
        for (String groupByColumn : groupByColumns) {
            builder.addGroupByColumns(ColumnName.of(groupByColumn));
        }
        return builder.addAllAggregations(aggregations).build();
    }

    @Override
    public final AggregateTable aggBy(Collection<? extends Aggregation> aggregations,
            Collection<? extends ColumnName> groupByColumns) {
        return AggregateTable.builder()
                .parent(this)
                .addAllGroupByColumns(groupByColumns)
                .addAllAggregations(aggregations)
                .build();
    }

    @Override
    public TableSpec aggBy(Collection<? extends Aggregation> aggregations, boolean preserveEmpty,
            TableSpec initialGroups, Collection<? extends ColumnName> groupByColumns) {
        return AggregateTable.builder()
                .parent(this)
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
    public final AggregateTable countBy(String countColumnName) {
        return countBy(countColumnName, Collections.emptyList());
    }

    @Override
    public final AggregateTable countBy(String countColumnName, String... groupByColumns) {
        return countBy(countColumnName, Arrays.asList(groupByColumns));
    }

    @Override
    public final AggregateTable countBy(String countColumnName, ColumnName... groupByColumns) {
        return AggregateTable.builder()
                .parent(this)
                .addAggregations(Aggregation.AggCount(countColumnName))
                .addGroupByColumns(groupByColumns)
                .build();
    }

    @Override
    public final AggregateTable countBy(String countColumnName, Collection<String> groupByColumns) {
        AggregateTable.Builder builder = AggregateTable.builder()
                .parent(this)
                .addAggregations(Aggregation.AggCount(countColumnName));
        for (String groupByColumn : groupByColumns) {
            builder.addGroupByColumns(ColumnName.of(groupByColumn));
        }
        return builder.build();
    }

    @Override
    public final AggregateAllTable firstBy() {
        return aggAllBy(AggSpec.first());
    }

    @Override
    public final AggregateAllTable firstBy(String... groupByColumns) {
        return aggAllBy(AggSpec.first(), groupByColumns);
    }

    @Override
    public final AggregateAllTable firstBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.first(), groupByColumns);
    }

    @Override
    public final AggregateAllTable firstBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.first(), groupByColumns);
    }

    @Override
    public final AggregateAllTable lastBy() {
        return aggAllBy(AggSpec.last());
    }

    @Override
    public final AggregateAllTable lastBy(String... groupByColumns) {
        return aggAllBy(AggSpec.last(), groupByColumns);
    }

    @Override
    public final AggregateAllTable lastBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.last(), groupByColumns);
    }

    @Override
    public final AggregateAllTable lastBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.last(), groupByColumns);
    }

    @Override
    public final AggregateAllTable minBy() {
        return aggAllBy(AggSpec.min());
    }

    @Override
    public final AggregateAllTable minBy(String... groupByColumns) {
        return aggAllBy(AggSpec.min(), groupByColumns);
    }

    @Override
    public final AggregateAllTable minBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.min(), groupByColumns);
    }

    @Override
    public final AggregateAllTable minBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.min(), groupByColumns);
    }

    @Override
    public final AggregateAllTable maxBy() {
        return aggAllBy(AggSpec.max());
    }

    @Override
    public final AggregateAllTable maxBy(String... groupByColumns) {
        return aggAllBy(AggSpec.max(), groupByColumns);
    }

    @Override
    public final AggregateAllTable maxBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.max(), groupByColumns);
    }

    @Override
    public final AggregateAllTable maxBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.max(), groupByColumns);
    }

    @Override
    public final AggregateAllTable sumBy() {
        return aggAllBy(AggSpec.sum());
    }

    @Override
    public final AggregateAllTable sumBy(String... groupByColumns) {
        return aggAllBy(AggSpec.sum(), groupByColumns);
    }

    @Override
    public final AggregateAllTable sumBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.sum(), groupByColumns);
    }

    @Override
    public final AggregateAllTable sumBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.sum(), groupByColumns);
    }

    @Override
    public final AggregateAllTable avgBy() {
        return aggAllBy(AggSpec.avg());
    }

    @Override
    public final AggregateAllTable avgBy(String... groupByColumns) {
        return aggAllBy(AggSpec.avg(), groupByColumns);
    }

    @Override
    public final AggregateAllTable avgBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.avg(), groupByColumns);
    }

    @Override
    public final AggregateAllTable avgBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.avg(), groupByColumns);
    }

    @Override
    public final AggregateAllTable medianBy() {
        return aggAllBy(AggSpec.median());
    }

    @Override
    public final AggregateAllTable medianBy(String... groupByColumns) {
        return aggAllBy(AggSpec.median(), groupByColumns);
    }

    @Override
    public final AggregateAllTable medianBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.median(), groupByColumns);
    }

    @Override
    public final AggregateAllTable medianBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.median(), groupByColumns);
    }

    @Override
    public final AggregateAllTable stdBy() {
        return aggAllBy(AggSpec.std());
    }

    @Override
    public final AggregateAllTable stdBy(String... groupByColumns) {
        return aggAllBy(AggSpec.std(), groupByColumns);
    }

    @Override
    public final AggregateAllTable stdBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.std(), groupByColumns);
    }

    @Override
    public final AggregateAllTable stdBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.std(), groupByColumns);
    }

    @Override
    public final AggregateAllTable varBy() {
        return aggAllBy(AggSpec.var());
    }

    @Override
    public final AggregateAllTable varBy(String... groupByColumns) {
        return aggAllBy(AggSpec.var(), groupByColumns);
    }

    @Override
    public final AggregateAllTable varBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.var(), groupByColumns);
    }

    @Override
    public final AggregateAllTable varBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.var(), groupByColumns);
    }

    @Override
    public final AggregateAllTable absSumBy() {
        return aggAllBy(AggSpec.absSum());
    }

    @Override
    public final AggregateAllTable absSumBy(String... groupByColumns) {
        return aggAllBy(AggSpec.absSum(), groupByColumns);
    }

    @Override
    public final AggregateAllTable absSumBy(ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.absSum(), groupByColumns);
    }

    @Override
    public final AggregateAllTable absSumBy(Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.absSum(), groupByColumns);
    }

    @Override
    public final AggregateAllTable wsumBy(String weightColumn) {
        return aggAllBy(AggSpec.wsum(weightColumn));
    }

    @Override
    public final AggregateAllTable wsumBy(String weightColumn, String... groupByColumns) {
        return aggAllBy(AggSpec.wsum(weightColumn), groupByColumns);
    }

    @Override
    public final AggregateAllTable wsumBy(String weightColumn, ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.wsum(weightColumn), groupByColumns);
    }

    @Override
    public final AggregateAllTable wsumBy(String weightColumn, Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.wsum(weightColumn), groupByColumns);
    }

    @Override
    public final AggregateAllTable wavgBy(String weightColumn) {
        return aggAllBy(AggSpec.wavg(weightColumn));
    }

    @Override
    public final AggregateAllTable wavgBy(String weightColumn, String... groupByColumns) {
        return aggAllBy(AggSpec.wavg(weightColumn), groupByColumns);
    }

    @Override
    public final AggregateAllTable wavgBy(String weightColumn, ColumnName... groupByColumns) {
        return aggAllBy(AggSpec.wavg(weightColumn), groupByColumns);
    }

    @Override
    public final AggregateAllTable wavgBy(String weightColumn, Collection<String> groupByColumns) {
        return aggAllBy(AggSpec.wavg(weightColumn), groupByColumns);
    }

    @Override
    public final UngroupTable ungroup() {
        return UngroupTable.builder()
                .parent(this)
                .build();
    }

    @Override
    public final UngroupTable ungroup(boolean nullFill) {
        return UngroupTable.builder()
                .parent(this)
                .nullFill(nullFill)
                .build();
    }

    @Override
    public final UngroupTable ungroup(String... columnsToUngroup) {
        final UngroupTable.Builder builder = UngroupTable.builder()
                .parent(this);
        for (String columnToUngroup : columnsToUngroup) {
            builder.addUngroupColumns(ColumnName.of(columnToUngroup));
        }
        return builder.build();
    }

    @Override
    public final UngroupTable ungroup(boolean nullFill, String... columnsToUngroup) {
        final UngroupTable.Builder builder = UngroupTable.builder()
                .parent(this)
                .nullFill(nullFill);
        for (String columnToUngroup : columnsToUngroup) {
            builder.addUngroupColumns(ColumnName.of(columnToUngroup));
        }
        return builder.build();
    }

    @Override
    public final UngroupTable ungroup(boolean nullFill, Collection<? extends ColumnName> columnsToUngroup) {
        return UngroupTable.builder()
                .parent(this)
                .nullFill(nullFill)
                .addAllUngroupColumns(columnsToUngroup)
                .build();
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

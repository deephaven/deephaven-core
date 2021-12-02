package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;

import java.io.Serializable;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Represents an aggregation that can be applied to a table.
 *
 * @see io.deephaven.api.TableOperations#aggBy(Collection, Collection)
 */
public interface Aggregation extends Serializable {

    static AbsSum AggAbsSum(String pair) {
        return AggregationFinisher.absSum().of(pair);
    }

    static Aggregation AggAbsSum(String... pairs) {
        return AggregationFinisher.absSum().of(pairs);
    }

    static Group AggGroup(String pair) {
        return AggregationFinisher.group().of(pair);
    }

    static Aggregation AggGroup(String... pairs) {
        return AggregationFinisher.group().of(pairs);
    }

    static Avg AggAvg(String pair) {
        return AggregationFinisher.avg().of(pair);
    }

    static Aggregation AggAvg(String... pairs) {
        return AggregationFinisher.avg().of(pairs);
    }

    static Count AggCount(String resultColumn) {
        return Count.of(resultColumn);
    }

    static CountDistinct AggCountDistinct(String pair) {
        return AggregationFinisher.countDistinct().of(pair);
    }

    static Aggregation AggCountDistinct(String... pairs) {
        return AggregationFinisher.countDistinct().of(pairs);
    }

    static CountDistinct AggCountDistinct(boolean countNulls, String pair) {
        return AggregationFinisher.countDistinct(countNulls).of(pair);
    }

    static Aggregation AggCountDistinct(boolean countNulls, String... pairs) {
        return AggregationFinisher.countDistinct(countNulls).of(pairs);
    }

    static Distinct AggDistinct(String pair) {
        return AggregationFinisher.distinct().of(pair);
    }

    static Aggregation AggDistinct(String... pairs) {
        return AggregationFinisher.distinct().of(pairs);
    }

    static Distinct AggDistinct(boolean includeNulls, String pair) {
        return AggregationFinisher.distinct(includeNulls).of(pair);
    }

    static Aggregation AggDistinct(boolean includeNulls, String... pairs) {
        return AggregationFinisher.distinct(includeNulls).of(pairs);
    }

    static First AggFirst(String pair) {
        return AggregationFinisher.first().of(pair);
    }

    static Aggregation AggFirst(String... pairs) {
        return AggregationFinisher.first().of(pairs);
    }

    static Last AggLast(String pair) {
        return AggregationFinisher.last().of(pair);
    }

    static Aggregation AggLast(String... pairs) {
        return AggregationFinisher.last().of(pairs);
    }

    static Max AggMax(String pair) {
        return AggregationFinisher.max().of(pair);
    }

    static Aggregation AggMax(String... pairs) {
        return AggregationFinisher.max().of(pairs);
    }

    static Med AggMed(String pair) {
        return AggregationFinisher.med().of(pair);
    }

    static Aggregation AggMed(String... pairs) {
        return AggregationFinisher.med().of(pairs);
    }

    static Min AggMin(String pair) {
        return AggregationFinisher.min().of(pair);
    }

    static Med AggMed(boolean average, String pair) {
        return AggregationFinisher.med(average).of(pair);
    }

    static Aggregation AggMed(boolean average, String... pairs) {
        return AggregationFinisher.med(average).of(pairs);
    }

    static Aggregation AggMin(String... pairs) {
        return AggregationFinisher.min().of(pairs);
    }

    static Pct AggPct(double percentile, String pair) {
        return AggregationFinisher.pct(percentile).of(pair);
    }

    static Aggregation AggPct(double percentile, String... pairs) {
        return AggregationFinisher.pct(percentile).of(pairs);
    }

    static Pct AggPct(double percentile, boolean average, String pair) {
        return AggregationFinisher.pct(percentile, average).of(pair);
    }

    static Aggregation AggPct(double percentile, boolean average, String... pairs) {
        return AggregationFinisher.pct(percentile, average).of(pairs);
    }

    static SortedFirst AggSortedFirst(String sortedColumn, String pair) {
        return AggregationFinisher.sortedFirst(SortColumn.asc(ColumnName.of(sortedColumn)))
                .of(pair);
    }

    static Aggregation AggSortedFirst(String sortedColumn, String... pairs) {
        return AggregationFinisher.sortedFirst(SortColumn.asc(ColumnName.of(sortedColumn)))
                .of(pairs);
    }

    static SortedFirst AggSortedFirst(Collection<? extends String> sortedColumns, String pair) {
        return AggregationFinisher.sortedFirst(
                sortedColumns.stream().map(ColumnName::of).map(SortColumn::asc).collect(Collectors.toList()))
                .of(pair);
    }

    static Aggregation AggSortedFirst(Collection<? extends String> sortedColumns, String... pairs) {
        return AggregationFinisher.sortedFirst(
                sortedColumns.stream().map(ColumnName::of).map(SortColumn::asc).collect(Collectors.toList()))
                .of(pairs);
    }

    static SortedLast AggSortedLast(String sortedColumn, String pair) {
        return AggregationFinisher.sortedLast(SortColumn.asc(ColumnName.of(sortedColumn)))
                .of(pair);
    }

    static Aggregation AggSortedLast(String sortedColumn, String... pairs) {
        return AggregationFinisher.sortedLast(SortColumn.asc(ColumnName.of(sortedColumn)))
                .of(pairs);
    }

    static SortedLast AggSortedLast(Collection<? extends String> sortedColumns, String pair) {
        return AggregationFinisher.sortedLast(
                sortedColumns.stream().map(ColumnName::of).map(SortColumn::asc).collect(Collectors.toList()))
                .of(pair);
    }

    static Aggregation AggSortedLast(Collection<? extends String> sortedColumns, String... pairs) {
        return AggregationFinisher.sortedLast(
                sortedColumns.stream().map(ColumnName::of).map(SortColumn::asc).collect(Collectors.toList()))
                .of(pairs);
    }

    static Std AggStd(String pair) {
        return AggregationFinisher.std().of(pair);
    }

    static Aggregation AggStd(String... pairs) {
        return AggregationFinisher.std().of(pairs);
    }

    static Sum AggSum(String pair) {
        return AggregationFinisher.sum().of(pair);
    }

    static Aggregation AggSum(String... pairs) {
        return AggregationFinisher.sum().of(pairs);
    }

    static Unique AggUnique(String pair) {
        return AggregationFinisher.unique().of(pair);
    }

    static Aggregation AggUnique(String... pairs) {
        return AggregationFinisher.unique().of(pairs);
    }

    static Unique AggUnique(boolean includeNulls, String pair) {
        return AggregationFinisher.unique(includeNulls).of(pair);
    }

    static Aggregation AggUnique(boolean includeNulls, String... pairs) {
        return AggregationFinisher.unique(includeNulls).of(pairs);
    }

    static Var AggVar(String pair) {
        return AggregationFinisher.var().of(pair);
    }

    static Aggregation AggVar(String... pairs) {
        return AggregationFinisher.var().of(pairs);
    }

    static WAvg AggWAvg(String weightColumn, String pair) {
        return AggregationFinisher.wAvg(ColumnName.of(weightColumn)).of(pair);
    }

    static Aggregation AggWAvg(String weightColumn, String... pairs) {
        return AggregationFinisher.wAvg(ColumnName.of(weightColumn)).of(pairs);
    }

    static WSum AggWSum(String weightColumn, String pair) {
        return AggregationFinisher.wSum(ColumnName.of(weightColumn)).of(pair);
    }

    static Aggregation AggWSum(String weightColumn, String... pairs) {
        return AggregationFinisher.wSum(ColumnName.of(weightColumn)).of(pairs);
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(AbsSum absSum);

        void visit(Group group);

        void visit(Avg avg);

        void visit(Count count);

        void visit(CountDistinct countDistinct);

        void visit(Distinct distinct);

        void visit(First first);

        void visit(Last last);

        void visit(Max max);

        void visit(Med med);

        void visit(Min min);

        void visit(Multi<?> multi);

        void visit(Pct pct);

        void visit(SortedFirst sortedFirst);

        void visit(SortedLast sortedLast);

        void visit(Std std);

        void visit(Sum sum);

        void visit(Unique unique);

        void visit(Var var);

        void visit(WAvg wAvg);

        void visit(WSum wSum);
    }
}

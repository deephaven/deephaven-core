package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.JoinAddition;
import io.deephaven.api.JoinMatch;
import io.deephaven.api.SortColumn;

public interface Aggregation {

    static Min AggMin(String matchPair) {
        return Min.of(matchPair);
    }

    static Max AggMax(String matchPair) {
        return Max.of(matchPair);
    }

    static Sum AggSum(String matchPair) {
        return Sum.of(matchPair);
    }

    static AbsSum AggAbsSum(String matchPair) {
        return AbsSum.of(matchPair);
    }

    static Var AggVar(String matchPair) {
        return Var.of(matchPair);
    }

    static Avg AggAvg(String matchPair) {
        return Avg.of(matchPair);
    }

    static First AggFirst(String matchPair) {
        return First.of(matchPair);
    }

    static Last AggLast(String matchPair) {
        return Last.of(matchPair);
    }

    static Std AggStd(String matchPair) {
        return Std.of(matchPair);
    }

    static Med AggMed(String matchPair) {
        return Med.of(matchPair);
    }

    static Pct AggPct(double percentile, String addition) {
        return Pct.of(percentile, JoinAddition.parse(addition));
    }

    static WSum AggWSum(String weight, String addition) {
        return WSum.of(ColumnName.of(weight), JoinAddition.parse(addition));
    }

    static WAvg AggWAvg(String weight, String addition) {
        return WAvg.of(ColumnName.of(weight), JoinAddition.parse(addition));
    }

    static Count AggCount(String resultColumn) {
        return Count.of(resultColumn);
    }

    static CountDistinct AggCountDistinct(String matchPair) {
        return CountDistinct.of(matchPair);
    }

    static Distinct AggDistinct(String matchPair) {
        return Distinct.of(matchPair);
    }

    static Array AggArray(String matchPair) {
        return Array.of(matchPair);
    }

    static Unique AggUnique(String matchPair) {
        return Unique.of(matchPair);
    }

    static SortedFirst AggSortedFirst(String sortedColumn, String matchPair) {
        return ImmutableSortedFirst.builder().addition(JoinAddition.parse(matchPair))
            .addColumns(SortColumn.asc(ColumnName.of(sortedColumn))).build();
    }

    static SortedFirst AggSortedFirst(String[] sortedColumns, String matchPair) {
        ImmutableSortedFirst.Builder builder =
            ImmutableSortedFirst.builder().addition(JoinAddition.parse(matchPair));
        for (String col : sortedColumns) {
            builder.addColumns(SortColumn.asc(ColumnName.of(col)));
        }
        return builder.build();
    }

    static SortedLast AggSortedLast(String sortedColumn, String matchPair) {
        return ImmutableSortedLast.builder().addition(JoinAddition.parse(matchPair))
            .addColumns(SortColumn.asc(ColumnName.of(sortedColumn))).build();
    }

    static SortedLast AggSortedLast(String[] sortedColumns, String matchPair) {
        ImmutableSortedLast.Builder builder =
            ImmutableSortedLast.builder().addition(JoinAddition.parse(matchPair));
        for (String col : sortedColumns) {
            builder.addColumns(SortColumn.asc(ColumnName.of(col)));
        }
        return builder.build();
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(Min min);

        void visit(Max max);

        void visit(Sum sum);

        void visit(AbsSum absSum);

        void visit(Var var);

        void visit(Avg avg);

        void visit(First first);

        void visit(Last last);

        void visit(Std std);

        void visit(Med med);

        void visit(Pct pct);

        void visit(WSum wSum);

        void visit(WAvg wAvg);

        void visit(Count count);

        void visit(CountDistinct countDistinct);

        void visit(Distinct distinct);

        void visit(Array array);

        void visit(Unique unique);

        void visit(SortedFirst sortedFirst);

        void visit(SortedLast sortedLast);
    }
}

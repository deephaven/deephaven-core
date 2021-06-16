package io.deephaven.qst.table.agg;

import io.deephaven.qst.table.ColumnName;
import io.deephaven.qst.table.JoinMatch;

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

    static Pct AggPct(double percentile, String matchPair) {
        return ImmutablePct.builder().match(JoinMatch.parse(matchPair)).percentile(percentile).build();
    }

    static WSum AggWSum(String weight, String matchPair) {
        return ImmutableWSum.of(JoinMatch.parse(matchPair), ColumnName.of(weight));
    }

    static WAvg AggWAvg(String weight, String matchPair) {
        return ImmutableWAvg.of(JoinMatch.parse(matchPair), ColumnName.of(weight));
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

    JoinMatch match();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(Min min);

        void visit(Max max);

        void visit(Sum sum);

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
    }
}

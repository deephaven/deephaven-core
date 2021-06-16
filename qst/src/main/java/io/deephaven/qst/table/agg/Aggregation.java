package io.deephaven.qst.table.agg;

import io.deephaven.qst.table.JoinMatch;

public interface Aggregation {

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

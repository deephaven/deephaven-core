package io.deephaven.api.agg.key;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.KeyedAggregation;
import io.deephaven.api.agg.Pair;

import java.util.Collection;

/**
 * A key represents the configurable options for {@link KeyedAggregation}, except for the input/output {@link Pair}.
 */
public interface Key {

    KeyedAggregation aggregation(Pair pair);

    Aggregation aggregation(Pair... pairs);

    Aggregation aggregation(Collection<? extends Pair> pairs);

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(KeyAbsSum absSum);

        void visit(KeyCountDistinct countDistinct);

        void visit(KeyDistinct distinct);

        void visit(KeyGroup group);

        void visit(KeyAvg avg);

        void visit(KeyFirst first);

        void visit(KeyLast last);

        void visit(KeyMax max);

        void visit(KeyMedian median);

        void visit(KeyMin min);

        void visit(KeyPct pct);

        void visit(KeySortedFirst sortedFirst);

        void visit(KeySortedLast sortedLast);

        void visit(KeyStd std);

        void visit(KeySum sum);

        void visit(KeyUnique unique);

        void visit(KeyWAvg wAvg);

        void visit(KeyWSum wSum);

        void visit(KeyVar var);
    }
}

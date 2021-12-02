package io.deephaven.api.agg;

import io.deephaven.api.agg.NormalAggregations.Builder;
import io.deephaven.api.agg.spec.AggSpec;

import java.io.Serializable;
import java.util.Collection;

/**
 * Represents an aggregation that can be applied to a table.
 *
 * @see io.deephaven.api.TableOperations#aggBy(Collection, Collection)
 * @see Count
 * @see NormalAggregation
 * @see NormalAggregations
 */
public interface Aggregation extends Serializable {

    static NormalAggregation of(AggSpec spec, String pair) {
        return NormalAggregation.of(spec, Pair.parse(pair));
    }

    static Aggregation of(AggSpec spec, String... pairs) {
        if (pairs.length == 1) {
            return of(spec, pairs[0]);
        }
        final Builder builder = NormalAggregations.builder().spec(spec);
        for (String pair : pairs) {
            builder.addPairs(Pair.parse(pair));
        }
        return builder.build();
    }

    static Aggregation AggAbsSum(String... pairs) {
        return of(AggSpec.absSum(), pairs);
    }

    static Aggregation AggAvg(String... pairs) {
        return of(AggSpec.avg(), pairs);
    }

    static Aggregation AggCount(String resultColumn) {
        return Count.of(resultColumn);
    }

    static Aggregation AggCountDistinct(String... pairs) {
        return of(AggSpec.countDistinct(), pairs);
    }

    static Aggregation AggCountDistinct(boolean countNulls, String... pairs) {
        return of(AggSpec.countDistinct(countNulls), pairs);
    }

    static Aggregation AggDistinct(String... pairs) {
        return of(AggSpec.distinct(), pairs);
    }

    static Aggregation AggDistinct(boolean includeNulls, String... pairs) {
        return of(AggSpec.distinct(includeNulls), pairs);
    }

    static Aggregation AggFirst(String... pairs) {
        return of(AggSpec.first(), pairs);
    }

    static Aggregation AggGroup(String... pairs) {
        return of(AggSpec.group(), pairs);
    }

    static Aggregation AggLast(String... pairs) {
        return of(AggSpec.last(), pairs);
    }

    static Aggregation AggMax(String... pairs) {
        return of(AggSpec.max(), pairs);
    }

    static Aggregation AggMed(String... pairs) {
        return of(AggSpec.median(), pairs);
    }

    static Aggregation AggMed(boolean average, String... pairs) {
        return of(AggSpec.median(average), pairs);
    }

    static Aggregation AggMin(String... pairs) {
        return of(AggSpec.min(), pairs);
    }

    static Aggregation AggPct(double percentile, String... pairs) {
        return of(AggSpec.percentile(percentile), pairs);
    }

    static Aggregation AggPct(double percentile, boolean average, String... pairs) {
        return of(AggSpec.percentile(percentile, average), pairs);
    }

    static Aggregation AggSortedFirst(String sortedColumn, String... pairs) {
        return of(AggSpec.sortedFirst(sortedColumn), pairs);
    }

    static Aggregation AggSortedFirst(Collection<? extends String> sortedColumns, String... pairs) {
        return of(AggSpec.sortedFirst(sortedColumns), pairs);
    }

    static Aggregation AggSortedLast(String sortedColumn, String... pairs) {
        return of(AggSpec.sortedLast(sortedColumn), pairs);
    }

    static Aggregation AggSortedLast(Collection<? extends String> sortedColumns, String... pairs) {
        return of(AggSpec.sortedLast(sortedColumns), pairs);
    }

    static Aggregation AggStd(String... pairs) {
        return of(AggSpec.std(), pairs);
    }

    static Aggregation AggSum(String... pairs) {
        return of(AggSpec.sum(), pairs);
    }

    static Aggregation AggUnique(String... pairs) {
        return of(AggSpec.unique(), pairs);
    }

    static Aggregation AggUnique(boolean includeNulls, String... pairs) {
        return of(AggSpec.unique(includeNulls), pairs);
    }

    static Aggregation AggVar(String... pairs) {
        return of(AggSpec.var(), pairs);
    }

    static Aggregation AggWAvg(String weightColumn, String... pairs) {
        return of(AggSpec.wavg(weightColumn), pairs);
    }

    static Aggregation AggWSum(String weightColumn, String... pairs) {
        return of(AggSpec.wsum(weightColumn), pairs);
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(Count count);

        void visit(NormalAggregation normalAgg);

        void visit(NormalAggregations normalAggs);
    }
}

package io.deephaven.api.agg;

import io.deephaven.api.agg.KeyedAggregations.Builder;
import io.deephaven.api.agg.key.Key;

import java.io.Serializable;
import java.util.Collection;

/**
 * Represents an aggregation that can be applied to a table.
 *
 * @see io.deephaven.api.TableOperations#aggBy(Collection, Collection)
 * @see Count
 * @see KeyedAggregation
 * @see KeyedAggregations
 */
public interface Aggregation extends Serializable {

    static KeyedAggregation of(Key key, String pair) {
        return KeyedAggregation.of(key, Pair.parse(pair));
    }

    static Aggregation of(Key key, String... pairs) {
        if (pairs.length == 1) {
            return of(key, pairs[0]);
        }
        final Builder builder = KeyedAggregations.builder().key(key);
        for (String pair : pairs) {
            builder.addPairs(Pair.parse(pair));
        }
        return builder.build();
    }

    static Aggregation AggAbsSum(String... pairs) {
        return of(Key.absSum(), pairs);
    }

    static Aggregation AggAvg(String... pairs) {
        return of(Key.avg(), pairs);
    }

    static Aggregation AggCount(String resultColumn) {
        return Count.of(resultColumn);
    }

    static Aggregation AggCountDistinct(String... pairs) {
        return of(Key.countDistinct(), pairs);
    }

    static Aggregation AggCountDistinct(boolean countNulls, String... pairs) {
        return of(Key.countDistinct(countNulls), pairs);
    }

    static Aggregation AggDistinct(String... pairs) {
        return of(Key.distinct(), pairs);
    }

    static Aggregation AggDistinct(boolean includeNulls, String... pairs) {
        return of(Key.distinct(includeNulls), pairs);
    }

    static Aggregation AggFirst(String... pairs) {
        return of(Key.first(), pairs);
    }

    static Aggregation AggGroup(String... pairs) {
        return of(Key.group(), pairs);
    }

    static Aggregation AggLast(String... pairs) {
        return of(Key.last(), pairs);
    }

    static Aggregation AggMax(String... pairs) {
        return of(Key.max(), pairs);
    }

    static Aggregation AggMed(String... pairs) {
        return of(Key.median(), pairs);
    }

    static Aggregation AggMed(boolean average, String... pairs) {
        return of(Key.median(average), pairs);
    }

    static Aggregation AggMin(String... pairs) {
        return of(Key.min(), pairs);
    }

    static Aggregation AggPct(double percentile, String... pairs) {
        return of(Key.percentile(percentile), pairs);
    }

    static Aggregation AggPct(double percentile, boolean average, String... pairs) {
        return of(Key.percentile(percentile, average), pairs);
    }

    static Aggregation AggSortedFirst(String sortedColumn, String... pairs) {
        return of(Key.sortedFirst(sortedColumn), pairs);
    }

    static Aggregation AggSortedFirst(Collection<? extends String> sortedColumns, String... pairs) {
        return of(Key.sortedFirst(sortedColumns), pairs);
    }

    static Aggregation AggSortedLast(String sortedColumn, String... pairs) {
        return of(Key.sortedLast(sortedColumn), pairs);
    }

    static Aggregation AggSortedLast(Collection<? extends String> sortedColumns, String... pairs) {
        return of(Key.sortedLast(sortedColumns), pairs);
    }

    static Aggregation AggStd(String... pairs) {
        return of(Key.std(), pairs);
    }

    static Aggregation AggSum(String... pairs) {
        return of(Key.sum(), pairs);
    }

    static Aggregation AggUnique(String... pairs) {
        return of(Key.unique(), pairs);
    }

    static Aggregation AggUnique(boolean includeNulls, String... pairs) {
        return of(Key.unique(includeNulls), pairs);
    }

    static Aggregation AggVar(String... pairs) {
        return of(Key.var(), pairs);
    }

    static Aggregation AggWAvg(String weightColumn, String... pairs) {
        return of(Key.wavg(weightColumn), pairs);
    }

    static Aggregation AggWSum(String weightColumn, String... pairs) {
        return of(Key.wsum(weightColumn), pairs);
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(Count count);

        void visit(KeyedAggregation keyedAgg);

        void visit(KeyedAggregations keyedAggs);
    }
}

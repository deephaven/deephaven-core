package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.KeyedAggregations.Builder;
import io.deephaven.api.agg.key.Key;
import io.deephaven.api.agg.key.KeyAbsSum;
import io.deephaven.api.agg.key.KeyAvg;
import io.deephaven.api.agg.key.KeyCountDistinct;
import io.deephaven.api.agg.key.KeyDistinct;
import io.deephaven.api.agg.key.KeyFirst;
import io.deephaven.api.agg.key.KeyGroup;
import io.deephaven.api.agg.key.KeyLast;
import io.deephaven.api.agg.key.KeyMax;
import io.deephaven.api.agg.key.KeyMedian;
import io.deephaven.api.agg.key.KeyMin;
import io.deephaven.api.agg.key.KeyPct;
import io.deephaven.api.agg.key.KeySortedFirst;
import io.deephaven.api.agg.key.KeySortedLast;
import io.deephaven.api.agg.key.KeyStd;
import io.deephaven.api.agg.key.KeySum;
import io.deephaven.api.agg.key.KeyUnique;
import io.deephaven.api.agg.key.KeyVar;
import io.deephaven.api.agg.key.KeyWAvg;
import io.deephaven.api.agg.key.KeyWSum;

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

    static KeyedAggregations of(Key key, String... pairs) {
        final Builder builder = KeyedAggregations.builder().key(key);
        for (String pair : pairs) {
            builder.addPairs(Pair.parse(pair));
        }
        return builder.build();
    }

    static Aggregation AggAbsSum(String pair) {
        return of(KeyAbsSum.of(), pair);
    }

    static Aggregation AggAbsSum(String... pairs) {
        return of(KeyAbsSum.of(), pairs);
    }

    static Aggregation AggGroup(String pair) {
        return of(KeyGroup.of(), pair);
    }

    static Aggregation AggGroup(String... pairs) {
        return of(KeyGroup.of(), pairs);
    }

    static Aggregation AggAvg(String pair) {
        return of(KeyAvg.of(), pair);
    }

    static Aggregation AggAvg(String... pairs) {
        return of(KeyAvg.of(), pairs);
    }

    static Aggregation AggCount(String resultColumn) {
        return Count.of(resultColumn);
    }

    static Aggregation AggCountDistinct(String pair) {
        return of(KeyCountDistinct.of(), pair);
    }

    static Aggregation AggCountDistinct(String... pairs) {
        return of(KeyCountDistinct.of(), pairs);
    }

    static Aggregation AggCountDistinct(boolean countNulls, String pair) {
        return of(KeyCountDistinct.of(countNulls), pair);
    }

    static Aggregation AggCountDistinct(boolean countNulls, String... pairs) {
        return of(KeyCountDistinct.of(countNulls), pairs);
    }

    static Aggregation AggDistinct(String pair) {
        return of(KeyDistinct.of(), pair);
    }

    static Aggregation AggDistinct(String... pairs) {
        return of(KeyDistinct.of(), pairs);
    }

    static Aggregation AggDistinct(boolean includeNulls, String pair) {
        return of(KeyDistinct.of(includeNulls), pair);
    }

    static Aggregation AggDistinct(boolean includeNulls, String... pairs) {
        return of(KeyDistinct.of(includeNulls), pairs);
    }

    static Aggregation AggFirst(String pair) {
        return of(KeyFirst.of(), pair);
    }

    static Aggregation AggFirst(String... pairs) {
        return of(KeyFirst.of(), pairs);
    }

    static Aggregation AggLast(String pair) {
        return of(KeyLast.of(), pair);
    }

    static Aggregation AggLast(String... pairs) {
        return of(KeyLast.of(), pairs);
    }

    static Aggregation AggMax(String pair) {
        return of(KeyMax.of(), pair);
    }

    static Aggregation AggMax(String... pairs) {
        return of(KeyMax.of(), pairs);
    }

    static Aggregation AggMed(String pair) {
        return of(KeyMedian.of(), pair);
    }

    static Aggregation AggMed(String... pairs) {
        return of(KeyMedian.of(), pairs);
    }

    static Aggregation AggMed(boolean average, String pair) {
        return of(KeyMedian.of(average), pair);
    }

    static Aggregation AggMed(boolean average, String... pairs) {
        return of(KeyMedian.of(average), pairs);
    }

    static Aggregation AggMin(String pair) {
        return of(KeyMin.of(), pair);
    }

    static Aggregation AggMin(String... pairs) {
        return of(KeyMin.of(), pairs);
    }

    static Aggregation AggPct(double percentile, String pair) {
        return of(KeyPct.of(percentile), pair);
    }

    static Aggregation AggPct(double percentile, String... pairs) {
        return of(KeyPct.of(percentile), pairs);
    }

    static Aggregation AggPct(double percentile, boolean average, String pair) {
        return of(KeyPct.of(percentile, average), pair);
    }

    static Aggregation AggPct(double percentile, boolean average, String... pairs) {
        return of(KeyPct.of(percentile, average), pairs);
    }

    static Aggregation AggSortedFirst(String sortedColumn, String pair) {
        return of(KeySortedFirst.builder().addColumns(SortColumn.asc(ColumnName.of(sortedColumn))).build(), pair);
    }

    static Aggregation AggSortedFirst(String sortedColumn, String... pairs) {
        return of(KeySortedFirst.builder().addColumns(SortColumn.asc(ColumnName.of(sortedColumn))).build(), pairs);
    }

    static Aggregation AggSortedFirst(Collection<? extends String> sortedColumns, String pair) {
        final KeySortedFirst.Builder builder = KeySortedFirst.builder();
        for (String sortedColumn : sortedColumns) {
            builder.addColumns(SortColumn.asc(ColumnName.of(sortedColumn)));
        }
        return of(builder.build(), pair);
    }

    static Aggregation AggSortedFirst(Collection<? extends String> sortedColumns, String... pairs) {
        final KeySortedFirst.Builder builder = KeySortedFirst.builder();
        for (String sortedColumn : sortedColumns) {
            builder.addColumns(SortColumn.asc(ColumnName.of(sortedColumn)));
        }
        return of(builder.build(), pairs);
    }

    static Aggregation AggSortedLast(String sortedColumn, String pair) {
        return of(KeySortedLast.builder().addColumns(SortColumn.asc(ColumnName.of(sortedColumn))).build(), pair);
    }

    static Aggregation AggSortedLast(String sortedColumn, String... pairs) {
        return of(KeySortedLast.builder().addColumns(SortColumn.asc(ColumnName.of(sortedColumn))).build(), pairs);
    }

    static Aggregation AggSortedLast(Collection<? extends String> sortedColumns, String pair) {
        final KeySortedLast.Builder builder = KeySortedLast.builder();
        for (String sortedColumn : sortedColumns) {
            builder.addColumns(SortColumn.asc(ColumnName.of(sortedColumn)));
        }
        return of(builder.build(), pair);
    }

    static Aggregation AggSortedLast(Collection<? extends String> sortedColumns, String... pairs) {
        final KeySortedLast.Builder builder = KeySortedLast.builder();
        for (String sortedColumn : sortedColumns) {
            builder.addColumns(SortColumn.asc(ColumnName.of(sortedColumn)));
        }
        return of(builder.build(), pairs);
    }

    static Aggregation AggStd(String pair) {
        return of(KeyStd.of(), pair);
    }

    static Aggregation AggStd(String... pairs) {
        return of(KeyStd.of(), pairs);
    }

    static Aggregation AggSum(String pair) {
        return of(KeySum.of(), pair);
    }

    static Aggregation AggSum(String... pairs) {
        return of(KeySum.of(), pairs);
    }

    static Aggregation AggUnique(String pair) {
        return of(KeyUnique.of(), pair);
    }

    static Aggregation AggUnique(String... pairs) {
        return of(KeyUnique.of(), pairs);
    }

    static Aggregation AggUnique(boolean includeNulls, String pair) {
        return of(KeyUnique.of(includeNulls), pair);
    }

    static Aggregation AggUnique(boolean includeNulls, String... pairs) {
        return of(KeyUnique.of(includeNulls), pairs);
    }

    static Aggregation AggVar(String pair) {
        return of(KeyVar.of(), pair);
    }

    static Aggregation AggVar(String... pairs) {
        return of(KeyVar.of(), pairs);
    }

    static Aggregation AggWAvg(String weightColumn, String pair) {
        return of(KeyWAvg.of(ColumnName.of(weightColumn)), pair);
    }

    static Aggregation AggWAvg(String weightColumn, String... pairs) {
        return of(KeyWAvg.of(ColumnName.of(weightColumn)), pairs);
    }

    static Aggregation AggWSum(String weightColumn, String pair) {
        return of(KeyWSum.of(ColumnName.of(weightColumn)), pair);
    }

    static Aggregation AggWSum(String weightColumn, String... pairs) {
        return of(KeyWSum.of(ColumnName.of(weightColumn)), pairs);
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(Count count);

        void visit(KeyedAggregation keyedAgg);

        void visit(KeyedAggregations keyedAggs);
    }
}

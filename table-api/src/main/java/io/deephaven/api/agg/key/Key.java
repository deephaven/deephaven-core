package io.deephaven.api.agg.key;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.KeyedAggregation;
import io.deephaven.api.agg.Pair;

import java.util.Arrays;
import java.util.Collection;

/**
 * A key represents the configurable options for {@link KeyedAggregation}, except for the input/output {@link Pair}.
 */
public interface Key {

    static KeyAbsSum absSum() {
        return KeyAbsSum.of();
    }

    static KeyAvg avg() {
        return KeyAvg.of();
    }

    static KeyCountDistinct countDistinct() {
        return KeyCountDistinct.of();
    }

    static KeyCountDistinct countDistinct(boolean countNulls) {
        return KeyCountDistinct.of(countNulls);
    }

    static KeyDistinct distinct() {
        return KeyDistinct.of();
    }

    static KeyDistinct distinct(boolean includeNulls) {
        return KeyDistinct.of(includeNulls);
    }

    static KeyFirst first() {
        return KeyFirst.of();
    }

    static KeyGroup group() {
        return KeyGroup.of();
    }

    static KeyLast last() {
        return KeyLast.of();
    }

    static KeyMax max() {
        return KeyMax.of();
    }

    static KeyMedian median() {
        return KeyMedian.of();
    }

    static KeyMedian median(boolean averageMedian) {
        return KeyMedian.of(averageMedian);
    }

    static KeyMin min() {
        return KeyMin.of();
    }

    static KeyPct percentile(double percentile) {
        return KeyPct.of(percentile);
    }

    static KeyPct percentile(double percentile, boolean averageMedian) {
        return KeyPct.of(percentile, averageMedian);
    }

    static KeySortedFirst sortedFirst(String... columns) {
        return sortedFirst(Arrays.asList(columns));
    }

    static KeySortedFirst sortedFirst(Collection<? extends String> columns) {
        final KeySortedFirst.Builder builder = KeySortedFirst.builder();
        for (String column : columns) {
            builder.addColumns(SortColumn.asc(ColumnName.of(column)));
        }
        return builder.build();
    }

    static KeySortedLast sortedLast(String... columns) {
        return sortedLast(Arrays.asList(columns));
    }

    static KeySortedLast sortedLast(Collection<? extends String> columns) {
        final KeySortedLast.Builder builder = KeySortedLast.builder();
        for (String column : columns) {
            builder.addColumns(SortColumn.asc(ColumnName.of(column)));
        }
        return builder.build();
    }

    static KeyStd std() {
        return KeyStd.of();
    }

    static KeySum sum() {
        return KeySum.of();
    }

    static KeyUnique unique() {
        return KeyUnique.of();
    }

    static KeyUnique unique(boolean includeNulls) {
        return KeyUnique.of(includeNulls);
    }

    static KeyVar var() {
        return KeyVar.of();
    }

    static KeyWAvg wavg(String weightColumn) {
        return KeyWAvg.of(ColumnName.of(weightColumn));
    }

    static KeyWSum wsum(String weightColumn) {
        return KeyWSum.of(ColumnName.of(weightColumn));
    }

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

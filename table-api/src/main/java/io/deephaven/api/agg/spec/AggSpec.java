package io.deephaven.api.agg.spec;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.ColumnAggregation;
import io.deephaven.api.agg.Pair;

import java.util.Arrays;
import java.util.Collection;

/**
 * An aggregation specification represents the configurable options for column aggregations.
 */
public interface AggSpec {

    static AggSpecAbsSum absSum() {
        return AggSpecAbsSum.of();
    }

    static AggSpecApproximatePercentile approximatePercentile(double percentile) {
        return AggSpecApproximatePercentile.of(percentile);
    }

    static AggSpecApproximatePercentile approximatePercentile(double percentile, double compression) {
        return AggSpecApproximatePercentile.of(percentile, compression);
    }

    static AggSpecAvg avg() {
        return AggSpecAvg.of();
    }

    static AggSpecCountDistinct countDistinct() {
        return AggSpecCountDistinct.of();
    }

    static AggSpecCountDistinct countDistinct(boolean countNulls) {
        return AggSpecCountDistinct.of(countNulls);
    }

    static AggSpecDistinct distinct() {
        return AggSpecDistinct.of();
    }

    static AggSpecDistinct distinct(boolean includeNulls) {
        return AggSpecDistinct.of(includeNulls);
    }

    static AggSpecFirst first() {
        return AggSpecFirst.of();
    }

    static AggSpecFormula formula(String formula) {
        return AggSpecFormula.of(formula);
    }

    static AggSpecFormula formula(String formula, String formulaParam) {
        return AggSpecFormula.of(formula, formulaParam);
    }

    static AggSpecFreeze freeze() {
        return AggSpecFreeze.of();
    }

    static AggSpecGroup group() {
        return AggSpecGroup.of();
    }

    static AggSpecLast last() {
        return AggSpecLast.of();
    }

    static AggSpecMax max() {
        return AggSpecMax.of();
    }

    static AggSpecMedian median() {
        return AggSpecMedian.of();
    }

    static AggSpecMedian median(boolean averageMedian) {
        return AggSpecMedian.of(averageMedian);
    }

    static AggSpecMin min() {
        return AggSpecMin.of();
    }

    static AggSpecPercentile percentile(double percentile) {
        return AggSpecPercentile.of(percentile);
    }

    static AggSpecPercentile percentile(double percentile, boolean averageMedian) {
        return AggSpecPercentile.of(percentile, averageMedian);
    }

    static AggSpecSortedFirst sortedFirst(String... columns) {
        return sortedFirst(Arrays.asList(columns));
    }

    static AggSpecSortedFirst sortedFirst(Collection<? extends String> columns) {
        final AggSpecSortedFirst.Builder builder = AggSpecSortedFirst.builder();
        for (String column : columns) {
            builder.addColumns(SortColumn.asc(ColumnName.of(column)));
        }
        return builder.build();
    }

    static AggSpecSortedLast sortedLast(String... columns) {
        return sortedLast(Arrays.asList(columns));
    }

    static AggSpecSortedLast sortedLast(Collection<? extends String> columns) {
        final AggSpecSortedLast.Builder builder = AggSpecSortedLast.builder();
        for (String column : columns) {
            builder.addColumns(SortColumn.asc(ColumnName.of(column)));
        }
        return builder.build();
    }

    static AggSpecStd std() {
        return AggSpecStd.of();
    }

    static AggSpecSum sum() {
        return AggSpecSum.of();
    }

    static AggSpecTDigest tDigest() {
        return AggSpecTDigest.of();
    }

    static AggSpecTDigest tDigest(double compression) {
        return AggSpecTDigest.of(compression);
    }

    static AggSpecUnique unique() {
        return AggSpecUnique.of();
    }

    static AggSpecUnique unique(boolean includeNulls, Object nonUniqueSentinel) {
        return AggSpecUnique.of(includeNulls, nonUniqueSentinel);
    }

    static AggSpecVar var() {
        return AggSpecVar.of();
    }

    static AggSpecWAvg wavg(String weightColumn) {
        return AggSpecWAvg.of(ColumnName.of(weightColumn));
    }

    static AggSpecWSum wsum(String weightColumn) {
        return AggSpecWSum.of(ColumnName.of(weightColumn));
    }

    ColumnAggregation aggregation(Pair pair);

    Aggregation aggregation(Pair... pairs);

    Aggregation aggregation(Collection<? extends Pair> pairs);

    String description();

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(AggSpecAbsSum absSum);

        void visit(AggSpecApproximatePercentile approxPct);

        void visit(AggSpecCountDistinct countDistinct);

        void visit(AggSpecDistinct distinct);

        void visit(AggSpecFreeze freeze);

        void visit(AggSpecGroup group);

        void visit(AggSpecAvg avg);

        void visit(AggSpecFirst first);

        void visit(AggSpecFormula formula);

        void visit(AggSpecLast last);

        void visit(AggSpecMax max);

        void visit(AggSpecMedian median);

        void visit(AggSpecMin min);

        void visit(AggSpecPercentile pct);

        void visit(AggSpecSortedFirst sortedFirst);

        void visit(AggSpecSortedLast sortedLast);

        void visit(AggSpecStd std);

        void visit(AggSpecSum sum);

        void visit(AggSpecTDigest tDigest);

        void visit(AggSpecUnique unique);

        void visit(AggSpecWAvg wAvg);

        void visit(AggSpecWSum wSum);

        void visit(AggSpecVar var);
    }
}

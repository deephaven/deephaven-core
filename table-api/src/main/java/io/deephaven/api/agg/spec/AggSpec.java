package io.deephaven.api.agg.spec;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.ColumnAggregation;
import io.deephaven.api.agg.Pair;

import java.util.Arrays;
import java.util.Collection;

/**
 * An aggregation specification represents the configurable options for {@link ColumnAggregation singular} and
 * {@link io.deephaven.api.agg.ColumnAggregations compound} column aggregations.
 */
public interface AggSpec {

    /**
     * @return {@link AggSpecAbsSum#of()}
     */
    static AggSpecAbsSum absSum() {
        return AggSpecAbsSum.of();
    }

    /**
     * @return {@link AggSpecApproximatePercentile#of(double)} for {@code percentile}
     */
    static AggSpecApproximatePercentile approximatePercentile(double percentile) {
        return AggSpecApproximatePercentile.of(percentile);
    }

    /**
     * @return {@link AggSpecApproximatePercentile#of(double, double)} for {@code percentile} and {@code compression}
     */
    static AggSpecApproximatePercentile approximatePercentile(double percentile, double compression) {
        return AggSpecApproximatePercentile.of(percentile, compression);
    }

    /**
     * @return {@link AggSpecAvg#of()}
     */
    static AggSpecAvg avg() {
        return AggSpecAvg.of();
    }

    static AggSpecCountDistinct countDistinct() {
        return AggSpecCountDistinct.of();
    }

    /**
     * @return {@link AggSpecCountDistinct#of(boolean)} for {@code countNulls}
     */
    static AggSpecCountDistinct countDistinct(boolean countNulls) {
        return AggSpecCountDistinct.of(countNulls);
    }

    /**
     * @return {@link AggSpecDistinct#of()}
     */
    static AggSpecDistinct distinct() {
        return AggSpecDistinct.of();
    }

    /**
     * @return {@link AggSpecDistinct#of(boolean)} for {@code includeNulls}
     */
    static AggSpecDistinct distinct(boolean includeNulls) {
        return AggSpecDistinct.of(includeNulls);
    }

    /**
     * @return {@link AggSpecFirst#of()}
     */
    static AggSpecFirst first() {
        return AggSpecFirst.of();
    }

    /**
     * @return {@link AggSpecFormula#of(String)} for {@code formula}
     */
    static AggSpecFormula formula(String formula) {
        return AggSpecFormula.of(formula);
    }

    /**
     * @return {@link AggSpecFormula#of(String, String)} for {@code formula} and {@code paramToken}
     */
    static AggSpecFormula formula(String formula, String paramToken) {
        return AggSpecFormula.of(formula, paramToken);
    }

    /**
     * @return {@link AggSpecFreeze#of()}
     */
    static AggSpecFreeze freeze() {
        return AggSpecFreeze.of();
    }

    /**
     * @return {@link AggSpecGroup#of()}
     */
    static AggSpecGroup group() {
        return AggSpecGroup.of();
    }

    /**
     * @return {@link AggSpecLast#of()}
     */
    static AggSpecLast last() {
        return AggSpecLast.of();
    }

    /**
     * @return {@link AggSpecMax#of()}
     */
    static AggSpecMax max() {
        return AggSpecMax.of();
    }

    /**
     * @return {@link AggSpecMedian#of()}
     */
    static AggSpecMedian median() {
        return AggSpecMedian.of();
    }

    /**
     * @return {@link AggSpecMedian#of(boolean)} for {@code averageEvenlyDivided}
     */
    static AggSpecMedian median(boolean averageEvenlyDivided) {
        return AggSpecMedian.of(averageEvenlyDivided);
    }

    /**
     * @return {@link AggSpecMin#of()}
     */
    static AggSpecMin min() {
        return AggSpecMin.of();
    }

    /**
     * @return {@link AggSpecPercentile#of(double)} for {@code percentile}
     */
    static AggSpecPercentile percentile(double percentile) {
        return AggSpecPercentile.of(percentile);
    }

    /**
     * @return {@link AggSpecPercentile#of(double, boolean)} for {@code percentile} and {@code averageEvenlyDivided}
     */
    static AggSpecPercentile percentile(double percentile, boolean averageEvenlyDivided) {
        return AggSpecPercentile.of(percentile, averageEvenlyDivided);
    }

    /**
     * @return {@link AggSpecSortedFirst} for the supplied sort {@code columns}
     */
    static AggSpecSortedFirst sortedFirst(String... columns) {
        return sortedFirst(Arrays.asList(columns));
    }

    /**
     * @return {@link AggSpecSortedFirst} for the supplied sort {@code columns}
     */
    static AggSpecSortedFirst sortedFirst(Collection<? extends String> columns) {
        final AggSpecSortedFirst.Builder builder = AggSpecSortedFirst.builder();
        for (String column : columns) {
            builder.addColumns(SortColumn.asc(ColumnName.of(column)));
        }
        return builder.build();
    }

    /**
     * @return {@link AggSpecSortedLast} for the supplied sort {@code columns}
     */
    static AggSpecSortedLast sortedLast(String... columns) {
        return sortedLast(Arrays.asList(columns));
    }

    /**
     * @return {@link AggSpecSortedLast} for the supplied sort {@code columns}
     */
    static AggSpecSortedLast sortedLast(Collection<? extends String> columns) {
        final AggSpecSortedLast.Builder builder = AggSpecSortedLast.builder();
        for (String column : columns) {
            builder.addColumns(SortColumn.asc(ColumnName.of(column)));
        }
        return builder.build();
    }

    /**
     * @return {@link AggSpecStd#of()}
     */
    static AggSpecStd std() {
        return AggSpecStd.of();
    }

    /**
     * @return {@link AggSpecSum#of()}
     */
    static AggSpecSum sum() {
        return AggSpecSum.of();
    }

    /**
     * @return {@link AggSpecTDigest#of()}
     */
    static AggSpecTDigest tDigest() {
        return AggSpecTDigest.of();
    }

    /**
     * @return {@link AggSpecTDigest#of(double)} for {@code compression}
     */
    static AggSpecTDigest tDigest(double compression) {
        return AggSpecTDigest.of(compression);
    }

    /**
     * @return {@link AggSpecUnique#of()}
     */
    static AggSpecUnique unique() {
        return AggSpecUnique.of();
    }

    /**
     * @return {@link AggSpecUnique#of(boolean, Object)} for {@code includeNulls} and {@code nonUniqueSentinel}
     */
    static AggSpecUnique unique(boolean includeNulls, Object nonUniqueSentinel) {
        return AggSpecUnique.of(includeNulls, nonUniqueSentinel);
    }

    /**
     * @return {@link AggSpecVar#of()}
     */
    static AggSpecVar var() {
        return AggSpecVar.of();
    }

    /**
     * @return {@link AggSpecWAvg#of(ColumnName)} for the supplied {@code weightColumn}
     */
    static AggSpecWAvg wavg(String weightColumn) {
        return AggSpecWAvg.of(ColumnName.of(weightColumn));
    }

    /**
     * @return {@link AggSpecWSum#of(ColumnName)} for the supplied {@code weightColumn}
     */
    static AggSpecWSum wsum(String weightColumn) {
        return AggSpecWSum.of(ColumnName.of(weightColumn));
    }

    /**
     * Build a {@link ColumnAggregation} for this AggSpec.
     *
     * @param pair The input/output column name pair
     * @return The aggregation
     */
    ColumnAggregation aggregation(Pair pair);

    /**
     * Build a {@link ColumnAggregation singular} or {@link io.deephaven.api.agg.ColumnAggregations compound}
     * aggregation for this AggSpec.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    Aggregation aggregation(Pair... pairs);

    /**
     * Build a {@link ColumnAggregation singular} or {@link io.deephaven.api.agg.ColumnAggregations compound}
     * aggregation for this AggSpec.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    Aggregation aggregation(Collection<? extends Pair> pairs);

    /**
     * Get a simple description for this AggSpec.
     *
     * @return The description
     */
    String description();

    /**
     * Glue method to deliver this AggSpec to a {@link Visitor}.
     *
     * @param visitor The visitor
     * @return The visitor
     */
    <V extends Visitor> V walk(V visitor);

    /*
     * Visitor interface. Combines with {@link #walk(Visitor) in order to allow for type-safe AggSpec evaluation without
     * switch statements or if-else blocks.
     */
    interface Visitor {
        void visit(AggSpecAbsSum absSum);

        void visit(AggSpecApproximatePercentile approxPct);

        void visit(AggSpecAvg avg);

        void visit(AggSpecCountDistinct countDistinct);

        void visit(AggSpecDistinct distinct);

        void visit(AggSpecFirst first);

        void visit(AggSpecFormula formula);

        void visit(AggSpecFreeze freeze);

        void visit(AggSpecGroup group);

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

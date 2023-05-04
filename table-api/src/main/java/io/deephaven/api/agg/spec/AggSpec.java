/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.api.agg.spec;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.ColumnAggregation;
import io.deephaven.api.agg.Pair;
import io.deephaven.api.object.UnionObject;

import java.util.Arrays;
import java.util.Collection;

/**
 * An aggregation specification represents the configurable options for {@link ColumnAggregation singular} and
 * {@link io.deephaven.api.agg.ColumnAggregations compound} column aggregations.
 */
public interface AggSpec {

    /**
     * Produces a single-row table with the absolute some of each column.
     *
     * @return {@link AggSpecAbsSum#of()}
     */
    static AggSpecAbsSum absSum() {
        return AggSpecAbsSum.of();
    }

    /**
     * Computes the approximate percentiles for the table.
     *
     * @param percentile the percentile to compute for each column
     * @return {@link AggSpecApproximatePercentile#of(double)} for {@code percentile}
     */
    static AggSpecApproximatePercentile approximatePercentile(double percentile) {
        return AggSpecApproximatePercentile.of(percentile);
    }

    /**
     * Computes the approximate percentiles for the table.
     *
     * @param percentile the percentile to compute for each column
     * @param compression the t-digest compression parameter
     * @return {@link AggSpecApproximatePercentile#of(double, double)} for {@code percentile} and {@code compression}
     */
    static AggSpecApproximatePercentile approximatePercentile(double percentile, double compression) {
        return AggSpecApproximatePercentile.of(percentile, compression);
    }

    /**
     * Returns the average of values in each group.
     *
     * @return {@link AggSpecAvg#of()}
     */
    static AggSpecAvg avg() {
        return AggSpecAvg.of();
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @return the number of distinct values
     */
    static AggSpecCountDistinct countDistinct() {
        return AggSpecCountDistinct.of();
    }

    /**
     * Counts the number of distinct elements in the array.
     *
     * @param countNulls whether to count null values. true includes, false excludes
     * @return {@link AggSpecCountDistinct#of(boolean)} for {@code countNulls}
     */
    static AggSpecCountDistinct countDistinct(boolean countNulls) {
        return AggSpecCountDistinct.of(countNulls);
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @return {@link AggSpecDistinct#of()}
     */
    static AggSpecDistinct distinct() {
        return AggSpecDistinct.of();
    }

    /**
     * Returns an array containing only the distinct values from the input.
     *
     * @param includeNulls whether to include null values. True includes, false excludes
     * @return {@link AggSpecDistinct#of(boolean)} for {@code includeNulls}
     */
    static AggSpecDistinct distinct(boolean includeNulls) {
        return AggSpecDistinct.of(includeNulls);
    }

    /**
     * Returns the first value in the input column for each group.
     *
     * @return {@link AggSpecFirst#of()}
     */
    static AggSpecFirst first() {
        return AggSpecFirst.of();
    }

    /**
     * Specifies an aggregation that applies a formula to each input group to produce the corresponding output value.
     *
     * @param formula the formula to use to calculate output values from grouped input values
     * @return {@link AggSpecFormula#of(String)} for {@code formula}
     */
    static AggSpecFormula formula(String formula) {
        return AggSpecFormula.of(formula);
    }

    /**
     * Specifies an aggregation that applies a formula to each input group to produce the corresponding output value.
     * Each input column name is substituted for the param token for evaluation.
     *
     * @param formula the formula to use to calculate output values from grouped input values
     * @return {@link AggSpecFormula#of(String, String)} for {@code formula} and {@code paramToken}
     */
    static AggSpecFormula formula(String formula, String paramToken) {
        return AggSpecFormula.of(formula, paramToken);
    }

    /**
     * Specifies an aggregation that freezes the first value for each group and ignores subsequent changes. When groups
     * are removed, the corresponding output row is removedd. When groups are re-added (on a subsequent update cycle),
     * the newly added value is then frozen.
     *
     * @return {@link AggSpecFreeze#of()}
     */
    static AggSpecFreeze freeze() {
        return AggSpecFreeze.of();
    }

    /**
     * Specifies an aggregation that outputs each group of input values as a Deephaven vector
     * (io.deephaven.vector.Vector).
     *
     * @return {@link AggSpecGroup#of()}
     */
    static AggSpecGroup group() {
        return AggSpecGroup.of();
    }

    /**
     * Specifies an aggregation that outputs the last value in the input column for each group.
     *
     * @return {@link AggSpecLast#of()}
     */
    static AggSpecLast last() {
        return AggSpecLast.of();
    }

    /**
     * Specifies an aggregation that outputs the maximum value in the input column for each group. Only works for
     * numeric or Comparable input types.
     *
     * @return {@link AggSpecMax#of()}
     */
    static AggSpecMax max() {
        return AggSpecMax.of();
    }

    /**
     * Specifier for a column aggregation that produces a median value from the input column's values for each group.
     * Only works for numeric or Comparable input types.
     *
     * @return {@link AggSpecMedian#of()}
     */
    static AggSpecMedian median() {
        return AggSpecMedian.of();
    }

    /**
     * Specifier for a column aggregation that produces a median value from the input column's values for each group.
     * Only works for numeric or Comparable input types.
     *
     * @param averageEvenlyDivided Whether to average the highest low-bucket value and lowest high-bucket value, when
     *        the low-bucket and high-bucket are of equal size. Only applies to numeric types.
     * @return {@link AggSpecMedian#of(boolean)} for {@code averageEvenlyDivided}
     */
    static AggSpecMedian median(boolean averageEvenlyDivided) {
        return AggSpecMedian.of(averageEvenlyDivided);
    }

    /**
     * Specifies an aggregation that outputs the minimum value in the input column for each group. Only works for
     * numeric or Comparable input types.
     *
     * @return {@link AggSpecMin#of()}
     */
    static AggSpecMin min() {
        return AggSpecMin.of();
    }

    /**
     * Specifier for a column aggregation that produces a percentile value from the input column's values for each
     * group. Only works for numeric or Comparable input types.
     *
     * @param percentile the percentile to calculate. Must be >= 0.0 and <= 1.0.
     * @return {@link AggSpecPercentile#of(double)} for {@code percentile}
     */
    static AggSpecPercentile percentile(double percentile) {
        return AggSpecPercentile.of(percentile);
    }

    /**
     * Specifier for a column aggregation that produces a percentile value from the input column's values for each
     * group. Only works for numeric or Comparable input types.
     *
     * @param percentile the percentile to calculate. Must be >= 0.0 and <= 1.0.
     * @param averageEvenlyDivided Whether to average the highest low-bucket value and lowest high-bucket value, when
     *        the low-bucket and high-bucket are of equal size. Only applies to numeric types.
     * @return {@link AggSpecPercentile#of(double, boolean)} for {@code percentile} and {@code averageEvenlyDivided}
     */
    static AggSpecPercentile percentile(double percentile, boolean averageEvenlyDivided) {
        return AggSpecPercentile.of(percentile, averageEvenlyDivided);
    }

    /**
     * Specifies an aggregation that outputs the first value in the input column for each group, after sorting the group
     * on the sort columns.
     *
     * @param columns the columns to sort on to determine the order within each group
     * @return {@link AggSpecSortedFirst} for the supplied sort {@code columns}
     */
    static AggSpecSortedFirst sortedFirst(String... columns) {
        return sortedFirst(Arrays.asList(columns));
    }

    /**
     * Specifies an aggregation that outputs the first value in the input column for each group, after sorting the group
     * on the sort columns.
     *
     * @param columns the columns to sort on to determine the order within each group
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
     * Specifies an aggregation that outputs the last value in the input column for each group, after sorting the group
     * on the sort columns.
     *
     * @param columns the columns to sort on to determine the order within each group
     * @return {@link AggSpecSortedLast} for the supplied sort {@code columns}
     */
    static AggSpecSortedLast sortedLast(String... columns) {
        return sortedLast(Arrays.asList(columns));
    }

    /**
     * Specifies an aggregation that outputs the last value in the input column for each group, after sorting the group
     * on the sort columns.
     *
     * @param columns the columns to sort on to determine the order within each group
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
     * Specifies an aggregation that outputs the standard deviation of the input column values for each group. Only
     * works for numeric input types.
     *
     * @return {@link AggSpecStd#of()}
     */
    static AggSpecStd std() {
        return AggSpecStd.of();
    }

    /**
     * Specifies an aggregation that outputs the sum of input values for each group. Only works with numeric input types
     * and Boolean.
     *
     * @return {@link AggSpecSum#of()}
     */
    static AggSpecSum sum() {
        return AggSpecSum.of();
    }

    /**
     * Specifies an aggregation that outputs a T-Digest (com.tdunning.math.stats.TDigest). May be used to implement
     * parallel percentile calculations by splitting inputs and accumulating results into a single downstream TDigest.
     * May only be used on static or add-only tables.
     *
     * @return {@link AggSpecTDigest#of()}
     */
    static AggSpecTDigest tDigest() {
        return AggSpecTDigest.of();
    }

    /**
     * Specifies an aggregation that outputs a T-Digest (com.tdunning.math.stats.TDigest) with the specified
     * compression. May be used to implement parallel percentile calculations by splitting inputs and accumulating
     * results into a single downstream TDigest. May only be used on static or add-only tables.
     *
     * @param compression T-Digest compression factor. Must be greater than or equal to 1. 1000 is extremely large. When
     *        not specified, the server will choose a compression value.
     * @return {@link AggSpecTDigest#of(double)} for {@code compression}
     */
    static AggSpecTDigest tDigest(double compression) {
        return AggSpecTDigest.of(compression);
    }

    /**
     * Create a unique aggregation for the supplied column name pairs. This will not consider null values when
     * determining if a group has a single unique value. Non-unique groups will have null values in the output column.
     *
     * @return {@link AggSpecUnique#of()}
     */
    static AggSpecUnique unique() {
        return AggSpecUnique.of();
    }

    /**
     * Create a unique aggregation for the supplied column name pairs. This will not consider null values when
     * determining if a group has a single unique value. Non-unique groups will have null values in the output column.
     *
     * @param includeNulls whether to consider null values toward uniqueness
     * @param nonUniqueSentinel the value to output for non-unique groups
     * @return {@link AggSpecUnique#of(boolean, Object)} for {@code includeNulls} and {@code nonUniqueSentinel}
     */
    static AggSpecUnique unique(boolean includeNulls, Object nonUniqueSentinel) {
        return AggSpecUnique.of(includeNulls, nonUniqueSentinel);
    }

    /**
     * Create a unique aggregation for the supplied column name pairs. This will not consider null values when
     * determining if a group has a single unique value. Non-unique groups will have null values in the output column.
     *
     * @param includeNulls whether to consider null values toward uniqueness
     * @param nonUniqueSentinel the value to output for non-unique groups
     * @return {@link AggSpecUnique#of(boolean, UnionObject)} for {@code includeNulls} and {@code nonUniqueSentinel}
     */
    static AggSpecUnique unique(boolean includeNulls, UnionObject nonUniqueSentinel) {
        return AggSpecUnique.of(includeNulls, nonUniqueSentinel);
    }

    /**
     * Create a variance aggregation for the supplied column name pairs.
     *
     * @return {@link AggSpecVar#of()}
     */
    static AggSpecVar var() {
        return AggSpecVar.of();
    }

    /**
     * Create a weighted average aggregation for the supplied weight column name and column name pairs.
     *
     * @param weightColumn the weight column name
     * @return {@link AggSpecWAvg#of(ColumnName)} for the supplied {@code weightColumn}
     */
    static AggSpecWAvg wavg(String weightColumn) {
        return AggSpecWAvg.of(ColumnName.of(weightColumn));
    }

    /**
     * Create a weighted sum aggregation for the supplied weight column name and column name pairs.
     *
     * @param weightColumn the weight column name
     * @return {@link AggSpecWSum#of(ColumnName)} for the supplied {@code weightColumn}
     */
    static AggSpecWSum wsum(String weightColumn) {
        return AggSpecWSum.of(ColumnName.of(weightColumn));
    }

    /**
     * Calls every single visit method of {@code visitor} with a {@code null} object.
     *
     * @param visitor the visitor
     */
    static void visitAll(Visitor visitor) {
        visitor.visit((AggSpecAbsSum) null);
        visitor.visit((AggSpecApproximatePercentile) null);
        visitor.visit((AggSpecAvg) null);
        visitor.visit((AggSpecCountDistinct) null);
        visitor.visit((AggSpecDistinct) null);
        visitor.visit((AggSpecFirst) null);
        visitor.visit((AggSpecFormula) null);
        visitor.visit((AggSpecFreeze) null);
        visitor.visit((AggSpecGroup) null);
        visitor.visit((AggSpecLast) null);
        visitor.visit((AggSpecMax) null);
        visitor.visit((AggSpecMedian) null);
        visitor.visit((AggSpecMin) null);
        visitor.visit((AggSpecPercentile) null);
        visitor.visit((AggSpecSortedFirst) null);
        visitor.visit((AggSpecSortedLast) null);
        visitor.visit((AggSpecStd) null);
        visitor.visit((AggSpecSum) null);
        visitor.visit((AggSpecTDigest) null);
        visitor.visit((AggSpecUnique) null);
        visitor.visit((AggSpecWAvg) null);
        visitor.visit((AggSpecWSum) null);
        visitor.visit((AggSpecVar) null);
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

package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.agg.util.PercentileOutput;
import io.deephaven.api.agg.util.Sentinel;

import java.io.Serializable;
import java.util.Collection;
import java.util.function.BiFunction;

/**
 * Represents an aggregation that can be applied to a table.
 *
 * @see io.deephaven.api.TableOperations#aggBy
 * @see io.deephaven.api.TableOperations#aggAllBy
 * @see Aggregations
 * @see ColumnAggregation
 * @see ColumnAggregations
 * @see Count
 * @see FirstRowKey
 * @see LastRowKey
 */
public interface Aggregation extends Serializable {

    /**
     * Combine an {@link AggSpec} and a {@link Pair column name pair} into a {@link ColumnAggregation}.
     *
     * @param spec The aggregation specifier to apply to the column pair
     * @param pair The input/output column name pair
     * @return The aggregation
     */
    static ColumnAggregation of(AggSpec spec, String pair) {
        return ColumnAggregation.of(spec, Pair.parse(pair));
    }

    /**
     * Combine an {@link AggSpec} and one or more {@link Pair column name pairs} into a {@link ColumnAggregation} or
     * {@link ColumnAggregations}.
     *
     * @param spec The aggregation specifier to apply to the column pair
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation of(AggSpec spec, String... pairs) {
        if (pairs.length == 1) {
            return of(spec, pairs[0]);
        }
        final ColumnAggregations.Builder builder = ColumnAggregations.builder().spec(spec);
        for (String pair : pairs) {
            builder.addPairs(Pair.parse(pair));
        }
        return builder.build();
    }

    /**
     * Pass through a single {@link Aggregation}, or combine many into an {@link Aggregations}.
     *
     * @param aggregations The aggregations to combine
     * @return The combined aggregation
     */
    static Aggregation of(Aggregation... aggregations) {
        if (aggregations.length == 1) {
            return aggregations[0];
        }
        return Aggregations.builder().addAggregations(aggregations).build();
    }

    /**
     * Create a single or compound {@link Aggregation} from a single input column and one or more per-aggregation input
     * values.
     *
     * @param columnAggFactory A factory for combining an input column and input value into an aggregation
     * @param inputColumn The input column for each component of the resulting aggregation
     * @param inputs The input values to combine with the input column via the factory
     * @return The combined aggregation
     */
    @SafeVarargs
    static <INPUT_TYPE> Aggregation of(BiFunction<ColumnName, INPUT_TYPE, ColumnAggregation> columnAggFactory,
            String inputColumn, INPUT_TYPE... inputs) {
        final ColumnName inputColumnName = ColumnName.of(inputColumn);
        if (inputs.length == 1) {
            return columnAggFactory.apply(inputColumnName, inputs[0]);
        }
        final Aggregations.Builder builder = Aggregations.builder();
        for (INPUT_TYPE input : inputs) {
            builder.addAggregations(columnAggFactory.apply(inputColumnName, input));
        }
        return builder.build();
    }

    /**
     * Create an {@link io.deephaven.api.agg.spec.AggSpecAbsSum absolute sum} aggregation for the supplied column name
     * pairs.
     *
     * @param pairs The input/output column name pairs
     * @return THe aggregation
     */
    static Aggregation AggAbsSum(String... pairs) {
        return of(AggSpec.absSum(), pairs);
    }

    /**
     * Create an {@link io.deephaven.api.agg.spec.AggSpecApproximatePercentile approximate percentile} aggregation for
     * the supplied percentile and column name pairs with the default T-Digest compression factor.
     *
     * @param percentile The percentile to use for all component aggregations
     * @param pairs The input/output column name pairs
     * @return THe aggregation
     */
    static Aggregation AggApproxPct(double percentile, String... pairs) {
        return of(AggSpec.approximatePercentile(percentile), pairs);
    }

    /**
     * Create an {@link io.deephaven.api.agg.spec.AggSpecApproximatePercentile approximate percentile} aggregation for
     * the supplied percentile, T-Digest compression factor, and column name pairs.
     *
     * @param percentile The percentile to use for all component aggregations
     * @param compression T-Digest compression factor; must be &gt; 1, should probably be &lt; 1000
     * @param pairs The input/output column name pairs
     * @return THe aggregation
     */
    static Aggregation AggApproxPct(double percentile, double compression, String... pairs) {
        return of(AggSpec.approximatePercentile(percentile, compression), pairs);
    }

    /**
     * Create an {@link io.deephaven.api.agg.spec.AggSpecApproximatePercentile approximate percentile} aggregation for
     * the supplied input column name and percentile/output column name pairs with the default T-Digest compression
     * factor.
     *
     * @param inputColumn The input column name
     * @param percentileOutputs The percentile/output column name pairs for the component aggregations
     * @return THe aggregation
     */
    static Aggregation AggApproxPct(String inputColumn, PercentileOutput... percentileOutputs) {
        final BiFunction<ColumnName, PercentileOutput, ColumnAggregation> aggFactory = (ic, po) -> ColumnAggregation
                .of(AggSpec.approximatePercentile(po.percentile()), Pair.of(ic, po.output()));
        return of(aggFactory, inputColumn, percentileOutputs);
    }

    /**
     * Create an {@link io.deephaven.api.agg.spec.AggSpecApproximatePercentile approximate percentile} aggregation for
     * the supplied input column name, T-Digest compression factor, and percentile/output column name pairs.
     *
     * @param inputColumn The input column name
     * @param compression T-Digest compression factor; must be &gt; 1, should probably be &lt; 1000
     * @param percentileOutputs The percentile/output column name pairs for the component aggregations
     * @return THe aggregation
     */
    static Aggregation AggApproxPct(String inputColumn, double compression, PercentileOutput... percentileOutputs) {
        final BiFunction<ColumnName, PercentileOutput, ColumnAggregation> aggFactory =
                (ic, po) -> ColumnAggregation.of(AggSpec.approximatePercentile(po.percentile(), compression),
                        Pair.of(ic, po.output()));
        return of(aggFactory, inputColumn, percentileOutputs);
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

    static Aggregation AggFirstRowKey(String resultColumn) {
        return FirstRowKey.of(resultColumn);
    }

    static Aggregation AggFormula(String formula, String formulaParam, String... pairs) {
        return of(AggSpec.formula(formula, formulaParam), pairs);
    }

    static Aggregation AggFreeze(String... pairs) {
        return of(AggSpec.freeze(), pairs);
    }

    static Aggregation AggGroup(String... pairs) {
        return of(AggSpec.group(), pairs);
    }

    static Aggregation AggLast(String... pairs) {
        return of(AggSpec.last(), pairs);
    }

    static Aggregation AggLastRowKey(String resultColumn) {
        return LastRowKey.of(resultColumn);
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

    static Aggregation AggPct(String inputColumn, PercentileOutput... percentileOutputs) {
        final BiFunction<ColumnName, PercentileOutput, ColumnAggregation> aggFactory =
                (ic, po) -> ColumnAggregation.of(AggSpec.percentile(po.percentile()), Pair.of(ic, po.output()));
        return of(aggFactory, inputColumn, percentileOutputs);
    }

    static Aggregation AggPct(String inputColumn, boolean average, PercentileOutput... percentileOutputs) {
        final BiFunction<ColumnName, PercentileOutput, ColumnAggregation> aggFactory = (ic, po) -> ColumnAggregation
                .of(AggSpec.percentile(po.percentile(), average), Pair.of(ic, po.output()));
        return of(aggFactory, inputColumn, percentileOutputs);
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

    static Aggregation AggTDigest(String... pairs) {
        return of(AggSpec.tDigest(), pairs);
    }

    static Aggregation AggTDigest(double compression, String... pairs) {
        return of(AggSpec.tDigest(compression), pairs);
    }

    static Aggregation AggUnique(String... pairs) {
        return of(AggSpec.unique(), pairs);
    }

    static Aggregation AggUnique(boolean includeNulls, String... pairs) {
        return AggUnique(includeNulls, Sentinel.of(), pairs);
    }

    static Aggregation AggUnique(boolean includeNulls, Sentinel nonUniqueSentinel, String... pairs) {
        return of(AggSpec.unique(includeNulls, nonUniqueSentinel.value()), pairs);
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

    static PercentileOutput PctOut(double percentile, String outputColumn) {
        return PercentileOutput.of(percentile, outputColumn);
    }

    /**
     * Make a {@link Sentinel sentinel} wrapping {@code value}. This serves to avoid ambiguity in the var-args overloads
     * of some Aggregation factory methods.
     *
     * @param value The value to wrap
     * @return The sentinel
     * @see #AggUnique(boolean, Sentinel, String...)
     */
    static Sentinel Sentinel(Object value) {
        return Sentinel.of(value);
    }

    /**
     * Make a {@link Sentinel sentinel} wrapping {@code null}. This serves to avoid ambiguity in the var-args overloads
     * of some Aggregation factory methods.
     * 
     * @return The sentinel
     * @see #AggUnique(boolean, Sentinel, String...)
     */
    static Sentinel Sentinel() {
        return Sentinel.of();
    }

    /**
     * Glue method to deliver this Aggregation to a visitor.
     *
     * @param visitor The visitor
     * @return The visitor
     */
    <V extends Visitor> V walk(V visitor);

    /**
     * Visitor interface. Combines with {@link #walk(Visitor)} in order to allow for type-safe Aggregation evaluation
     * without switch statements or if-else blocks.
     */
    interface Visitor {

        /**
         * Visit a {@link Aggregations compound aggregation}.
         *
         * @param aggregations The compound aggregation to visit
         */
        void visit(Aggregations aggregations);

        /**
         * Visit a {@link ColumnAggregation column aggregation}.
         *
         * @param columnAgg The column aggregation to visit
         */
        void visit(ColumnAggregation columnAgg);

        /**
         * Visit a {@link ColumnAggregations compound column aggregation}.
         *
         * @param columnAggs The compound column aggregation to visit
         */
        void visit(ColumnAggregations columnAggs);

        /**
         * Visit a {@link Count count aggregation}.
         *
         * @param count The count aggregation
         */
        void visit(Count count);

        /**
         * Visit a {@link FirstRowKey first row key aggregation}.
         *
         * @param firstRowKey The first row key aggregation
         */
        void visit(FirstRowKey firstRowKey);

        /**
         * Visit a {@link LastRowKey last row key aggregation}.
         *
         * @param lastRowKey The last row key aggregation
         */
        void visit(LastRowKey lastRowKey);
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.Pair;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.agg.spec.AggSpecApproximatePercentile;
import io.deephaven.api.agg.spec.AggSpecCountDistinct;
import io.deephaven.api.agg.spec.AggSpecDistinct;
import io.deephaven.api.agg.spec.AggSpecFormula;
import io.deephaven.api.agg.spec.AggSpecMedian;
import io.deephaven.api.agg.spec.AggSpecPercentile;
import io.deephaven.api.agg.spec.AggSpecTDigest;
import io.deephaven.api.agg.spec.AggSpecUnique;
import io.deephaven.api.agg.spec.AggSpecWAvg;
import io.deephaven.api.agg.spec.AggSpecWSum;
import io.deephaven.api.agg.util.PercentileOutput;
import io.deephaven.api.filter.Filter;
import io.deephaven.api.object.UnionObject;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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
public interface Aggregation {

    /**
     * Combine an {@link AggSpec} and an input/output {@link Pair column name pair} into a {@link ColumnAggregation}.
     *
     * @param spec The {@link ColumnAggregation#spec() aggregation specifier} to apply to the column name pair
     * @param pair The {@link ColumnAggregation#pair() input/output column name pair}
     * @return The aggregation
     */
    static ColumnAggregation of(AggSpec spec, String pair) {
        return ColumnAggregation.of(spec, Pair.parse(pair));
    }

    /**
     * Combine an {@link AggSpec} and one or more input/output {@link Pair column name pairs} into a
     * {@link ColumnAggregation} or {@link ColumnAggregations}.
     *
     * @param spec The {@link ColumnAggregation#spec() aggregation specifier} to apply to the column name pair(s)
     * @param pairs The input/output column name {@link ColumnAggregation#pair() pair} or
     *        {@link ColumnAggregations#pairs() pairs}
     * @return The aggregation
     */
    static Aggregation of(AggSpec spec, String... pairs) {
        return of(spec, Arrays.asList(pairs));
    }

    /**
     * Combine an {@link AggSpec} and one or more input/output {@link Pair column name pairs} into a
     * {@link ColumnAggregation} or {@link ColumnAggregations}.
     *
     * @param spec The {@link ColumnAggregation#spec() aggregation specifier} to apply to the column name pair(s)
     * @param pairs The input/output column name {@link ColumnAggregation#pair() pair} or
     *        {@link ColumnAggregations#pairs() pairs}
     * @return The aggregation
     */
    static Aggregation of(AggSpec spec, List<String> pairs) {
        if (pairs.isEmpty()) {
            throw new IllegalArgumentException(
                    "Must have at least one pair to create an Aggregation from an AggSpec. Did you mean to use TableOperations#aggAllBy?");
        }
        if (pairs.size() == 1) {
            return of(spec, pairs.get(0));
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
     * @param aggregations The {@link Aggregations#aggregations() aggregations} to combine
     * @return The combined aggregation
     */
    static Aggregation of(Aggregation... aggregations) {
        if (aggregations.length == 0) {
            throw new IllegalArgumentException("Unable to create an empty aggregation.");
        }
        if (aggregations.length == 1) {
            return aggregations[0];
        }
        return Aggregations.builder().addAggregations(aggregations).build();
    }

    /**
     * Create a single or compound {@link Aggregation} from a single input column and one or more per-aggregation input
     * values.
     *
     * @param columnAggFactory A factory for combining an input column and input value into a {@link ColumnAggregation
     *        aggregation}
     * @param inputColumn The input column for each component of the resulting aggregation
     * @param inputs The input values to combine with the input column via the factory
     * @return The combined aggregation
     */
    @SafeVarargs
    static <INPUT_TYPE> Aggregation of(BiFunction<ColumnName, INPUT_TYPE, ColumnAggregation> columnAggFactory,
            String inputColumn, INPUT_TYPE... inputs) {
        if (inputs.length == 0) {
            throw new IllegalArgumentException("Unable to create an empty aggregation.");
        }
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
     * @return The aggregation
     */
    static Aggregation AggAbsSum(String... pairs) {
        return of(AggSpec.absSum(), pairs);
    }

    /**
     * Create an {@link io.deephaven.api.agg.spec.AggSpecApproximatePercentile approximate percentile} aggregation for
     * the supplied percentile and column name pairs with the default T-Digest
     * {@link AggSpecApproximatePercentile#compression() compression} factor.
     *
     * @param percentile The {@link AggSpecApproximatePercentile#percentile() percentile} to use for all component
     *        aggregations
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggApproxPct(double percentile, String... pairs) {
        return of(AggSpec.approximatePercentile(percentile), pairs);
    }

    /**
     * Create an {@link io.deephaven.api.agg.spec.AggSpecApproximatePercentile approximate percentile} aggregation for
     * the supplied percentile, T-Digest compression factor, and column name pairs.
     *
     * @param percentile The {@link AggSpecApproximatePercentile#percentile() percentile} to use for all component
     *        aggregations
     * @param compression T-Digest {@link AggSpecTDigest#compression() compression} factor; must be &gt; 1, should
     *        probably be &lt; 1000
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggApproxPct(double percentile, double compression, String... pairs) {
        return of(AggSpec.approximatePercentile(percentile, compression), pairs);
    }

    /**
     * Create an {@link io.deephaven.api.agg.spec.AggSpecApproximatePercentile approximate percentile} aggregation for
     * the supplied input column name and percentile/output column name pairs with the default T-Digest
     * {@link AggSpecApproximatePercentile#compression() compression} factor.
     *
     * @param inputColumn The input column name
     * @param percentileOutputs The {@link PercentileOutput percentile/output column name pairs} for the component
     *        aggregations; see {@link #PctOut(double, String)}.
     * @return The aggregation
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
     * @param compression T-Digest {@link AggSpecTDigest#compression() compression} factor; must be &gt; 1, should
     *        probably be &lt; 1000
     * @param percentileOutputs The {@link PercentileOutput percentile/output column name pairs} for the component
     *        aggregations; see {@link #PctOut(double, String)}.
     * @return The aggregation
     */
    static Aggregation AggApproxPct(String inputColumn, double compression, PercentileOutput... percentileOutputs) {
        final BiFunction<ColumnName, PercentileOutput, ColumnAggregation> aggFactory =
                (ic, po) -> ColumnAggregation.of(AggSpec.approximatePercentile(po.percentile(), compression),
                        Pair.of(ic, po.output()));
        return of(aggFactory, inputColumn, percentileOutputs);
    }

    /**
     * Create an {@link io.deephaven.api.agg.spec.AggSpecAvg average} (<i>arithmetic mean</i>) aggregation for the
     * supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggAvg(String... pairs) {
        return of(AggSpec.avg(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.Count count} aggregation with the supplied output column name.
     *
     * @param resultColumn The {@link Count#column() output column} name
     * @return The aggregation
     */
    static Count AggCount(String resultColumn) {
        return Count.of(resultColumn);
    }

    /**
     * Create a {@link io.deephaven.api.agg.CountWhere count} aggregation with the supplied output column name, counting
     * values that pass the supplied {@code filters}.
     *
     * @param resultColumn The {@link Count#column() output column} name
     * @return The aggregation
     */
    static CountWhere AggCountWhere(String resultColumn, String... filters) {
        return CountWhere.of(resultColumn, filters);
    }

    /**
     * Create a {@link io.deephaven.api.agg.CountWhere count} aggregation with the supplied output column name, counting
     * values that pass the supplied {@code filter}.
     *
     * @param resultColumn The {@link Count#column() output column} name
     * @return The aggregation
     */
    static CountWhere AggCountWhere(String resultColumn, Filter filter) {
        return CountWhere.of(resultColumn, filter);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecCountDistinct count distinct} aggregation for the supplied
     * column name pairs. This will not count {@code null} values from the input column(s).
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggCountDistinct(String... pairs) {
        return of(AggSpec.countDistinct(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecCountDistinct count distinct} aggregation for the supplied
     * column name pairs. This will count {@code null} values from the input column(s) if {@code countNulls} is
     * {@code true}.
     *
     * @param countNulls Whether {@code null} values should be counted; see {@link AggSpecCountDistinct#countNulls()}}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggCountDistinct(boolean countNulls, String... pairs) {
        return of(AggSpec.countDistinct(countNulls), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecDistinct distinct} aggregation for the supplied column name
     * pairs. This will not include {@code null} values in the output column(s).
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggDistinct(String... pairs) {
        return of(AggSpec.distinct(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecDistinct distinct} aggregation for the supplied column name
     * pairs. This will include {@code null} values in the output column(s) if {@code includeNulls} is {@code true}.
     *
     * @param includeNulls Whether {@code null} values should be included; see {@link AggSpecDistinct#includeNulls()}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggDistinct(boolean includeNulls, String... pairs) {
        return of(AggSpec.distinct(includeNulls), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecFirst first} aggregation for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggFirst(String... pairs) {
        return of(AggSpec.first(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.FirstRowKey first row key} aggregation with the supplied result column.
     *
     * @param resultColumn The {@link FirstRowKey#column() output column} name
     * @return The aggregation
     */
    static FirstRowKey AggFirstRowKey(String resultColumn) {
        return FirstRowKey.of(resultColumn);
    }

    /**
     * <p>
     * Create a {@link Formula formula} aggregation with the supplied {@code formula}. This variant requires the formula
     * to provide the output column name and specific input column names in the following format:
     * </p>
     * {@code
     * AggFormula("output_col=(input_col1 + input_col2) * input_col3")
     * }
     *
     * @param formulaString The formula to use to produce the output column
     * @return The aggregation
     */
    static Formula AggFormula(String formulaString) {
        return Formula.parse(formulaString);
    }

    /**
     * <p>
     * Create a {@link Formula formula} aggregation with the supplied {@code columnName} and {@code expression}. This
     * variant requires the formula to provide the output column name and the expression to evaluate in the following
     * format:
     * </p>
     * {@code
     * AggFormula("output_col", "(input_col1 + input_col2) * input_col3")
     * }
     *
     * @param columnName The output column name
     * @param expression The expression to use to produce the output column
     * @return The aggregation
     */
    static Formula AggFormula(String columnName, String expression) {
        return Formula.of(columnName, expression);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecFormula formula} aggregation with the supplied {@code formula},
     * {@code paramToken}, and column name pairs.
     *
     * @param formula The {@link AggSpecFormula#formula() formula} to use for all input columns to produce all output
     *        columns
     * @param paramToken The {@link AggSpecFormula#paramToken() parameter token} to replace with the input column name
     *        in {@code formula}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    @Deprecated
    static Aggregation AggFormula(String formula, String paramToken, String... pairs) {
        return of(AggSpec.formula(formula, paramToken), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecFreeze freeze} aggregation for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggFreeze(String... pairs) {
        return of(AggSpec.freeze(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecGroup group} aggregation for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggGroup(String... pairs) {
        return of(AggSpec.group(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecLast last} aggregation for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggLast(String... pairs) {
        return of(AggSpec.last(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.LastRowKey last row key} aggregation with the supplied result column.
     *
     * @param resultColumn The {@link LastRowKey#column() output column} name
     * @return The aggregation
     */
    static LastRowKey AggLastRowKey(String resultColumn) {
        return LastRowKey.of(resultColumn);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecMax max} aggregation for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggMax(String... pairs) {
        return of(AggSpec.max(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecMedian median} aggregation for the supplied column name pairs.
     * For numeric types, if there are an even number of values the result will be an average of the two middle values.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggMed(String... pairs) {
        return of(AggSpec.median(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecMedian median} aggregation for the supplied column name pairs.
     * For numeric types, if there are an even number of values the result will be an average of the two middle values
     * if {@code average} is {@code true}, else the result will be the lower of the two middle values.
     *
     * @param average Whether to average the middle two values for even-sized result sets of numeric types; see
     *        {@link AggSpecMedian#averageEvenlyDivided()}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggMed(boolean average, String... pairs) {
        return of(AggSpec.median(average), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecMin min} aggregation for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggMin(String... pairs) {
        return of(AggSpec.min(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.Partition partition} aggregation with the supplied output column name and
     * group-by columns included in the output sub-tables.
     *
     * @param resultColumn The {@link Partition#column() output column} name
     * @return The aggregation
     */
    static Partition AggPartition(String resultColumn) {
        return Partition.of(resultColumn);
    }

    /**
     * Create a {@link io.deephaven.api.agg.Partition partition} aggregation with the supplied output column name.
     *
     * @param resultColumn The {@link Partition#column() output column} name
     * @param includeGroupByColumns Whether to include group-by columns in the result
     * @return The aggregation
     */
    static Partition AggPartition(String resultColumn, boolean includeGroupByColumns) {
        return Partition.of(resultColumn, includeGroupByColumns);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecPercentile percentile} aggregation for the supplied percentile
     * and column name pairs.
     *
     * @param percentile The {@link AggSpecPercentile#percentile() percentile} to use for all component aggregations
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggPct(double percentile, String... pairs) {
        return of(AggSpec.percentile(percentile), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecPercentile percentile} aggregation for the supplied percentile
     * and column name pairs. If the percentile equally divides the value space, the result will be the average of the
     * values immediately below and above if {@code average} is {@code true}.
     *
     * @param percentile The {@link AggSpecPercentile#percentile() percentile} to use for all component aggregations
     * @param average Whether to average the lower and higher values for evenly divided result sets of numeric types;
     *        see {@link AggSpecPercentile#averageEvenlyDivided()}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggPct(double percentile, boolean average, String... pairs) {
        return of(AggSpec.percentile(percentile, average), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecPercentile percentile} aggregation for the supplied input column
     * name and percentile/output column name pairs.
     *
     * @param inputColumn The input column name
     * @param percentileOutputs The {@link PercentileOutput percentile/output column name pairs} for the component
     *        aggregations; see {@link #PctOut(double, String)}.
     * @return The aggregation
     */
    static Aggregation AggPct(String inputColumn, PercentileOutput... percentileOutputs) {
        final BiFunction<ColumnName, PercentileOutput, ColumnAggregation> aggFactory =
                (ic, po) -> ColumnAggregation.of(AggSpec.percentile(po.percentile()), Pair.of(ic, po.output()));
        return of(aggFactory, inputColumn, percentileOutputs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecPercentile percentile} aggregation for the supplied input column
     * name and percentile/output column name pairs. If the percentile equally divides the value space, the result will
     * be the average of the values immediately below and above if {@code average} is {@code true}.
     *
     * @param inputColumn The input column name
     * @param average Whether to average the lower and higher values for evenly divided result sets of numeric types;
     *        see {@link AggSpecPercentile#averageEvenlyDivided()}
     * @param percentileOutputs The {@link PercentileOutput percentile/output column name pairs} for the component
     *        aggregations; see {@link #PctOut(double, String)}.
     * @return The aggregation
     */
    static Aggregation AggPct(String inputColumn, boolean average, PercentileOutput... percentileOutputs) {
        final BiFunction<ColumnName, PercentileOutput, ColumnAggregation> aggFactory = (ic, po) -> ColumnAggregation
                .of(AggSpec.percentile(po.percentile(), average), Pair.of(ic, po.output()));
        return of(aggFactory, inputColumn, percentileOutputs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecSortedFirst sorted first} aggregation for the supplied sort
     * column name and input/output column name pairs.
     *
     * @param sortColumn The sort column name
     * @param pairs The input/output column name pairs for the component aggregations
     * @return The aggregation
     */
    static Aggregation AggSortedFirst(String sortColumn, String... pairs) {
        return of(AggSpec.sortedFirst(sortColumn), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecSortedFirst sorted first} aggregation for the supplied sort
     * column names and input/output column name pairs.
     *
     * @param sortColumns The sort column names
     * @param pairs The input/output column name pairs for the component aggregations
     * @return The aggregation
     */
    static Aggregation AggSortedFirst(Collection<? extends String> sortColumns, String... pairs) {
        return of(AggSpec.sortedFirst(sortColumns), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecSortedLast sorted last} aggregation for the supplied sort column
     * name and input/output column name pairs.
     *
     * @param sortColumn The sort column name
     * @param pairs The input/output column name pairs for the component aggregations
     * @return The aggregation
     */
    static Aggregation AggSortedLast(String sortColumn, String... pairs) {
        return of(AggSpec.sortedLast(sortColumn), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecSortedLast sorted last} aggregation for the supplied sort column
     * names and input/output column name pairs.
     *
     * @param sortColumns The sort column names
     * @param pairs The input/output column name pairs for the component aggregations
     * @return The aggregation
     */
    static Aggregation AggSortedLast(Collection<? extends String> sortColumns, String... pairs) {
        return of(AggSpec.sortedLast(sortColumns), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecStd sample standard deviation} aggregation for the supplied
     * column name pairs.
     * <p>
     * Sample standard deviation is computed using Bessel's correction
     * (https://en.wikipedia.org/wiki/Bessel%27s_correction), which ensures that the sample variance will be an unbiased
     * estimator of population variance.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggStd(String... pairs) {
        return of(AggSpec.std(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecSum sum} aggregation for the supplied column name pairs.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggSum(String... pairs) {
        return of(AggSpec.sum(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecTDigest T-Digest} aggregation for the supplied column name pairs
     * with the default T-Digest {@link AggSpecTDigest#compression() compression} factor.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggTDigest(String... pairs) {
        return of(AggSpec.tDigest(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecTDigest T-Digest} aggregation for the supplied column name pairs
     * with the supplied {@code compression} factor.
     *
     * @param compression T-Digest {@link AggSpecTDigest#compression() compression} factor; must be &gt; 1, should
     *        probably be &lt; 1000
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggTDigest(double compression, String... pairs) {
        return of(AggSpec.tDigest(compression), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecUnique unique} aggregation for the supplied column name pairs.
     * This will not consider {@code null} values when determining if a group has a single unique value. Non-unique
     * groups will have {@code null} values in the output column.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggUnique(String... pairs) {
        return of(AggSpec.unique(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecUnique unique} aggregation for the supplied column name pairs.
     * This will consider {@code null} values when determining if a group has a single unique value if
     * {@code includeNulls} is {@code true}. Non-unique groups will have {@code null} values in the output column.
     *
     * @param includeNulls Whether to consider {@code null} values towards uniqueness; see
     *        {@link AggSpecUnique#includeNulls()}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggUnique(boolean includeNulls, String... pairs) {
        return AggUnique(includeNulls, null, pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecUnique unique} aggregation for the supplied column name pairs.
     * This will consider {@code null} values when determining if a group has a single unique value if
     * {@code includeNulls} is {@code true}. Non-unique groups will have the value wrapped by {@code nonUniqueSentinel}
     * in the output column.
     *
     * @param includeNulls Whether to consider {@code null} values towards uniqueness
     * @param nonUniqueSentinel The value to output for non-unique groups
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggUnique(boolean includeNulls, UnionObject nonUniqueSentinel, String... pairs) {
        return of(AggSpec.unique(includeNulls, nonUniqueSentinel), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecVar sample variance} aggregation for the supplied column name
     * pairs.
     * <p>
     * Sample variance is computed using Bessel's correction (https://en.wikipedia.org/wiki/Bessel%27s_correction),
     * which ensures that the sample variance will be an unbiased estimator of population variance.
     *
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggVar(String... pairs) {
        return of(AggSpec.var(), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecWAvg weighted average} aggregation for the supplied weight
     * column name and column name pairs.
     *
     * @param weightColumn The {@link AggSpecWAvg#weight() weight column name}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggWAvg(String weightColumn, String... pairs) {
        return of(AggSpec.wavg(weightColumn), pairs);
    }

    /**
     * Create a {@link io.deephaven.api.agg.spec.AggSpecWSum weighted sum} aggregation for the supplied weight column
     * name and column name pairs.
     *
     * @param weightColumn The {@link AggSpecWSum#weight() weight column name}
     * @param pairs The input/output column name pairs
     * @return The aggregation
     */
    static Aggregation AggWSum(String weightColumn, String... pairs) {
        return of(AggSpec.wsum(weightColumn), pairs);
    }

    /**
     * Make a {@link PercentileOutput percentile/output column name pair}. This allows for strongly-typed input to
     * various approximate percentile and percentile aggregation factory methods.
     *
     * @param percentile The percentile for the aggregation
     * @param outputColumn The output column name to associate with the percentile
     * @return The percentile/output column name pair
     * @see #AggApproxPct(String, PercentileOutput...)
     * @see #AggApproxPct(String, double, PercentileOutput...)
     * @see #AggPct(String, PercentileOutput...)
     * @see #AggPct(String, boolean, PercentileOutput...)
     */
    static PercentileOutput PctOut(double percentile, String outputColumn) {
        return PercentileOutput.of(percentile, outputColumn);
    }

    /**
     * Calls every single visit method of {@code visitor} with a {@code null} object.
     *
     * @param visitor the visitor
     */
    static void visitAll(Visitor visitor) {
        visitor.visit((Aggregations) null);
        visitor.visit((ColumnAggregation) null);
        visitor.visit((ColumnAggregations) null);
        visitor.visit((Count) null);
        visitor.visit((CountWhere) null);
        visitor.visit((FirstRowKey) null);
        visitor.visit((LastRowKey) null);
        visitor.visit((Partition) null);
        visitor.visit((Formula) null);
    }

    /**
     * Glue method to deliver this Aggregation to a {@link AggSpec.Visitor}.
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
         * Visit a {@link CountWhere count aggregation}.
         *
         * @param countWhere The count aggregation
         */
        void visit(CountWhere countWhere);

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

        /**
         * Visit a {@link Partition partition aggregation}.
         *
         * @param partition The partition aggregation
         */
        void visit(Partition partition);

        /**
         * Visit a {@link Formula formula aggregation}.
         *
         * @param formula The formula aggregation
         */
        void visit(Formula formula);
    }
}

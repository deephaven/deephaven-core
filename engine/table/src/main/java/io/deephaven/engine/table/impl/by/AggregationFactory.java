/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.engine.table.impl.by;

import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Multi;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.*;
import io.deephaven.engine.table.impl.*;
import io.deephaven.engine.table.impl.by.ssmminmax.SsmChunkedMinMaxOperator;
import io.deephaven.engine.table.impl.select.MatchPairFactory;
import io.deephaven.engine.table.impl.sources.ReinterpretUtils;
import io.deephaven.engine.table.impl.sources.SingleValueObjectColumnSource;
import io.deephaven.engine.table.impl.ssms.SegmentedSortedMultiSet;
import io.deephaven.time.DateTime;
import io.deephaven.vector.Vector;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * The AggregationFactory combines one or more aggregations into an {@link AggregationSpec} for use internally by the
 * implementation of {@link Table#aggBy}.
 *
 * <p>
 * The intended use of this class is to call the {@link #AggCombo(AggregationElement...)} method with a set of
 * aggregations defined by:
 * <ul>
 * <li>{@link #AggMin}</li>
 * <li>{@link #AggMax}</li>
 * <li>{@link #AggSum}</li>
 * <li>{@link #AggAbsSum}</li>
 * <li>{@link #AggVar}</li>
 * <li>{@link #AggAvg}</li>
 * <li>{@link #AggWAvg}</li>
 * <li>{@link #AggWSum}</li>
 * <li>{@link #AggMed}</li>
 * <li>{@link #AggPct}</li>
 * <li>{@link #AggStd}</li>
 * <li>{@link #AggFirst}</li>
 * <li>{@link #AggLast}</li>
 * <li>{@link #AggCount}</li>
 * <li>{@link #AggCountDistinct}</li>
 * <li>{@link #AggDistinct}</li>
 * <li>{@link #AggGroup}</li>
 * <li>{@link #AggSortedFirst}</li>
 * <li>{@link #AggSortedLast}</li>
 * </ul>
 */
public class AggregationFactory implements AggregationSpec {
    static final String ROLLUP_RUNNING_SUM_COLUMN_ID = "_RS_";
    static final String ROLLUP_RUNNING_SUM2_COLUMN_ID = "_RS2_";
    static final String ROLLUP_NONNULL_COUNT_COLUMN_ID = "_NNC_";
    static final String ROLLUP_NAN_COLUMN_ID = "_NaN_";
    static final String ROLLUP_PIC_COLUMN_ID = "_PIC_";
    static final String ROLLUP_NIC_COLUMN_ID = "_NIC_";
    public static final String ROLLUP_DISTINCT_SSM_COLUMN_ID = "_SSM_";

    private final List<AggregationElement> underlyingAggregations = new ArrayList<>();
    private final boolean isRollup;
    private final boolean secondLevel;

    public static final String ROLLUP_COLUMN_SUFFIX = "__ROLLUP__";

    /**
     * Create a new AggregationFactory suitable for passing to
     * {@link QueryTable#by(AggregationSpec, io.deephaven.engine.table.impl.select.SelectColumn...)}.
     *
     * @param aggregations the aggregations to compute
     *
     * @return a new table with the specified aggregations.
     */
    public static AggregationFactory AggCombo(AggregationElement... aggregations) {
        return new AggregationFactory(aggregations);
    }

    /**
     * Create a formula aggregation.
     *
     * @param formula the formula to apply to each group
     * @param formulaParam the parameter name within the formula
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggFormula(String formula, String formulaParam, final String... matchPairs) {
        return new AggregationElementImpl(new AggregationFormulaSpec(formula, formulaParam), matchPairs);
    }

    /**
     * Create a minimum aggregation, equivalent to {@link Table#minBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggMin(final String... matchPairs) {
        return Agg(AggType.Min, matchPairs);
    }

    /**
     * Create a maximum aggregation, equivalent to {@link Table#maxBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggMax(final String... matchPairs) {
        return Agg(AggType.Max, matchPairs);
    }

    /**
     * Create a summation aggregation, equivalent to {@link Table#sumBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggSum(final String... matchPairs) {
        return Agg(AggType.Sum, matchPairs);
    }

    /**
     * Create an absolute sum aggregation, equivalent to {@link Table#absSumBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggAbsSum(final String... matchPairs) {
        return Agg(AggType.AbsSum, matchPairs);
    }

    /**
     * Create a variance aggregation, equivalent to {@link Table#varBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggVar(final String... matchPairs) {
        return Agg(AggType.Var, matchPairs);
    }

    /**
     * Create an average aggregation, equivalent to {@link Table#avgBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggAvg(final String... matchPairs) {
        return Agg(AggType.Avg, matchPairs);
    }

    /**
     * Create a weighted average aggregation, equivalent to {@link Table#wavgBy(String, String...)}.
     *
     * @param weight the name of the column to use as the weight for the average
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggWAvg(final String weight, final String... matchPairs) {
        return Agg(new WeightedAverageSpecImpl(weight), matchPairs);
    }

    /**
     * Create a weighted sum aggregation, equivalent to {@link Table#wsumBy(String, String...)}.
     *
     * @param weight the name of the column to use as the weight for the sum
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggWSum(final String weight, final String... matchPairs) {
        return Agg(new WeightedSumSpecImpl(weight), matchPairs);
    }

    /**
     * Create a median aggregation, equivalent to {@link Table#medianBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggMed(final String... matchPairs) {
        return AggPct(0.50d, true, matchPairs);
    }

    /**
     * Create a standard deviation aggregation, equivalent to {@link Table#stdBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggStd(final String... matchPairs) {
        return Agg(AggType.Std, matchPairs);
    }

    /**
     * Create a first aggregation, equivalent to {@link Table#firstBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggFirst(final String... matchPairs) {
        return Agg(AggType.First, matchPairs);
    }

    /**
     * Create a last aggregation, equivalent to {@link Table#lastBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggLast(final String... matchPairs) {
        return Agg(AggType.Last, matchPairs);
    }

    /**
     * Create a sorted first aggregation, equivalent to {@link io.deephaven.engine.util.SortedBy#sortedFirstBy}.
     *
     * @param sortColumn the column to sort by
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggSortedFirst(final String sortColumn, final String... matchPairs) {
        return Agg(new SortedFirstBy(sortColumn), matchPairs);
    }

    /**
     * Create a sorted last aggregation, equivalent to {@link io.deephaven.engine.util.SortedBy#sortedLastBy}.
     *
     * @param sortColumn the column to sort by
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggSortedLast(final String sortColumn, final String... matchPairs) {
        return Agg(new SortedLastBy(sortColumn), matchPairs);
    }

    /**
     * Create a sorted first aggregation, equivalent to {@link io.deephaven.engine.util.SortedBy#sortedFirstBy}.
     *
     * @param sortColumns the column to sort by
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggSortedFirst(final String[] sortColumns, final String... matchPairs) {
        return Agg(new SortedFirstBy(sortColumns), matchPairs);
    }

    /**
     * Create a sorted last aggregation, equivalent to {@link io.deephaven.engine.util.SortedBy#sortedLastBy}.
     *
     * @param sortColumns the columns to sort by
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggSortedLast(final String[] sortColumns, final String... matchPairs) {
        return Agg(new SortedLastBy(sortColumns), matchPairs);
    }

    /**
     * Create a group aggregation, equivalent to {@link Table#groupBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggGroup(final String... matchPairs) {
        return Agg(AggType.Group, matchPairs);
    }

    /**
     * Create an count aggregation, equivalent to {@link Table#countBy(String)}.
     *
     * @param resultColumn the name of the result column containing the count of each group
     *
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggCount(final String resultColumn) {
        return new CountAggregationElement(resultColumn);
    }

    /**
     * Create a distinct count aggregation.
     *
     * The output column contains the number of distinct values for the input column in that group.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}. Null values
     *         are not counted.
     */
    public static AggregationElement AggCountDistinct(final String... matchPairs) {
        return AggCountDistinct(false, matchPairs);
    }

    /**
     * Create a distinct count aggregation.
     *
     * The output column contains the number of distinct values for the input column in that group.
     *
     * @param countNulls if true null values are counted as a distinct value, otherwise null values are ignored
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggCountDistinct(boolean countNulls, final String... matchPairs) {
        return Agg(new CountDistinctSpec(countNulls), matchPairs);
    }

    /**
     * Create a distinct aggregation.
     *
     * The output column contains a {@link Vector} with the distinct values for the input column within the group.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}. Null values
     *         are ignored.
     */
    public static AggregationElement AggDistinct(final String... matchPairs) {
        return AggDistinct(false, matchPairs);
    }


    /**
     * Create a distinct aggregation.
     *
     * The output column contains a {@link Vector} with the distinct values for the input column within the group.
     *
     * @param countNulls if true, then null values are included in the result, otherwise null values are ignored
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggDistinct(boolean countNulls, final String... matchPairs) {
        return Agg(new DistinctSpec(countNulls), matchPairs);
    }

    /**
     * Create a Unique aggregation.
     *
     * The output column contains a value of the same type as the input column which contains<br>
     * <ul>
     * <li>The "no key value" - if there are no values present</li>
     * <li>The single unique value - if there is only a single value present</li>
     * <li>The "non unique value" - if there are more than 1 distinct values present</li>
     * </ul>
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggUnique(final String... matchPairs) {
        return Agg(new UniqueSpec(false), matchPairs);
    }

    /**
     * Create a Unique aggregation.
     *
     * The output column contains a value of the same type as the input column which contains<br>
     * <ul>
     * <li>The "no key value" - if there are no values present</li>
     * <li>The single unique value - if there is only a single value present</li>
     * <li>The "non unique value" - if there are more than 1 distinct values present</li>
     * </ul>
     *
     * @param countNulls if true, then null values are included in the result, otherwise null values are ignored
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}. Output
     *         columns contain null if there are no values present or there are more than 1 distinct values present.
     */
    public static AggregationElement AggUnique(boolean countNulls, final String... matchPairs) {
        return AggUnique(countNulls, null, null, matchPairs);
    }

    /**
     * Create a Unique aggregation.
     *
     * The output column contains a value of the same type as the input column which contains<br>
     * <ul>
     * <li>The "no key value" - if there are no values present</li>
     * <li>The single unique value - if there is only a single value present</li>
     * <li>The "non unique value" - if there are more than 1 distinct values present</li>
     * </ul>
     *
     * @param countNulls if true, then null values are included in the result, otherwise null values are ignored
     * @param noKeyValue the value to use if there are no values present
     * @param nonUniqueValue the value to use if there are more than 1 values present
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggUnique(boolean countNulls, Object noKeyValue, Object nonUniqueValue,
            final String... matchPairs) {
        return Agg(new UniqueSpec(countNulls, noKeyValue, nonUniqueValue), matchPairs);
    }

    /**
     * Create a percentile aggregation.
     *
     * @param percentile the percentile to calculate
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggPct(double percentile, final String... matchPairs) {
        return Agg(new PercentileBySpecImpl(percentile), matchPairs);
    }

    /**
     * Create a percentile aggregation.
     *
     * @param percentile the percentile to calculate
     * @param averageMedian if true, then when the upper values and lower values have an equal size; average the highest
     *        lower value and lowest upper value to produce the median value for integers, longs, doubles, and floats
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement AggPct(double percentile, boolean averageMedian, final String... matchPairs) {
        return Agg(new PercentileBySpecImpl(percentile, averageMedian), matchPairs);
    }

    /**
     * Create an aggregation.
     *
     * @param factory aggregation factory.
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement Agg(AggregationSpec factory, final String... matchPairs) {
        return new AggregationElementImpl(factory, matchPairs);
    }

    /**
     * Create an aggregation.
     *
     * @param factory aggregation factory.
     * @param matchPairs the columns to apply the aggregation to.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement Agg(AggregationSpec factory, final MatchPair... matchPairs) {
        return new AggregationElementImpl(factory, matchPairs);
    }

    /**
     * Create an aggregation.
     *
     * @param factoryType aggregation factory type.
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *        the same name, then the column name can be specified.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement Agg(AggType factoryType, final String... matchPairs) {
        return Agg(factoryType, MatchPairFactory.getExpressions(matchPairs));
    }

    /**
     * Create an aggregation.
     *
     * @param factoryType aggregation factory type.
     * @param matchPairs the columns to apply the aggregation to.
     * @return a AggregationElement object suitable for passing to {@link #AggCombo(AggregationElement...)}
     */
    public static AggregationElement Agg(AggType factoryType, final MatchPair... matchPairs) {
        final AggregationSpec factory;
        switch (factoryType) {
            case Min:
                factory = new MinMaxBySpecImpl(true);
                break;
            case Max:
                factory = new MinMaxBySpecImpl(false);
                break;
            case Sum:
                factory = new SumSpec();
                break;
            case AbsSum:
                factory = new AbsSumSpec();
                break;
            case Avg:
                factory = new AvgSpec();
                break;
            case Var:
                factory = new VarSpec();
                break;
            case Std:
                factory = new StdSpec();
                break;
            case First:
                factory = new FirstBySpecImpl();
                break;
            case Last:
                factory = new LastBySpecImpl();
                break;
            case Group:
                factory = new AggregationGroupSpec();
                break;
            case CountDistinct:
                factory = new CountDistinctSpec();
                break;
            case Distinct:
                factory = new DistinctSpec();
                break;
            case Unique:
                factory = new UniqueSpec();
                break;
            case Skip:
                throw new IllegalArgumentException("Skip is not a valid aggregation type for AggCombo.");
            default:
                throw new UnsupportedOperationException("Unknown AggType: " + factoryType);
        }
        return new AggregationElementImpl(factory, matchPairs);
    }

    /**
     * Create a factory for performing rollups.
     */
    public AggregationFactory rollupFactory() {
        // we want to leave off the null value column source for children; but add a by external combo for the rollup
        return new AggregationFactory(
                Stream.concat(underlyingAggregations.subList(0, underlyingAggregations.size() - 1).stream().map(x -> {
                    final AggregationSpec underlyingStateFactory = x.getSpec();
                    Assert.assertion(underlyingStateFactory instanceof ReaggregatableStatefactory,
                            "underlyingStateFactory instanceof ReaggregatableStatefactory", underlyingStateFactory,
                            "UnderlyingStateFactory");

                    // noinspection ConstantConditions
                    final ReaggregatableStatefactory reaggregatableStatefactory =
                            (ReaggregatableStatefactory) underlyingStateFactory;

                    Assert.assertion(reaggregatableStatefactory.supportsRollup(),
                            "((ReaggregatableStatefactory)x.getUnderlyingStateFactory()).supportsRollup()",
                            underlyingStateFactory, "UnderlyingStateFactory");
                    final ReaggregatableStatefactory factory = reaggregatableStatefactory.rollupFactory();

                    final List<String> leftColumns = new ArrayList<>();
                    Collections.addAll(leftColumns, MatchPair.getLeftColumns(x.getResultPairs()));

                    return Agg(factory, leftColumns.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
                }), Stream.of(new PartitionAggregationElement(false))).collect(Collectors.toList()), true, true);
    }

    public AggregationFactory forRollup(boolean includeConstituents) {
        final List<AggregationElement> newUnderliers =
                underlyingAggregations.stream().map(AggregationElement::forRollup).collect(Collectors.toList());
        newUnderliers.add(includeConstituents ? new PartitionAggregationElement(true)
                : new NullAggregationElement(Collections.singletonMap(RollupInfo.ROLLUP_COLUMN, Object.class)));
        return new AggregationFactory(newUnderliers, true, false);
    }

    /**
     * Create a new factory that will have columns with all null values.
     *
     * Used by rollup to empty out unused grouping columns.
     *
     * @param nullColumns a map of column names to types.
     *
     * @return a new AggregationFactory that will produce null values for the given columns.
     */
    public AggregationFactory withNulls(Map<String, Class<?>> nullColumns) {
        final List<AggregationElement> newAggregations = new ArrayList<>(underlyingAggregations.size() + 1);
        newAggregations.add(new NullAggregationElement(nullColumns));
        newAggregations.addAll(underlyingAggregations);
        return new AggregationFactory(newAggregations, isRollup, secondLevel);
    }

    private static final String[] ROLLUP_KEY_COLUMNS = {RollupInfo.ROLLUP_COLUMN};

    private String[] getKeyColumns() {
        return ROLLUP_KEY_COLUMNS;
    }

    public interface AggregationElement {

        /**
         * Equivalent to {@code convertOrdered(Collections.singleton(aggregation))} or
         * {@code optimizeAndConvert(Collections.singleton(aggregation))}.
         *
         * @param aggregation The {@link Aggregation aggregation}
         * @return A list of {@link AggregationElement aggregation elements}
         * @see #optimizeAndConvert(Collection)
         * @see #convert(Collection)
         */
        static List<AggregationElement> convert(Aggregation aggregation) {
            return optimizeAndConvert(Collections.singleton(aggregation));
        }

        /**
         * Converts and optimizes the aggregations, collapsing relevant aggregations into single
         * {@link AggregationElement elements} where applicable.
         *
         * <p>
         * Note: due to the optimization, the aggregation elements may not be in the same order as specified in
         * {@code aggregations}.
         *
         * @param aggregations The {@link Aggregation aggregation}
         * @return A list of {@link AggregationElement aggregation elements}
         * @see #convert(Aggregation)
         * @see #convert(Collection)
         */
        static List<AggregationElement> optimizeAndConvert(Collection<? extends Aggregation> aggregations) {
            AggregationAdapterOptimized builder = new AggregationAdapterOptimized();
            aggregations.forEach(a -> a.walk(builder));
            return builder.build();
        }

        /**
         * Converts and the aggregations, only collapsing {@link Multi multi} aggregations into single
         * {@link AggregationElement elements}, leaving singular aggregations as they are.
         *
         * <p>
         * Note: The results will preserve the intended order of the inputs.
         *
         * @param aggregations The {@link Aggregation aggregation}
         * @return A list of {@link AggregationElement aggregation elements}
         * @see #convert(Aggregation)
         * @see #optimizeAndConvert(Collection)
         */
        static List<AggregationElement> convert(Collection<? extends Aggregation> aggregations) {
            AggregationAdapterOrdered builder = new AggregationAdapterOrdered();
            aggregations.forEach(a -> a.walk(builder));
            return builder.build();
        }

        AggregationSpec getSpec();

        String[] getSourceColumns();

        MatchPair[] getResultPairs();

        AggregationElement forRollup();

        AggregationMemoKey getMemoKey();
    }

    static public class AggregationElementImpl implements AggregationElement {
        private final MatchPair[] matchPairs;
        private final String[] rightColumns;
        private final AggregationSpec spec;

        public AggregationElementImpl(final AggregationSpec spec, final String... matchPairs) {
            this(spec, MatchPairFactory.getExpressions(matchPairs));
        }

        @SuppressWarnings("unused")
        public AggregationElementImpl(final AggregationSpec spec, final Collection<String> matchPairs) {
            this(spec, MatchPairFactory.getExpressions(matchPairs));
        }

        AggregationElementImpl(final AggregationSpec spec, final MatchPair... matchPairs) {
            this.matchPairs = matchPairs;
            this.spec = spec;
            this.rightColumns = new String[matchPairs.length];
            for (int ii = 0; ii < matchPairs.length; ++ii) {
                this.rightColumns[ii] = this.matchPairs[ii].rightColumn;
            }
        }

        @Override
        public AggregationSpec getSpec() {
            return spec;
        }

        @Override
        public String[] getSourceColumns() {
            return rightColumns;
        }

        @Override
        public MatchPair[] getResultPairs() {
            return matchPairs;
        }

        @Override
        public AggregationElement forRollup() {
            if (!(spec instanceof ReaggregatableStatefactory)) {
                throw new UnsupportedOperationException(
                        "Not a reaggregatable state factory: " + spec);
            }
            if (!((ReaggregatableStatefactory) spec).supportsRollup()) {
                throw new UnsupportedOperationException(
                        "Underlying state factory does not support rollup: " + spec);
            }
            return new AggregationElementImpl(((ReaggregatableStatefactory) spec).forRollup(), matchPairs);
        }

        @Override
        public AggregationMemoKey getMemoKey() {
            return getSpec().getMemoKey();
        }

        @Override
        public String toString() {
            return "Agg{" + spec + ", " + Arrays.toString(matchPairs) + '}';
        }
    }

    static public class CountAggregationElement implements AggregationElement {
        private final String resultColumn;
        private final CountBySpecImpl underlyingStateFactory;

        public CountAggregationElement(String resultColumn) {
            this.resultColumn = resultColumn;
            underlyingStateFactory = new CountBySpecImpl(resultColumn);
        }

        @Override
        public AggregationSpec getSpec() {
            return underlyingStateFactory;
        }

        @Override
        public String[] getSourceColumns() {
            return new String[0];
        }

        @Override
        public MatchPair[] getResultPairs() {
            return new MatchPair[] {new MatchPair(resultColumn, resultColumn)};
        }

        @Override
        public AggregationElement forRollup() {
            return this;
        }


        @Override
        public AggregationMemoKey getMemoKey() {
            return getSpec().getMemoKey();
        }

        @Override
        public String toString() {
            return "Count(" + resultColumn + ")";
        }
    }

    static public class NullAggregationElement implements AggregationElement {
        private final Map<String, Class<?>> resultColumns;
        private final NullAggregationSpecImpl underlyingStateFactory;

        NullAggregationElement(Map<String, Class<?>> resultColumns) {
            this.resultColumns = resultColumns;
            this.underlyingStateFactory = new NullAggregationSpecImpl();
        }

        @Override
        public AggregationSpec getSpec() {
            return underlyingStateFactory;
        }

        @Override
        public String[] getSourceColumns() {
            return new String[0];
        }

        @Override
        public MatchPair[] getResultPairs() {
            return resultColumns.keySet().stream().map(rc -> new MatchPair(rc, rc)).toArray(MatchPair[]::new);
        }

        @Override
        public AggregationElement forRollup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public AggregationMemoKey getMemoKey() {
            return getSpec().getMemoKey();
        }

        @Override
        public String toString() {
            return "NullAggregationElement(" + resultColumns + ')';
        }
    }

    static private class PartitionAggregationElement implements AggregationElement {
        private final boolean leafLevel;

        private PartitionAggregationElement(boolean leafLevel) {
            this.leafLevel = leafLevel;
        }

        @Override
        public AggregationSpec getSpec() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String[] getSourceColumns() {
            return new String[0];
        }

        @Override
        public MatchPair[] getResultPairs() {
            return new MatchPair[] {new MatchPair(RollupInfo.ROLLUP_COLUMN, RollupInfo.ROLLUP_COLUMN)};
        }

        @Override
        public AggregationElement forRollup() {
            return this;
        }

        @Override
        public AggregationMemoKey getMemoKey() {
            // TODO: MEMOIZE!
            return null;
        }

        @Override
        public String toString() {
            return "PartitionAggregationElement(leafLevel=" + leafLevel + ')';
        }

    }

    public AggregationFactory(Collection<AggregationElement> aggregations) {
        this(aggregations, false, false);
    }

    public AggregationFactory(Collection<AggregationElement> aggregations, boolean isRollup,
            boolean secondLevelRollup) {
        this.isRollup = isRollup;
        this.secondLevel = secondLevelRollup;
        underlyingAggregations.addAll(aggregations);

        final Map<String, List<AggregationElement>> usedColumns = new LinkedHashMap<>();

        for (final AggregationElement aggregationElement : underlyingAggregations) {
            Stream.of(aggregationElement.getResultPairs()).map(MatchPair::leftColumn)
                    .forEach(rl -> usedColumns.computeIfAbsent(rl, x -> new ArrayList<>()).add(aggregationElement));
        }

        final Map<String, List<AggregationElement>> duplicates =
                usedColumns.entrySet().stream().filter(kv -> kv.getValue().size() > 1)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (!duplicates.isEmpty()) {
            final String errors =
                    duplicates.entrySet().stream().map(kv -> kv.getKey() + " used " + kv.getValue().size() + " times")
                            .collect(Collectors.joining(", "));
            throw new IllegalArgumentException("Duplicate output columns: " + errors);
        }
    }

    public AggregationFactory(AggregationElement... aggregations) {
        this(Arrays.asList(aggregations), false, false);
    }

    @Override
    public AggregationMemoKey getMemoKey() {
        final UnderlyingMemoKey[] underlyingMemoKeys = new UnderlyingMemoKey[underlyingAggregations.size()];
        for (int ii = 0; ii < underlyingMemoKeys.length; ++ii) {
            final AggregationElement aggregationElement = underlyingAggregations.get(ii);
            final AggregationMemoKey key = aggregationElement.getMemoKey();
            if (key == null) {
                return null;
            }
            underlyingMemoKeys[ii] = new UnderlyingMemoKey(key, aggregationElement.getSourceColumns(),
                    aggregationElement.getResultPairs());
        }

        return new AggByMemoKey(underlyingMemoKeys);
    }

    private static class UnderlyingMemoKey {
        private final AggregationMemoKey componentMemoKey;
        private final String[] sourceColumns;
        private final MatchPair[] resultPairs;


        private UnderlyingMemoKey(AggregationMemoKey componentMemoKey, String[] sourceColumns,
                MatchPair[] resultPairs) {
            this.componentMemoKey = componentMemoKey;
            this.sourceColumns = sourceColumns;
            this.resultPairs = resultPairs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            final UnderlyingMemoKey that = (UnderlyingMemoKey) o;
            return Objects.equals(componentMemoKey, that.componentMemoKey) &&
                    Arrays.equals(sourceColumns, that.sourceColumns) &&
                    Arrays.equals(resultPairs, that.resultPairs);
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(componentMemoKey);
            result = 31 * result + Arrays.hashCode(sourceColumns);
            result = 31 * result + Arrays.hashCode(resultPairs);
            return result;
        }
    }

    private static class AggByMemoKey implements AggregationMemoKey {
        private final UnderlyingMemoKey[] underlyingMemoKeys;

        private AggByMemoKey(UnderlyingMemoKey[] underlyingMemoKeys) {
            this.underlyingMemoKeys = underlyingMemoKeys;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(underlyingMemoKeys);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof AggByMemoKey
                    && Arrays.equals(underlyingMemoKeys, ((AggByMemoKey) obj).underlyingMemoKeys);
        }
    }

    @Override
    public String toString() {
        return "AggregationFactory{" + underlyingAggregations + '}';
    }

    public List<MatchPair> getMatchPairs() {
        return underlyingAggregations.stream().flatMap(c -> Arrays.stream(c.getResultPairs()))
                .collect(Collectors.toList());
    }

    public AggregationContextFactory makeAggregationContextFactory() {
        return (table, groupByColumns) -> {
            final List<IterativeChunkedAggregationOperator> operators = new ArrayList<>();
            final List<String[]> inputNames = new ArrayList<>();
            final List<ChunkSource.WithPrev<Values>> inputColumns = new ArrayList<>();
            final List<AggregationContextTransformer> transformers = new ArrayList<>();

            int trackedFirstOrLastIndex = -1;
            boolean externalFound = false;


            for (final AggregationElement aggregationElement : underlyingAggregations) {
                final boolean isStream = ((BaseTable) table).isStream();
                final boolean isAddOnly = ((BaseTable) table).isAddOnly();

                if (aggregationElement instanceof CountAggregationElement) {
                    operators.add(
                            new CountAggregationOperator(((CountAggregationElement) aggregationElement).resultColumn));
                    inputColumns.add(null);
                    inputNames.add(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
                } else if (aggregationElement instanceof AggregationElementImpl) {
                    final AggregationSpec inputAggregationSpec = aggregationElement.getSpec();

                    final boolean isNumeric = inputAggregationSpec.getClass() == SumSpec.class ||
                            inputAggregationSpec.getClass() == AbsSumSpec.class ||
                            inputAggregationSpec.getClass() == AvgSpec.class ||
                            inputAggregationSpec.getClass() == VarSpec.class ||
                            inputAggregationSpec.getClass() == StdSpec.class;
                    final boolean isCountDistinct =
                            inputAggregationSpec.getClass() == CountDistinctSpec.class;
                    final boolean isDistinct = inputAggregationSpec.getClass() == DistinctSpec.class;
                    final boolean isSelectDistinct =
                            inputAggregationSpec.getClass() == SelectDistinctSpecImpl.class;
                    final boolean isAggUnique = inputAggregationSpec.getClass() == UniqueSpec.class;
                    final boolean isMinMax = inputAggregationSpec instanceof MinMaxBySpecImpl;
                    final boolean isPercentile =
                            inputAggregationSpec.getClass() == PercentileBySpecImpl.class;
                    final boolean isSortedFirstOrLastBy =
                            inputAggregationSpec instanceof SortedFirstOrLastByFactoryImpl;
                    final boolean isFirst = inputAggregationSpec.getClass() == FirstBySpecImpl.class ||
                            inputAggregationSpec.getClass() == TrackingFirstBySpecImpl.class ||
                            (inputAggregationSpec.getClass() == KeyOnlyFirstOrLastBySpec.class &&
                                    !((KeyOnlyFirstOrLastBySpec) inputAggregationSpec).isLast());
                    final boolean isLast = inputAggregationSpec.getClass() == LastBySpecImpl.class ||
                            inputAggregationSpec.getClass() == TrackingLastBySpecImpl.class ||
                            (inputAggregationSpec.getClass() == KeyOnlyFirstOrLastBySpec.class &&
                                    ((KeyOnlyFirstOrLastBySpec) inputAggregationSpec).isLast());
                    final boolean isWeightedAverage =
                            inputAggregationSpec.getClass() == WeightedAverageSpecImpl.class;
                    final boolean isWeightedSum =
                            inputAggregationSpec.getClass() == WeightedSumSpecImpl.class;
                    final boolean isAggGroup =
                            inputAggregationSpec.getClass() == AggregationGroupSpec.class;
                    final boolean isFormula =
                            inputAggregationSpec.getClass() == AggregationFormulaSpec.class;

                    // noinspection StatementWithEmptyBody
                    if (isSelectDistinct) {
                        // Select-distinct is accomplished as a side effect of aggregating on the group-by columns.
                    } else {
                        final MatchPair[] comboMatchPairs = ((AggregationElementImpl) aggregationElement).matchPairs;
                        if (isSortedFirstOrLastBy) {
                            // noinspection ConstantConditions
                            final SortedFirstOrLastByFactoryImpl sortedFirstOrLastByFactory =
                                    (SortedFirstOrLastByFactoryImpl) inputAggregationSpec;
                            final boolean isSortedFirstBy = sortedFirstOrLastByFactory.isSortedFirst();

                            final MatchPair[] updatedMatchPairs;
                            if (sortedFirstOrLastByFactory.secondRollup
                                    && sortedFirstOrLastByFactory.getSortColumnNames().length == 1
                                    && sortedFirstOrLastByFactory.getSortColumnNames()[0]
                                            .endsWith(ROLLUP_COLUMN_SUFFIX)) {
                                updatedMatchPairs = Arrays.copyOf(comboMatchPairs, comboMatchPairs.length + 1);
                                final String redirectionName = sortedFirstOrLastByFactory.getSortColumnNames()[0];
                                updatedMatchPairs[updatedMatchPairs.length - 1] =
                                        new MatchPair(redirectionName, redirectionName);
                            } else {
                                updatedMatchPairs = comboMatchPairs;
                            }
                            final AggregationContext sflac = SortedFirstOrLastByAggregationFactory
                                    .getAggregationContext(table, sortedFirstOrLastByFactory.getSortColumnNames(),
                                            isSortedFirstBy, true, updatedMatchPairs);
                            Assert.eq(sflac.operators.length, "sflac.operators.length", 1);
                            Assert.eq(sflac.inputColumns.length, "sflac.operators.length", 1);
                            Assert.eq(sflac.inputNames.length, "sflac.operators.length", 1);
                            operators.add(sflac.operators[0]);
                            inputColumns.add(sflac.inputColumns[0]);
                            inputNames.add(sflac.inputNames[0]);
                        } else if (isNumeric || isMinMax || isPercentile || isCountDistinct || isDistinct
                                || isAggUnique) {
                            // add the stuff
                            Arrays.stream(comboMatchPairs).forEach(mp -> {
                                if (isRollup && secondLevel) {
                                    final boolean isAverage =
                                            inputAggregationSpec.getClass() == AvgSpec.class;
                                    final boolean isStd =
                                            inputAggregationSpec.getClass() == StdSpec.class;
                                    final boolean isVar =
                                            inputAggregationSpec.getClass() == VarSpec.class;
                                    final boolean isStdVar = isStd || isVar;
                                    if (isAverage || isStdVar) {
                                        final String runningSumName =
                                                mp.leftColumn() + ROLLUP_RUNNING_SUM_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                                        final String runningSum2Name =
                                                mp.leftColumn() + ROLLUP_RUNNING_SUM2_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                                        final String nonNullName =
                                                mp.leftColumn() + ROLLUP_NONNULL_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                                        final String nanName =
                                                mp.leftColumn() + ROLLUP_NAN_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                                        final String picName =
                                                mp.leftColumn() + ROLLUP_PIC_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                                        final String nicName =
                                                mp.leftColumn() + ROLLUP_NIC_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;

                                        final boolean isFloatingPoint = table.hasColumns(nanName);

                                        // record non null count
                                        inputColumns.add(table.getColumnSource(nonNullName));
                                        inputNames.add(new String[] {nonNullName});

                                        // record running sum
                                        inputColumns.add(table.getColumnSource(runningSumName));
                                        inputNames.add(new String[] {runningSumName});

                                        if (isStdVar) {
                                            inputColumns.add(table.getColumnSource(runningSum2Name));
                                            inputNames.add(new String[] {runningSum2Name});
                                        }

                                        if (isFloatingPoint) {
                                            // record nans, positive and negative infinities
                                            inputColumns.add(table.getColumnSource(nanName));
                                            inputNames.add(new String[] {nanName});
                                            inputColumns.add(table.getColumnSource(picName));
                                            inputNames.add(new String[] {picName});
                                            inputColumns.add(table.getColumnSource(nicName));
                                            inputNames.add(new String[] {nicName});
                                        }

                                        // then the input column for the updater (reavg/revar) operator
                                        inputColumns.add(null);

                                        // now add add the operators, and the final inputNames that matches the updating
                                        // operator
                                        final LongChunkedSumOperator nonNull =
                                                new LongChunkedSumOperator(false, nonNullName);
                                        operators.add(nonNull);

                                        if (isFloatingPoint) {
                                            final DoubleChunkedSumOperator runningSum =
                                                    new DoubleChunkedSumOperator(false, runningSumName);
                                            operators.add(runningSum);

                                            final DoubleChunkedSumOperator runningSum2;
                                            if (isStdVar) {
                                                runningSum2 = new DoubleChunkedSumOperator(false, runningSum2Name);
                                                operators.add(runningSum2);
                                            } else {
                                                runningSum2 = null;
                                            }

                                            final LongChunkedSumOperator nanSum =
                                                    new LongChunkedSumOperator(false, nanName);
                                            operators.add(nanSum);
                                            final LongChunkedSumOperator picSum =
                                                    new LongChunkedSumOperator(false, picName);
                                            operators.add(picSum);
                                            final LongChunkedSumOperator nicSum =
                                                    new LongChunkedSumOperator(false, nicName);
                                            operators.add(nicSum);

                                            if (isAverage) {
                                                if (table.getColumnSource(mp.leftColumn())
                                                        .getChunkType() == ChunkType.Float) {
                                                    operators.add(
                                                            new FloatChunkedReAvgOperator(mp.leftColumn(), runningSum,
                                                                    nonNull, nanSum, picSum, nicSum));
                                                } else if (table.getColumnSource(mp.leftColumn())
                                                        .getChunkType() == ChunkType.Double) {
                                                    operators.add(
                                                            new DoubleChunkedReAvgOperator(mp.leftColumn(), runningSum,
                                                                    nonNull, nanSum, picSum, nicSum));
                                                } else {
                                                    throw new UnsupportedOperationException();
                                                }
                                            } else {
                                                if (table.getColumnSource(mp.leftColumn())
                                                        .getChunkType() == ChunkType.Float
                                                        || table.getColumnSource(mp.leftColumn())
                                                                .getChunkType() == ChunkType.Double) {
                                                    operators.add(new FloatChunkedReVarOperator(mp.leftColumn(), isStd,
                                                            runningSum, runningSum2, nonNull, nanSum, picSum, nicSum));
                                                } else {
                                                    throw new UnsupportedOperationException();
                                                }
                                            }

                                            // our final operator is updated if any input changes
                                            final String[] inputNamesForColumn = new String[isStdVar ? 6 : 5];
                                            inputNamesForColumn[0] = nonNullName;
                                            inputNamesForColumn[1] = runningSumName;
                                            inputNamesForColumn[2] = nanName;
                                            inputNamesForColumn[3] = picName;
                                            inputNamesForColumn[4] = nicName;
                                            if (isStdVar) {
                                                inputNamesForColumn[5] = runningSum2Name;
                                            }
                                            inputNames.add(inputNamesForColumn);
                                        } else if (isStdVar) {
                                            final boolean isBigInteger = BigInteger.class
                                                    .isAssignableFrom(table.getColumnSource(runningSumName).getType());
                                            final boolean isBigDecimal = BigDecimal.class
                                                    .isAssignableFrom(table.getColumnSource(runningSumName).getType());

                                            if (isBigInteger) {
                                                final BigIntegerChunkedSumOperator runningSum =
                                                        new BigIntegerChunkedSumOperator(false, runningSumName);
                                                operators.add(runningSum);
                                                final BigIntegerChunkedSumOperator runningSum2 =
                                                        new BigIntegerChunkedSumOperator(false, runningSum2Name);
                                                operators.add(runningSum2);
                                                operators.add(new BigIntegerChunkedReVarOperator(mp.leftColumn(), isStd,
                                                        runningSum, runningSum2, nonNull));
                                            } else if (isBigDecimal) {
                                                final BigDecimalChunkedSumOperator runningSum =
                                                        new BigDecimalChunkedSumOperator(false, runningSumName);
                                                operators.add(runningSum);
                                                final BigDecimalChunkedSumOperator runningSum2 =
                                                        new BigDecimalChunkedSumOperator(false, runningSum2Name);
                                                operators.add(runningSum2);
                                                operators.add(new BigDecimalChunkedReVarOperator(mp.leftColumn(), isStd,
                                                        runningSum, runningSum2, nonNull));
                                            } else {
                                                final DoubleChunkedSumOperator runningSum =
                                                        new DoubleChunkedSumOperator(false, runningSumName);
                                                operators.add(runningSum);
                                                final DoubleChunkedSumOperator runningSum2 =
                                                        new DoubleChunkedSumOperator(false, runningSum2Name);
                                                operators.add(runningSum2);
                                                operators.add(new IntegralChunkedReVarOperator(mp.leftColumn(), isStd,
                                                        runningSum, runningSum2, nonNull));
                                            }
                                            // our final operator is updated if any input changes
                                            inputNames.add(new String[] {nonNullName, runningSumName, runningSum2Name});
                                        } else { // is an average and not floating point
                                            final boolean isBigDecimal = BigDecimal.class
                                                    .isAssignableFrom(table.getColumnSource(runningSumName).getType());
                                            final boolean isBigInteger = BigInteger.class
                                                    .isAssignableFrom(table.getColumnSource(runningSumName).getType());

                                            if (isBigInteger) {
                                                final BigIntegerChunkedSumOperator runningSum =
                                                        new BigIntegerChunkedSumOperator(false, runningSumName);
                                                operators.add(runningSum);
                                                operators.add(
                                                        new BigIntegerChunkedReAvgOperator(mp.leftColumn(), runningSum,
                                                                nonNull));
                                            } else if (isBigDecimal) {
                                                final BigDecimalChunkedSumOperator runningSum =
                                                        new BigDecimalChunkedSumOperator(false, runningSumName);
                                                operators.add(runningSum);
                                                operators.add(
                                                        new BigDecimalChunkedReAvgOperator(mp.leftColumn(), runningSum,
                                                                nonNull));
                                            } else {
                                                final LongChunkedSumOperator runningSum =
                                                        new LongChunkedSumOperator(false, runningSumName);
                                                operators.add(runningSum);
                                                operators.add(
                                                        new IntegralChunkedReAvgOperator(mp.leftColumn(), runningSum,
                                                                nonNull));
                                            }

                                            // our final operator is updated if any input changes
                                            inputNames.add(new String[] {nonNullName, runningSumName});
                                        }
                                        return;
                                    } else if (isCountDistinct || isDistinct || isAggUnique) {
                                        final String ssmColName =
                                                mp.leftColumn() + ROLLUP_DISTINCT_SSM_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                                        final ColumnSource<SegmentedSortedMultiSet<?>> ssmSource =
                                                table.getColumnSource(ssmColName);
                                        final ColumnSource<?> lastLevelResult = table.getColumnSource(mp.leftColumn());
                                        final boolean countNulls;
                                        final IterativeChunkedAggregationOperator op;
                                        if (isDistinct) {
                                            // noinspection ConstantConditions
                                            countNulls =
                                                    ((DistinctSpec) inputAggregationSpec).countNulls();
                                            op = IterativeOperatorSpec.getDistinctChunked(
                                                    lastLevelResult.getComponentType(), mp.leftColumn(), countNulls,
                                                    true,
                                                    true);
                                        } else if (isCountDistinct) {
                                            // noinspection ConstantConditions
                                            countNulls = ((CountDistinctSpec) inputAggregationSpec)
                                                    .countNulls();
                                            op = IterativeOperatorSpec.getCountDistinctChunked(
                                                    ssmSource.getComponentType(), mp.leftColumn(), countNulls, true,
                                                    true);
                                        } else {
                                            // noinspection ConstantConditions
                                            countNulls =
                                                    ((UniqueSpec) inputAggregationSpec).countNulls();
                                            // noinspection ConstantConditions
                                            op = IterativeOperatorSpec.getUniqueChunked(
                                                    lastLevelResult.getType(), mp.leftColumn(), countNulls, true,
                                                    ((UniqueSpec) inputAggregationSpec).getNoKeyValue(),
                                                    ((UniqueSpec) inputAggregationSpec)
                                                            .getNonUniqueValue(),
                                                    true);
                                        }

                                        inputColumns.add(ssmSource);
                                        inputNames.add(new String[] {ssmColName});
                                        operators.add(op);

                                        return;
                                    }
                                }

                                final ColumnSource<?> columnSource = table.getColumnSource(mp.rightColumn());
                                final Class<?> type = columnSource.getType();
                                final ColumnSource<?> inputSource = columnSource.getType() == DateTime.class
                                        ? ReinterpretUtils.dateTimeToLongSource(columnSource)
                                        : columnSource;

                                final String resultName = mp.leftColumn();
                                final boolean hasSource;
                                if (isMinMax) {
                                    final boolean isMinimum =
                                            ((MinMaxBySpecImpl) inputAggregationSpec).isMinimum();
                                    final OptionalInt priorMinMax = IntStream.range(0, inputColumns.size())
                                            .filter(idx -> (inputColumns.get(idx) == inputSource)
                                                    && (operators.get(idx) instanceof SsmChunkedMinMaxOperator))
                                            .findFirst();
                                    if (priorMinMax.isPresent()) {
                                        final SsmChunkedMinMaxOperator ssmChunkedMinMaxOperator =
                                                (SsmChunkedMinMaxOperator) operators.get(priorMinMax.getAsInt());
                                        operators.add(
                                                ssmChunkedMinMaxOperator.makeSecondaryOperator(isMinimum, resultName));
                                        hasSource = false;
                                    } else {
                                        operators.add(IterativeOperatorSpec.getMinMaxChunked(type, isMinimum,
                                                isStream || isAddOnly, resultName));
                                        hasSource = true;
                                    }
                                } else if (isPercentile) {
                                    if (isRollup) {
                                        throw new UnsupportedOperationException(
                                                "Percentile or Median can not be used in a rollup!");
                                    }
                                    operators.add(IterativeOperatorSpec.getPercentileChunked(type,
                                            ((PercentileBySpecImpl) inputAggregationSpec)
                                                    .getPercentile(),
                                            ((PercentileBySpecImpl) inputAggregationSpec)
                                                    .getAverageMedian(),
                                            resultName));
                                    hasSource = true;
                                } else {
                                    operators.add(((IterativeOperatorSpec) inputAggregationSpec)
                                            .getChunkedOperator(type, resultName, isRollup));
                                    hasSource = true;
                                }

                                if (hasSource) {
                                    inputColumns.add(inputSource);
                                } else {
                                    inputColumns.add(null);
                                }
                                inputNames.add(new String[] {mp.rightColumn()});
                            });
                        } else if (isFirst || isLast) {
                            inputColumns.add(null);
                            final String exposeRedirectionAs;
                            if (isRollup) {
                                exposeRedirectionAs =
                                        makeRedirectionName((IterativeIndexSpec) inputAggregationSpec);
                            } else if (inputAggregationSpec instanceof KeyOnlyFirstOrLastBySpec) {
                                exposeRedirectionAs = ((KeyOnlyFirstOrLastBySpec) inputAggregationSpec)
                                        .getResultColumn();
                            } else {
                                exposeRedirectionAs = null;
                            }

                            if (table.isRefreshing()) {
                                if (isStream) {
                                    operators.add(isFirst ? new StreamFirstChunkedOperator(comboMatchPairs, table)
                                            : new StreamLastChunkedOperator(comboMatchPairs, table));
                                } else if (isAddOnly) {
                                    operators.add(new AddOnlyFirstOrLastChunkedOperator(isFirst, comboMatchPairs, table,
                                            exposeRedirectionAs));
                                } else {
                                    if (trackedFirstOrLastIndex >= 0) {
                                        final IterativeChunkedAggregationOperator operator =
                                                operators.get(trackedFirstOrLastIndex);
                                        final FirstOrLastChunkedOperator firstOrLastChunkedOperator =
                                                (FirstOrLastChunkedOperator) operator;
                                        operators.add(firstOrLastChunkedOperator.makeSecondaryOperator(isFirst,
                                                comboMatchPairs, table, exposeRedirectionAs));
                                    } else {
                                        operators.add(new FirstOrLastChunkedOperator(isFirst, comboMatchPairs, table,
                                                exposeRedirectionAs));
                                        trackedFirstOrLastIndex = operators.size() - 1;
                                    }
                                }
                            } else {
                                operators.add(new StaticFirstOrLastChunkedOperator(isFirst, comboMatchPairs, table,
                                        exposeRedirectionAs));
                            }
                            inputNames.add(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
                        } else if (isAggGroup) {
                            if (isStream) {
                                throw streamUnsupported("AggGroup");
                            }
                            inputColumns.add(null);
                            operators.add(new GroupByChunkedOperator((QueryTable) table, true, comboMatchPairs));
                            inputNames.add(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
                        } else if (isFormula) {
                            if (isStream) {
                                throw streamUnsupported("AggFormula");
                            }
                            final AggregationFormulaSpec formulaStateFactory =
                                    (AggregationFormulaSpec) inputAggregationSpec;
                            final GroupByChunkedOperator groupByChunkedOperator =
                                    new GroupByChunkedOperator((QueryTable) table, false,
                                            Arrays.stream(comboMatchPairs).map(MatchPair::rightColumn)
                                                    .map(MatchPairFactory::getExpression).toArray(MatchPair[]::new));
                            final FormulaChunkedOperator formulaChunkedOperator = new FormulaChunkedOperator(
                                    groupByChunkedOperator, true, formulaStateFactory.getFormula(),
                                    formulaStateFactory.getColumnParamName(), comboMatchPairs);
                            inputColumns.add(null);
                            operators.add(formulaChunkedOperator);
                            inputNames.add(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
                        } else if (isWeightedAverage || isWeightedSum) {
                            final String weightName;

                            if (isWeightedAverage) {
                                weightName = ((WeightedAverageSpecImpl) inputAggregationSpec)
                                        .getWeightName();
                            } else {
                                weightName =
                                        ((WeightedSumSpecImpl) inputAggregationSpec).getWeightName();
                            }

                            final ColumnSource<?> weightSource = table.getColumnSource(weightName);
                            final DoubleWeightRecordingInternalOperator weightOperator =
                                    new DoubleWeightRecordingInternalOperator(weightSource.getChunkType());
                            inputColumns.add(weightSource);
                            operators.add(weightOperator);

                            inputNames.add(Stream
                                    .concat(Stream.of(weightName),
                                            Arrays.stream(comboMatchPairs).map(MatchPair::rightColumn))
                                    .toArray(String[]::new));

                            Arrays.stream(comboMatchPairs).forEach(mp -> {
                                final ColumnSource<?> columnSource = table.getColumnSource(mp.rightColumn());
                                inputColumns.add(columnSource);
                                inputNames.add(new String[] {weightName, mp.rightColumn()});
                                if (isWeightedAverage) {
                                    operators.add(new ChunkedWeightedAverageOperator(columnSource.getChunkType(),
                                            weightOperator, mp.leftColumn()));
                                } else {
                                    operators.add(new DoubleChunkedWeightedSumOperator(columnSource.getChunkType(),
                                            weightOperator, mp.leftColumn()));
                                }
                            });
                        } else {
                            throw new UnsupportedOperationException(
                                    "Unknown AggregationElementImpl: " + inputAggregationSpec.getClass());
                        }
                    }
                } else if (aggregationElement instanceof NullAggregationElement) {
                    transformers.add(new NullColumnAggregationTransformer(
                            ((NullAggregationElement) aggregationElement).resultColumns));
                } else if (aggregationElement instanceof PartitionAggregationElement) {
                    if (!isRollup) {
                        throw new IllegalStateException("PartitionAggregationElement must be used only with rollups.");
                    }
                    inputColumns.add(null);
                    inputNames.add(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
                    final boolean includeConstituents = ((PartitionAggregationElement) aggregationElement).leafLevel;
                    if (includeConstituents) {
                        if (isStream) {
                            throw streamUnsupported("rollup with included constituents");
                        }
                        Assert.eqFalse(secondLevel, "secondLevel");
                    }

                    final QueryTable parentTable = (QueryTable) table;
                    final QueryTable adjustedTable;
                    final List<String> columnsToDrop =
                            parentTable.getDefinition().getColumnStream().map(ColumnDefinition::getName)
                                    .filter(cn -> cn.endsWith(ROLLUP_COLUMN_SUFFIX)).collect(Collectors.toList());
                    if (!columnsToDrop.isEmpty()) {
                        adjustedTable = (QueryTable) parentTable.dropColumns(columnsToDrop);
                    } else {
                        if (includeConstituents) {
                            adjustedTable = (QueryTable) parentTable.updateView(RollupInfo.ROLLUP_COLUMN + "=" + null);
                        } else {
                            adjustedTable = parentTable;
                        }
                    }
                    if (adjustedTable != parentTable && parentTable.hasAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE)) {
                        adjustedTable.setAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE,
                                parentTable.getAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE));
                    }
                    final PartitionByChunkedOperator.AttributeCopier copier;
                    if (includeConstituents) {
                        copier = RollupAttributeCopier.LEAF_WITHCONSTITUENTS_INSTANCE;
                    } else {
                        copier = RollupAttributeCopier.DEFAULT_INSTANCE;
                    }
                    final PartitionByChunkedOperator tableMapOperator = new PartitionByChunkedOperator(parentTable,
                            adjustedTable, copier, Collections.emptyList(), groupByColumns);
                    operators.add(tableMapOperator);

                    if (groupByColumns.length == 0) {
                        transformers.add(new StaticColumnSourceTransformer(RollupInfo.ROLLUP_COLUMN,
                                new SingleValueObjectColumnSource<>(SmartKey.EMPTY)));
                    } else if (groupByColumns.length == 1) {
                        transformers.add(new RollupKeyColumnDuplicationTransformer(groupByColumns[0]));
                    } else {
                        transformers.add(new RollupSmartKeyColumnDuplicationTransformer(groupByColumns));
                    }

                    transformers.add(new RollupTableMapAndReverseLookupAttributeSetter(tableMapOperator, this,
                            secondLevel, includeConstituents));

                    externalFound = true;
                } else {
                    throw new UnsupportedOperationException(
                            "Unknown AggregationElement: " + aggregationElement.getClass());
                }
            }

            if (!externalFound && isRollup && !secondLevel) {
                transformers.add(new NoKeyLeafRollupAttributeSetter());
            }

            final IterativeChunkedAggregationOperator[] operatorsArray = operators.toArray(
                    IterativeChunkedAggregationOperator.ZERO_LENGTH_ITERATIVE_CHUNKED_AGGREGATION_OPERATOR_ARRAY);
            final AggregationContextTransformer[] transformersArray = transformers
                    .toArray(AggregationContextTransformer.ZERO_LENGTH_AGGREGATION_CONTEXT_TRANSFORMER_ARRAY);
            final String[][] inputNamesArray = inputNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY_ARRAY);
            // noinspection unchecked
            final ChunkSource.WithPrev<Values>[] inputColumnsArray =
                    inputColumns.toArray(ChunkSource.WithPrev.ZERO_LENGTH_CHUNK_SOURCE_WITH_PREV_ARRAY);

            return new AggregationContext(operatorsArray, inputNamesArray, inputColumnsArray, transformersArray, true);
        };
    }

    private static UnsupportedOperationException streamUnsupported(@NotNull final String operatorTypeName) {
        return new UnsupportedOperationException("Stream tables do not support " + operatorTypeName
                + "; use StreamTableTools.streamToAppendOnlyTable to accumulate full history");
    }

    @NotNull
    private static String makeRedirectionName(IterativeIndexSpec inputAggregationStateFactory) {
        return IterativeIndexSpec.ROW_REDIRECTION_PREFIX + inputAggregationStateFactory.rollupColumnIdentifier
                + ROLLUP_COLUMN_SUFFIX;
    }

    private static class RollupTableMapAndReverseLookupAttributeSetter implements AggregationContextTransformer {
        private final PartitionByChunkedOperator tableMapOperator;
        private final AggregationFactory factory;
        private final boolean secondLevel;
        private final boolean includeConstituents;
        private ReverseLookup reverseLookup;

        RollupTableMapAndReverseLookupAttributeSetter(PartitionByChunkedOperator tableMapOperator,
                AggregationFactory factory, boolean secondLevel, boolean includeConstituents) {
            this.tableMapOperator = tableMapOperator;
            this.factory = factory;
            this.secondLevel = secondLevel;
            this.includeConstituents = includeConstituents;
        }

        @Override
        public QueryTable transformResult(QueryTable table) {
            table.setAttribute(QueryTable.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE, tableMapOperator.getTableMap());
            if (secondLevel || includeConstituents) {
                table.setAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE, reverseLookup);
            } else {
                setLeafRollupAttributes(table);
            }
            return table;
        }

        @Override
        public void setReverseLookupFunction(ToIntFunction<Object> reverseLookup) {
            this.reverseLookup = new ReverseLookupAdapter(reverseLookup);
        }

        private class ReverseLookupAdapter implements ReverseLookup {
            private final ToIntFunction<Object> reverseLookup;

            public ReverseLookupAdapter(ToIntFunction<Object> reverseLookup) {
                this.reverseLookup = reverseLookup;
            }

            @Override
            public long get(Object key) {
                return reverseLookup.applyAsInt(key);
            }

            @Override
            public long getPrev(Object key) {
                return get(key);
            }

            @Override
            public long getNoEntryValue() {
                return -1;
            }

            @Override
            public String[] getKeyColumns() {
                return factory.getKeyColumns();
            }
        }
    }

    private static class NoKeyLeafRollupAttributeSetter implements AggregationContextTransformer {
        @Override
        public QueryTable transformResult(QueryTable table) {
            setLeafRollupAttributes(table);
            return table;
        }
    }

    private static void setLeafRollupAttributes(QueryTable table) {
        table.setAttribute(Table.ROLLUP_LEAF_ATTRIBUTE, RollupInfo.LeafType.Normal);
        table.setAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE, EmptyTableMap.INSTANCE);
        table.setAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE, ReverseLookup.NULL);
    }

}

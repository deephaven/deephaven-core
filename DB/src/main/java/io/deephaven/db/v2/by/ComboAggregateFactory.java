/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.v2.by;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.AbsSum;
import io.deephaven.api.agg.Aggregation;
import io.deephaven.api.agg.Array;
import io.deephaven.api.agg.Avg;
import io.deephaven.api.agg.Count;
import io.deephaven.api.agg.CountDistinct;
import io.deephaven.api.agg.Distinct;
import io.deephaven.api.agg.First;
import io.deephaven.api.agg.Last;
import io.deephaven.api.agg.Max;
import io.deephaven.api.agg.Med;
import io.deephaven.api.agg.Min;
import io.deephaven.api.agg.Multi;
import io.deephaven.api.agg.Pair;
import io.deephaven.api.agg.Pct;
import io.deephaven.api.agg.SortedFirst;
import io.deephaven.api.agg.SortedLast;
import io.deephaven.api.agg.Std;
import io.deephaven.api.agg.Sum;
import io.deephaven.api.agg.Unique;
import io.deephaven.api.agg.Var;
import io.deephaven.api.agg.WAvg;
import io.deephaven.api.agg.WSum;
import io.deephaven.base.verify.Assert;
import io.deephaven.datastructures.util.CollectionUtil;
import io.deephaven.datastructures.util.SmartKey;
import io.deephaven.db.tables.ColumnDefinition;
import io.deephaven.db.tables.Table;
import io.deephaven.db.tables.select.MatchPair;
import io.deephaven.db.tables.select.MatchPairFactory;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.util.tuples.generated.ByteDoubleTuple;
import io.deephaven.db.v2.BaseTable;
import io.deephaven.db.v2.QueryTable;
import io.deephaven.db.v2.ReverseLookup;
import io.deephaven.db.v2.RollupAttributeCopier;
import io.deephaven.db.v2.RollupInfo;
import io.deephaven.db.v2.TableMap;
import io.deephaven.db.v2.by.ssmminmax.SsmChunkedMinMaxOperator;
import io.deephaven.db.v2.sources.ColumnSource;
import io.deephaven.db.v2.sources.ObjectArraySource;
import io.deephaven.db.v2.sources.ReinterpretUtilities;
import io.deephaven.db.v2.sources.SingleValueObjectColumnSource;
import io.deephaven.db.v2.sources.chunk.Attributes.Values;
import io.deephaven.db.v2.sources.chunk.ChunkSource;
import io.deephaven.db.v2.sources.chunk.ChunkType;
import io.deephaven.db.v2.ssms.SegmentedSortedMultiSet;
import org.jetbrains.annotations.NotNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalInt;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

/**
 * The ComboAggregateFactory combines one or more aggregations into an operator for use with {@link Table#by(AggregationStateFactory)}.
 *
 * <p>The intended use of this class is to call the {@link #AggCombo(ComboBy...)} method with a set of aggregations
 * defined by:
 * <ul>
 *     <li>{@link #AggMin}</li>
 *     <li>{@link #AggMax}</li>
 *     <li>{@link #AggSum}</li>
 *     <li>{@link #AggAbsSum}</li>
 *     <li>{@link #AggVar}</li>
 *     <li>{@link #AggAvg}</li>
 *     <li>{@link #AggWAvg}</li>
 *     <li>{@link #AggWSum}</li>
 *     <li>{@link #AggMed}</li>
 *     <li>{@link #AggPct}</li>
 *     <li>{@link #AggStd}</li>
 *     <li>{@link #AggFirst}</li>
 *     <li>{@link #AggLast}</li>
 *     <li>{@link #AggCount}</li>
 *     <li>{@link #AggCountDistinct}</li>
 *     <li>{@link #AggDistinct}</li>
 *     <li>{@link #AggArray}</li>
 *     <li>{@link #AggSortedFirst}</li>
 *     <li>{@link #AggSortedLast}</li>
 * </ul>
 *
 * <p>For example, to produce a table with several aggregations on the LastPrice of a Trades table:
 * {@code ohlc=trades.by(AggCombo(AggFirst("Open=LastPrice"), AggLast("Close=LastPrice"), AggMax("High=LastPrice"), AggMin("Low=LastPrice"), AggSum("Volume=Size"), AggWAvg("Size", "VWAP=LastPrice"), "Symbol")}</p>
 */
public class ComboAggregateFactory implements AggregationStateFactory {
    static final String ROLLUP_RUNNING_SUM_COLUMN_ID = "_RS_";
    static final String ROLLUP_RUNNING_SUM2_COLUMN_ID = "_RS2_";
    static final String ROLLUP_NONNULL_COUNT_COLUMN_ID = "_NNC_";
    static final String ROLLUP_NAN_COLUMN_ID = "_NaN_";
    static final String ROLLUP_PIC_COLUMN_ID = "_PIC_";
    static final String ROLLUP_NIC_COLUMN_ID = "_NIC_";
    public static final String ROLLUP_DISTINCT_SSM_COLUMN_ID = "_SSM_";

    private final List<ComboBy> underlyingAggregations = new ArrayList<>();
    private final boolean isRollup;
    private final boolean secondLevel;

    public static final String ROLLUP_COLUMN_SUFFIX = "__ROLLUP__";

    /**
     * Create a new ComboAggregateFactory suitable for passing to {@link Table#by(AggregationStateFactory, String...)}.
     *
     * @param aggregations the aggregations to compute
     *
     * @return a new table with the specified aggregations.
     */
    public static ComboAggregateFactory AggCombo(ComboBy... aggregations) {
        return new ComboAggregateFactory(aggregations);
    }

    /**
     * Create a formula aggregation.
     *
     * @param formula the formula to apply to each group
     * @param formulaParam the parameter name within the formula
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggFormula(String formula, String formulaParam, final String... matchPairs) {
        return new ComboByImpl(new AggregationFormulaStateFactory(formula, formulaParam), matchPairs);
    }

    /**
     * Create a minimum aggregation, equivalent to {@link Table#minBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggMin(final String... matchPairs) {
        return Agg(AggType.Min, matchPairs);
    }

    /**
     * Create a maximum aggregation, equivalent to {@link Table#maxBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggMax(final String... matchPairs) {
        return Agg(AggType.Max, matchPairs);
    }

    /**
     * Create a summation aggregation, equivalent to {@link Table#sumBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggSum(final String... matchPairs) {
        return Agg(AggType.Sum, matchPairs);
    }

    /**
     * Create an absolute sum aggregation, equivalent to {@link Table#absSumBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggAbsSum(final String... matchPairs) {
        return Agg(AggType.AbsSum, matchPairs);
    }

    /**
     * Create a variance aggregation, equivalent to {@link Table#varBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggVar(final String... matchPairs) {
        return Agg(AggType.Var, matchPairs);
    }

    /**
     * Create an average aggregation, equivalent to {@link Table#avgBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggAvg(final String... matchPairs) {
        return Agg(AggType.Avg, matchPairs);
    }

    /**
     * Create a weighted average aggregation, equivalent to {@link Table#wavgBy(String, String...)}.
     *
     * @param weight the name of the column to use as the weight for the average
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggWAvg(final String weight, final String... matchPairs) {
        return Agg(new WeightedAverageStateFactoryImpl(weight), matchPairs);
    }

    /**
     * Create a weighted sum aggregation, equivalent to {@link Table#wsumBy(String, String...)}.
     *
     * @param weight the name of the column to use as the weight for the sum
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggWSum(final String weight, final String... matchPairs) {
        return Agg(new WeightedSumStateFactoryImpl(weight), matchPairs);
    }

    /**
     * Create a median aggregation, equivalent to {@link Table#medianBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggMed(final String... matchPairs) {
        return AggPct(0.50d, true, matchPairs);
    }

    /**
     * Create a standard deviation aggregation, equivalent to {@link Table#stdBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggStd(final String... matchPairs) {
        return Agg(AggType.Std, matchPairs);
    }

    /**
     * Create a first aggregation, equivalent to {@link Table#firstBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggFirst(final String... matchPairs) {
        return Agg(AggType.First, matchPairs);
    }

    /**
     * Create a last aggregation, equivalent to {@link Table#lastBy(String...)}.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggLast(final String... matchPairs) {
        return Agg(AggType.Last, matchPairs);
    }
    
    /**
     * Create a sorted first aggregation, equivalent to {@link io.deephaven.db.util.SortedBy#sortedFirstBy}.
     *
     * @param sortColumn the column to sort by
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggSortedFirst(final String sortColumn, final String... matchPairs) {
        return Agg(new SortedFirstBy(sortColumn), matchPairs);
    }

    /**
     * Create a sorted last aggregation, equivalent to {@link io.deephaven.db.util.SortedBy#sortedLastBy}.
     *
     * @param sortColumn the column to sort by
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggSortedLast(final String sortColumn, final String... matchPairs) {
        return Agg(new SortedLastBy(sortColumn), matchPairs);
    }

    /**
     * Create a sorted first aggregation, equivalent to {@link io.deephaven.db.util.SortedBy#sortedFirstBy}.
     *
     * @param sortColumns the column to sort by
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggSortedFirst(final String [] sortColumns, final String... matchPairs) {
        return Agg(new SortedFirstBy(sortColumns), matchPairs);
    }

    /**
     * Create a sorted last aggregation, equivalent to {@link io.deephaven.db.util.SortedBy#sortedLastBy}.
     *
     * @param sortColumns the columns to sort by
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggSortedLast(final String [] sortColumns, final String... matchPairs) {
        return Agg(new SortedLastBy(sortColumns), matchPairs);
    }

    /**
     * Create an array aggregation, equivalent to {@link Table#by(String...)}.
     * 
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggArray(final String... matchPairs) {
        return Agg(AggType.Array, matchPairs);
    }

    /**
     * Create an count aggregation, equivalent to {@link Table#countBy(String)}.
     *
     * @param resultColumn the name of the result column containing the count of each group
     *
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggCount(final String resultColumn) {
        return new CountComboBy(resultColumn);
    }

    /**
     * Create a distinct count aggregation.
     *
     * The output column contains the number of distinct values for the input column in that group.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}.  Null values are not counted.
     */
    public static ComboBy AggCountDistinct(final String... matchPairs) { return AggCountDistinct(false, matchPairs); }

    /**
     * Create a distinct count aggregation.
     *
     * The output column contains the number of distinct values for the input column in that group.
     *
     * @param countNulls if true null values are counted as a distinct value, otherwise null values are ignored
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggCountDistinct(boolean countNulls, final String... matchPairs) {
        return Agg(new CountDistinctStateFactory(countNulls), matchPairs);
    }

    /**
     * Create a distinct aggregation.
     *
     * The output column contains a {@link io.deephaven.db.tables.dbarrays.DbArrayBase} with the distinct values for
     * the input column within the group.
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}.  Null values are ignored.
     */
    public static ComboBy AggDistinct(final String... matchPairs) { return AggDistinct(false, matchPairs); }


    /**
     * Create a distinct aggregation.
     *
     * The output column contains a {@link io.deephaven.db.tables.dbarrays.DbArrayBase} with the distinct values for
     * the input column within the group.
     *
     * @param countNulls if true, then null values are included in the result, otherwise null values are ignored
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggDistinct(boolean countNulls, final String... matchPairs) {
        return Agg(new DistinctStateFactory(countNulls), matchPairs);
    }

    /**
     * Create a Unique aggregation.
     *
     * The output column contains a value of the same type as the input column which contains<br>
     *     <ul>
     *         <li>The "no key value" - if there are no values present </li>
     *         <li>The single unique value - if there is only a single value present</li>
     *         <li>The "non unique value" - if there are more than 1 distinct values present</li>
     *     </ul>
     *
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggUnique(final String... matchPairs) {
        return Agg(new UniqueStateFactory(false), matchPairs);
    }

    /**
     * Create a Unique aggregation.
     *
     * The output column contains a value of the same type as the input column which contains<br>
     *     <ul>
     *         <li>The "no key value" - if there are no values present </li>
     *         <li>The single unique value - if there is only a single value present</li>
     *         <li>The "non unique value" - if there are more than 1 distinct values present</li>
     *     </ul>
     *
     * @param countNulls if true, then null values are included in the result, otherwise null values are ignored
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}.  Output columns contain null if
     *          there are no values present or there are more than 1 distinct values present.
     */
    public static ComboBy AggUnique(boolean countNulls, final String... matchPairs) {
        return AggUnique(countNulls, null, null, matchPairs);
    }

    /**
     * Create a Unique aggregation.
     *
     * The output column contains a value of the same type as the input column which contains<br>
     *     <ul>
     *         <li>The "no key value" - if there are no values present </li>
     *         <li>The single unique value - if there is only a single value present</li>
     *         <li>The "non unique value" - if there are more than 1 distinct values present</li>
     *     </ul>
     *
     * @param countNulls if true, then null values are included in the result, otherwise null values are ignored
     * @param noKeyValue the value to use if there are no values present
     * @param nonUniqueValue the value to use if there are more than 1 values present
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggUnique(boolean countNulls, Object noKeyValue, Object nonUniqueValue, final String... matchPairs) {
        return Agg(new UniqueStateFactory(countNulls, noKeyValue, nonUniqueValue), matchPairs);
    }

    /**
     * Create a percentile aggregation.
     *
     * @param percentile the percentile to calculate
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggPct(double percentile, final String... matchPairs) {
        return Agg(new PercentileByStateFactoryImpl(percentile), matchPairs);
    }

    /**
     * Create a percentile aggregation.
     *
     * @param percentile the percentile to calculate
     * @param averageMedian if true, then when the upper values and lower values have an equal size; average the highest
     *                      lower value and lowest upper value to produce the median value for integers, longs, doubles,
     *                      and floats
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy AggPct(double percentile, boolean averageMedian, final String... matchPairs) {
        return Agg(new PercentileByStateFactoryImpl(percentile, averageMedian), matchPairs);
    }

    /**
     * Create an aggregation.
     *
     * @param factory aggregation factory.
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy Agg(AggregationStateFactory factory, final String... matchPairs) {
        return new ComboByImpl(factory, matchPairs);
    }

    /**
     * Create an aggregation.
     *
     * @param factory aggregation factory.
     * @param matchPairs the columns to apply the aggregation to.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy Agg(AggregationStateFactory factory, final MatchPair... matchPairs) {
        return new ComboByImpl(factory, matchPairs);
    }

    /**
     * Create an aggregation.
     *
     * @param factoryType aggregation factory type.
     * @param matchPairs the columns to apply the aggregation to in the form Output=Input, if the Output and Input have
     *                   the same name, then the column name can be specified.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy Agg(AggType factoryType, final String... matchPairs) {
        return Agg(factoryType, MatchPairFactory.getExpressions(matchPairs));
    }

    /**
     * Create an aggregation.
     *
     * @param factoryType aggregation factory type.
     * @param matchPairs the columns to apply the aggregation to.
     * @return a ComboBy object suitable for passing to {@link #AggCombo(ComboBy...)}
     */
    public static ComboBy Agg(AggType factoryType, final MatchPair... matchPairs) {
        final AggregationStateFactory factory;
        switch (factoryType) {
            case Min:
                factory = new MinMaxByStateFactoryImpl(true);
                break;
            case Max:
                factory = new MinMaxByStateFactoryImpl(false);
                break;
            case Sum:
                factory = new SumStateFactory();
                break;
            case AbsSum:
                factory = new AbsSumStateFactory();
                break;
            case Avg:
                factory = new AvgStateFactory();
                break;
            case Var:
                factory = new VarStateFactory();
                break;
            case Std:
                factory = new StdStateFactory();
                break;
            case First:
                factory = new FirstByStateFactoryImpl();
                break;
            case Last:
                factory = new LastByStateFactoryImpl();
                break;
            case Array:
                factory = new AggregationIndexStateFactory();
                break;
            case CountDistinct:
                factory = new CountDistinctStateFactory();
                break;
            case Distinct:
                factory = new DistinctStateFactory();
                break;
            case Unique:
                factory = new UniqueStateFactory();
                break;
            case Skip:
                throw new IllegalArgumentException("Skip is not a valid aggregation type for AggCombo.");
            default:
                throw new UnsupportedOperationException("Unknown AggType: " + factoryType);
        }
        return new ComboByImpl(factory, matchPairs);
    }

    /**
     * Create a factory for performing rollups.
     */
    public ComboAggregateFactory rollupFactory() {
        // we want to leave off the null value column source for children; but add a by external combo for the rollup
        return new ComboAggregateFactory(Stream.concat(underlyingAggregations.subList(0, underlyingAggregations.size() - 1).stream().map(x -> {
            final AggregationStateFactory underlyingStateFactory = x.getUnderlyingStateFactory();
            Assert.assertion(underlyingStateFactory instanceof ReaggregatableStatefactory, "underlyingStateFactory instanceof ReaggregatableStatefactory", underlyingStateFactory, "UnderlyingStateFactory");

            //noinspection ConstantConditions
            final ReaggregatableStatefactory reaggregatableStatefactory = (ReaggregatableStatefactory) underlyingStateFactory;

            Assert.assertion(reaggregatableStatefactory.supportsRollup(), "((ReaggregatableStatefactory)x.getUnderlyingStateFactory()).supportsRollup()", underlyingStateFactory, "UnderlyingStateFactory");
            final ReaggregatableStatefactory factory = reaggregatableStatefactory.rollupFactory();

            final List<String> leftColumns = new ArrayList<>();
            Collections.addAll(leftColumns, MatchPair.getLeftColumns(x.getResultPairs()));

            return Agg(factory, leftColumns.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY));
        }), Stream.of(new ExternalComboBy(false))).collect(Collectors.toList()), true, true);
    }

    public ComboAggregateFactory forRollup(boolean includeConstituents) {
        final List<ComboBy> newUnderliers = underlyingAggregations.stream().map(ComboBy::forRollup).collect(Collectors.toList());
        newUnderliers.add(includeConstituents ? new ExternalComboBy(true)
                                              : new NullComboBy(Collections.singletonMap(RollupInfo.ROLLUP_COLUMN, Object.class)));
        return new ComboAggregateFactory(newUnderliers, true, false);
    }

    /**
     * Create a new factory that will have columns with all null values.
     *
     * Used by rollup to empty out unused grouping columns.
     *
     * @param nullColumns a map of column names to types.
     *
     * @return a new ComboAggregateFactory that will produce null values for the given columns.
     */
    public ComboAggregateFactory withNulls(Map<String, Class> nullColumns) {
        final List<ComboBy> newAggregations = new ArrayList<>(underlyingAggregations.size() + 1);
        newAggregations.add(new NullComboBy(nullColumns));
        newAggregations.addAll(underlyingAggregations);
        return new ComboAggregateFactory(newAggregations, isRollup, secondLevel);
    }

    private static final String[] ROLLUP_KEY_COLUMNS = {RollupInfo.ROLLUP_COLUMN};
    private String[] getKeyColumns() {
        return ROLLUP_KEY_COLUMNS;
    }

    public interface ComboBy {

        /**
         * Equivalent to {@code optimize(Collections.singleton(aggregation))}.
         *
         * @param aggregation the aggregation
         * @return the optimized combos
         * @see #optimize(Collection)
         */
        static List<ComboBy> optimize(Aggregation aggregation) {
            return optimize(Collections.singleton(aggregation));
        }

        /**
         * Optimizes the aggregations, collapsing relevant aggregations into single {@link ComboBy comboBys} where
         * applicable.
         *
         * <p>
         * Note: due to the optimization, the combo bys may not be in the same order as specified in {@code aggregations}.
         *
         * @param aggregations the aggregations
         * @return the optimized combos
         */
        static List<ComboBy> optimize(Collection<? extends Aggregation> aggregations) {
            ComboByAggregationAdapterOptimizer builder = new ComboByAggregationAdapterOptimizer();
            for (Aggregation a : aggregations) {
                a.walk(builder);
            }
            return builder.build();
        }

        AggregationStateFactory getUnderlyingStateFactory();
        String [] getSourceColumns();
        MatchPair [] getResultPairs();
        ComboBy forRollup();
        AggregationMemoKey getMemoKey();
    }

    static public class ComboByImpl implements ComboBy {
        private final MatchPair [] matchPairs;
        private final String [] rightColumns;
        private final AggregationStateFactory underlyingStateFactory;

        public ComboByImpl(final AggregationStateFactory underlyingStateFactory, final String... matchPairs) {
            this(underlyingStateFactory, MatchPairFactory.getExpressions(matchPairs));
        }

        @SuppressWarnings("unused")
        public ComboByImpl(final AggregationStateFactory underlyingStateFactory, final Collection<String> matchPairs) {
            this(underlyingStateFactory, MatchPairFactory.getExpressions(matchPairs));
        }

        ComboByImpl(final AggregationStateFactory underlyingStateFactory, final MatchPair... matchPairs) {
            this.matchPairs = matchPairs;
            this.underlyingStateFactory = underlyingStateFactory;
            this.rightColumns = new String[matchPairs.length];
            for (int ii = 0; ii < matchPairs.length; ++ii) {
                this.rightColumns[ii] = this.matchPairs[ii].rightColumn;
            }
        }

        @Override
        public AggregationStateFactory getUnderlyingStateFactory() {
            return underlyingStateFactory;
        }

        @Override
        public String [] getSourceColumns() {
            return rightColumns;
        }

        @Override
        public MatchPair [] getResultPairs() {
            return matchPairs;
        }

        @Override
        public ComboBy forRollup() {
            if (!(underlyingStateFactory instanceof ReaggregatableStatefactory)) {
                throw new UnsupportedOperationException("Not a reaggregatable state factory: " + underlyingStateFactory);
            }
            if (!((ReaggregatableStatefactory) underlyingStateFactory).supportsRollup()) {
                throw new UnsupportedOperationException("Underlying state factory does not support rollup: " + underlyingStateFactory);
            }
            return new ComboByImpl(((ReaggregatableStatefactory) underlyingStateFactory).forRollup(), matchPairs);
        }

        @Override
        public AggregationMemoKey getMemoKey() {
            return getUnderlyingStateFactory().getMemoKey();
        }

        @Override
        public String toString() {
            return "ComboByImpl{" +
                    "matchPairs=" + Arrays.toString(matchPairs) +
                    ", underlyingStateFactory=" + underlyingStateFactory +
                    '}';
        }
    }

    static public class CountComboBy implements ComboBy {
        private final String resultColumn;
        private final CountByStateFactoryImpl underlyingStateFactory;

        public CountComboBy(String resultColumn) {
            this.resultColumn = resultColumn;
            underlyingStateFactory = new CountByStateFactoryImpl(resultColumn);
        }

        @Override
        public AggregationStateFactory getUnderlyingStateFactory() {
            return underlyingStateFactory;
        }

        @Override
        public String [] getSourceColumns() {
            return new String[0];
        }

        @Override
        public MatchPair [] getResultPairs() {
            return new MatchPair[]{new MatchPair(resultColumn, resultColumn)};
        }

        @Override
        public ComboBy forRollup() {
            return this;
        }


        @Override
        public AggregationMemoKey getMemoKey() {
            return getUnderlyingStateFactory().getMemoKey();
        }

        @Override
        public String toString() {
            return "Count(" + resultColumn + ")";
        }
    }

    static public class NullComboBy implements ComboBy {
        private final Map<String, Class> resultColumns;
        private final NullAggregationStateFactoryImpl underlyingStateFactory;

        NullComboBy(Map<String, Class> resultColumns) {
            this.resultColumns = resultColumns;
            this.underlyingStateFactory = new NullAggregationStateFactoryImpl();
        }

        @Override
        public AggregationStateFactory getUnderlyingStateFactory() {
            return underlyingStateFactory;
        }

        @Override
        public String [] getSourceColumns() {
            return new String[0];
        }

        @Override
        public MatchPair [] getResultPairs() {
            return resultColumns.keySet().stream().map(rc -> new MatchPair(rc, rc)).toArray(MatchPair[]::new);
        }

        @Override
        public ComboBy forRollup() {
            throw new UnsupportedOperationException();
        }

        @Override
        public AggregationMemoKey getMemoKey() {
            return getUnderlyingStateFactory().getMemoKey();
        }

        @Override
        public String toString() {
            return "NullComboBy(" + resultColumns + ')';
        }
    }

    static private class ExternalComboBy implements ComboBy {
        private final boolean leafLevel;

        private ExternalComboBy(boolean leafLevel) {
            this.leafLevel = leafLevel;
        }

        @Override
        public AggregationStateFactory getUnderlyingStateFactory() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String [] getSourceColumns() {
            return new String[0];
        }

        @Override
        public MatchPair [] getResultPairs() {
            return new MatchPair[]{new MatchPair(RollupInfo.ROLLUP_COLUMN, RollupInfo.ROLLUP_COLUMN)};
        }

        @Override
        public ComboBy forRollup() {
            return this;
        }

        @Override
        public AggregationMemoKey getMemoKey() {
            // TODO: MEMOIZE!
            return null;
        }

        @Override
        public String toString() {
            return "ExternalComboBy(leafLevel=" + leafLevel + ')';
        }

    }

    public ComboAggregateFactory(Collection<ComboBy> aggregations) {
        this(aggregations, false, false);
    }

    public ComboAggregateFactory(Collection<ComboBy> aggregations, boolean isRollup, boolean secondLevelRollup) {
        this.isRollup = isRollup;
        this.secondLevel = secondLevelRollup;
        underlyingAggregations.addAll(aggregations);

        final Map<String, List<ComboBy>> usedColumns = new LinkedHashMap<>();

        for (final ComboBy comboBy : underlyingAggregations) {
            Stream.of(comboBy.getResultPairs()).map(MatchPair::left).forEach(rl -> usedColumns.computeIfAbsent(rl, x -> new ArrayList<>()).add(comboBy));
        }

        final Map<String, List<ComboBy>> duplicates = usedColumns.entrySet().stream().filter(kv -> kv.getValue().size() > 1).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        if (!duplicates.isEmpty()) {
            final String errors = duplicates.entrySet().stream().map(kv -> kv.getKey() + " used " + kv.getValue().size() + " times").collect(Collectors.joining(", "));
            throw new IllegalArgumentException("Duplicate output columns: " + errors);
        }
    }

    public ComboAggregateFactory(ComboBy... aggregations)
    {
        this(Arrays.asList(aggregations), false, false);
    }

    @Override
    public AggregationMemoKey getMemoKey() {
        final UnderlyingMemoKey [] underlyingMemoKeys = new UnderlyingMemoKey[underlyingAggregations.size()];
        for (int ii = 0; ii < underlyingMemoKeys.length; ++ii) {
            final ComboBy comboBy = underlyingAggregations.get(ii);
            final AggregationMemoKey key = comboBy.getMemoKey();
            if (key == null) {
                return null;
            }
            underlyingMemoKeys[ii] = new UnderlyingMemoKey(key, comboBy.getSourceColumns(), comboBy.getResultPairs());
        }

        return new ComboByMemoKey(underlyingMemoKeys);
    }

    private static class UnderlyingMemoKey {
        private final AggregationMemoKey componentMemoKey;
        private final String [] sourceColumns;
        private final MatchPair [] resultPairs;


        private UnderlyingMemoKey(AggregationMemoKey componentMemoKey, String[] sourceColumns, MatchPair[] resultPairs) {
            this.componentMemoKey = componentMemoKey;
            this.sourceColumns = sourceColumns;
            this.resultPairs = resultPairs;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
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

    private static class ComboByMemoKey implements AggregationMemoKey {
        private final UnderlyingMemoKey [] underlyingMemoKeys;

        private ComboByMemoKey(UnderlyingMemoKey [] underlyingMemoKeys) {
            this.underlyingMemoKeys = underlyingMemoKeys;
        }

        @Override
        public int hashCode() {
            return Arrays.hashCode(underlyingMemoKeys);
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof ComboByMemoKey && Arrays.equals(underlyingMemoKeys, ((ComboByMemoKey) obj).underlyingMemoKeys);
        }
    }

    @Override
    public String toString() {
        return "ComboAggregateFactory{" + underlyingAggregations + '}';
    }

    public List<MatchPair> getMatchPairs() {
        return underlyingAggregations.stream().flatMap(c -> Arrays.stream(c.getResultPairs())).collect(Collectors.toList());
    }

    public AggregationContextFactory makeAggregationContextFactory() {
        return (table, groupByColumns) -> {
            final List<IterativeChunkedAggregationOperator> operators = new ArrayList<>();
            final List<String[]> inputNames = new ArrayList<>();
            final List<ChunkSource.WithPrev<Values>> inputColumns = new ArrayList<>();
            final List<AggregationContextTransformer> transformers = new ArrayList<>();

            int trackedFirstOrLastIndex = -1;
            boolean externalFound = false;


            for (final ComboBy comboBy : underlyingAggregations) {
                final boolean isStream = ((BaseTable) table).isStream();
                final boolean isAddOnly = ((BaseTable) table).isAddOnly();

                if (comboBy instanceof CountComboBy) {
                    operators.add(new CountAggregationOperator(((CountComboBy) comboBy).resultColumn));
                    inputColumns.add(null);
                    inputNames.add(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
                }
                else if (comboBy instanceof ComboByImpl) {
                    final AggregationStateFactory inputAggregationStateFactory = comboBy.getUnderlyingStateFactory();

                    final boolean isNumeric = inputAggregationStateFactory.getClass() == SumStateFactory.class ||
                            inputAggregationStateFactory.getClass() == AbsSumStateFactory.class ||
                            inputAggregationStateFactory.getClass() == AvgStateFactory.class ||
                            inputAggregationStateFactory.getClass() == VarStateFactory.class ||
                            inputAggregationStateFactory.getClass() == StdStateFactory.class;
                    final boolean isCountDistinct = inputAggregationStateFactory.getClass() == CountDistinctStateFactory.class;
                    final boolean isDistinct = inputAggregationStateFactory.getClass() == DistinctStateFactory.class;
                    final boolean isSelectDistinct = inputAggregationStateFactory.getClass() == SelectDistinctStateFactoryImpl.class;
                    final boolean isAggUnique = inputAggregationStateFactory.getClass() == UniqueStateFactory.class;
                    final boolean isMinMax = inputAggregationStateFactory instanceof MinMaxByStateFactoryImpl;
                    final boolean isPercentile = inputAggregationStateFactory.getClass() == PercentileByStateFactoryImpl.class;
                    final boolean isSortedFirstOrLastBy = inputAggregationStateFactory instanceof SortedFirstOrLastByFactoryImpl;
                    final boolean isFirst = inputAggregationStateFactory.getClass() == FirstByStateFactoryImpl.class ||
                                            inputAggregationStateFactory.getClass() == TrackingFirstByStateFactoryImpl.class ||
                                            (inputAggregationStateFactory.getClass() == KeyOnlyFirstOrLastByStateFactory.class &&
                                                    !((KeyOnlyFirstOrLastByStateFactory)inputAggregationStateFactory).isLast());
                    final boolean isLast = inputAggregationStateFactory.getClass() == LastByStateFactoryImpl.class ||
                                           inputAggregationStateFactory.getClass() == TrackingLastByStateFactoryImpl.class ||
                                          (inputAggregationStateFactory.getClass() == KeyOnlyFirstOrLastByStateFactory.class &&
                                                  ((KeyOnlyFirstOrLastByStateFactory)inputAggregationStateFactory).isLast());
                    final boolean isWeightedAverage = inputAggregationStateFactory.getClass() == WeightedAverageStateFactoryImpl.class;
                    final boolean isWeightedSum = inputAggregationStateFactory.getClass() == WeightedSumStateFactoryImpl.class;
                    final boolean isAggArray = inputAggregationStateFactory.getClass() == AggregationIndexStateFactory.class;
                    final boolean isFormula = inputAggregationStateFactory.getClass() == AggregationFormulaStateFactory.class;

                    //noinspection StatementWithEmptyBody
                    if (isSelectDistinct) {
                        // Select-distinct is accomplished as a side effect of aggregating on the group-by columns.
                    } else {
                        final MatchPair[] comboMatchPairs = ((ComboByImpl) comboBy).matchPairs;
                        if (isSortedFirstOrLastBy) {
                            final SortedFirstOrLastByFactoryImpl sortedFirstOrLastByFactory = (SortedFirstOrLastByFactoryImpl) inputAggregationStateFactory;
                            final boolean isSortedFirstBy = sortedFirstOrLastByFactory.isSortedFirst();

                            final MatchPair[] updatedMatchPairs;
                            if (sortedFirstOrLastByFactory.secondRollup && sortedFirstOrLastByFactory.getSortColumnNames().length == 1 && sortedFirstOrLastByFactory.getSortColumnNames()[0].endsWith(ROLLUP_COLUMN_SUFFIX)) {
                                updatedMatchPairs = Arrays.copyOf(comboMatchPairs, comboMatchPairs.length + 1);
                                final String redirectionName = sortedFirstOrLastByFactory.getSortColumnNames()[0];
                                updatedMatchPairs[updatedMatchPairs.length - 1] = new MatchPair(redirectionName, redirectionName);
                            } else {
                                updatedMatchPairs = comboMatchPairs;
                            }
                            final AggregationContext sflac = SortedFirstOrLastByAggregationFactory.getAggregationContext(table, sortedFirstOrLastByFactory.getSortColumnNames(), isSortedFirstBy, true, updatedMatchPairs);
                            Assert.eq(sflac.operators.length, "sflac.operators.length", 1);
                            Assert.eq(sflac.inputColumns.length, "sflac.operators.length", 1);
                            Assert.eq(sflac.inputNames.length, "sflac.operators.length", 1);
                            operators.add(sflac.operators[0]);
                            inputColumns.add(sflac.inputColumns[0]);
                            inputNames.add(sflac.inputNames[0]);
                        } else if (isNumeric || isMinMax || isPercentile || isCountDistinct || isDistinct || isAggUnique) {
                            // add the stuff
                            Arrays.stream(comboMatchPairs).forEach(mp -> {
                                if (isRollup && secondLevel) {
                                    final boolean isAverage = inputAggregationStateFactory.getClass() == AvgStateFactory.class;
                                    final boolean isStd = inputAggregationStateFactory.getClass() == StdStateFactory.class;
                                    final boolean isVar = inputAggregationStateFactory.getClass() == VarStateFactory.class;
                                    final boolean isStdVar = isStd || isVar;
                                    if (isAverage || isStdVar) {
                                        final String runningSumName = mp.left() + ROLLUP_RUNNING_SUM_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                                        final String runningSum2Name = mp.left() + ROLLUP_RUNNING_SUM2_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                                        final String nonNullName = mp.left() + ROLLUP_NONNULL_COUNT_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                                        final String nanName = mp.left() + ROLLUP_NAN_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                                        final String picName = mp.left() + ROLLUP_PIC_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                                        final String nicName = mp.left() + ROLLUP_NIC_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;

                                        final boolean isFloatingPoint = table.hasColumns(nanName);

                                        // record non null count
                                        //noinspection unchecked
                                        inputColumns.add(table.getColumnSource(nonNullName));
                                        inputNames.add(new String[]{nonNullName});

                                        // record running sum
                                        //noinspection unchecked
                                        inputColumns.add(table.getColumnSource(runningSumName));
                                        inputNames.add(new String[]{runningSumName});

                                        if (isStdVar) {
                                            //noinspection unchecked
                                            inputColumns.add(table.getColumnSource(runningSum2Name));
                                            inputNames.add(new String[]{runningSum2Name});
                                        }

                                        if (isFloatingPoint) {
                                            // record nans, positive and negative infinities
                                            //noinspection unchecked
                                            inputColumns.add(table.getColumnSource(nanName));
                                            inputNames.add(new String[]{nanName});
                                            //noinspection unchecked
                                            inputColumns.add(table.getColumnSource(picName));
                                            inputNames.add(new String[]{picName});
                                            //noinspection unchecked
                                            inputColumns.add(table.getColumnSource(nicName));
                                            inputNames.add(new String[]{nicName});
                                        }

                                        // then the input column for the updater (reavg/revar) operator
                                        inputColumns.add(null);

                                        // now add add the operators, and the final inputNames that matches the updating operator
                                        final LongChunkedSumOperator nonNull = new LongChunkedSumOperator(false, nonNullName);
                                        operators.add(nonNull);

                                        if (isFloatingPoint) {
                                            final DoubleChunkedSumOperator runningSum = new DoubleChunkedSumOperator(false, runningSumName);
                                            operators.add(runningSum);

                                            final DoubleChunkedSumOperator runningSum2;
                                            if (isStdVar) {
                                                runningSum2 = new DoubleChunkedSumOperator(false, runningSum2Name);
                                                operators.add(runningSum2);
                                            } else {
                                                runningSum2 = null;
                                            }

                                            final LongChunkedSumOperator nanSum = new LongChunkedSumOperator(false, nanName);
                                            operators.add(nanSum);
                                            final LongChunkedSumOperator picSum = new LongChunkedSumOperator(false, picName);
                                            operators.add(picSum);
                                            final LongChunkedSumOperator nicSum = new LongChunkedSumOperator(false, nicName);
                                            operators.add(nicSum);

                                            if (isAverage) {
                                                if (table.getColumnSource(mp.left()).getChunkType() == ChunkType.Float) {
                                                    operators.add(new FloatChunkedReAvgOperator(mp.left(), runningSum, nonNull, nanSum, picSum, nicSum));
                                                } else if (table.getColumnSource(mp.left()).getChunkType() == ChunkType.Double) {
                                                    operators.add(new DoubleChunkedReAvgOperator(mp.left(), runningSum, nonNull, nanSum, picSum, nicSum));
                                                } else {
                                                    throw new UnsupportedOperationException();
                                                }
                                            } else {
                                                if (table.getColumnSource(mp.left()).getChunkType() == ChunkType.Float || table.getColumnSource(mp.left()).getChunkType() == ChunkType.Double) {
                                                    operators.add(new FloatChunkedReVarOperator(mp.left(), isStd, runningSum, runningSum2, nonNull, nanSum, picSum, nicSum));
                                                } else {
                                                    throw new UnsupportedOperationException();
                                                }
                                            }

                                            // our final operator is updated if any input changes
                                            final String [] inputNamesForColumn = new String[isStdVar ? 6 : 5];
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
                                            final boolean isBigInteger = BigInteger.class.isAssignableFrom(table.getColumnSource(runningSumName).getType());
                                            final boolean isBigDecimal = BigDecimal.class.isAssignableFrom(table.getColumnSource(runningSumName).getType());

                                            if (isBigInteger) {
                                                final BigIntegerChunkedSumOperator runningSum = new BigIntegerChunkedSumOperator(false, runningSumName);
                                                operators.add(runningSum);
                                                final BigIntegerChunkedSumOperator runningSum2 = new BigIntegerChunkedSumOperator(false, runningSum2Name);
                                                operators.add(runningSum2);
                                                operators.add(new BigIntegerChunkedReVarOperator(mp.left(), isStd, runningSum, runningSum2, nonNull));
                                            } else if (isBigDecimal) {
                                                final BigDecimalChunkedSumOperator runningSum = new BigDecimalChunkedSumOperator(false, runningSumName);
                                                operators.add(runningSum);
                                                final BigDecimalChunkedSumOperator runningSum2 = new BigDecimalChunkedSumOperator(false, runningSum2Name);
                                                operators.add(runningSum2);
                                                operators.add(new BigDecimalChunkedReVarOperator(mp.left(), isStd, runningSum, runningSum2, nonNull));
                                            } else {
                                                final DoubleChunkedSumOperator runningSum = new DoubleChunkedSumOperator(false, runningSumName);
                                                operators.add(runningSum);
                                                final DoubleChunkedSumOperator runningSum2 = new DoubleChunkedSumOperator(false, runningSum2Name);
                                                operators.add(runningSum2);
                                                operators.add(new IntegralChunkedReVarOperator(mp.left(), isStd, runningSum, runningSum2, nonNull));
                                            }
                                            // our final operator is updated if any input changes
                                            inputNames.add(new String[]{nonNullName, runningSumName, runningSum2Name});
                                        } else { // is an average and not floating point
                                            final boolean isBigDecimal = BigDecimal.class.isAssignableFrom(table.getColumnSource(runningSumName).getType());
                                            final boolean isBigInteger = BigInteger.class.isAssignableFrom(table.getColumnSource(runningSumName).getType());

                                            if (isBigInteger) {
                                                final BigIntegerChunkedSumOperator runningSum = new BigIntegerChunkedSumOperator(false, runningSumName);
                                                operators.add(runningSum);
                                                operators.add(new BigIntegerChunkedReAvgOperator(mp.left(), runningSum, nonNull));
                                            } else if (isBigDecimal) {
                                                final BigDecimalChunkedSumOperator runningSum = new BigDecimalChunkedSumOperator(false, runningSumName);
                                                operators.add(runningSum);
                                                operators.add(new BigDecimalChunkedReAvgOperator(mp.left(), runningSum, nonNull));
                                            } else {
                                                final LongChunkedSumOperator runningSum = new LongChunkedSumOperator(false, runningSumName);
                                                operators.add(runningSum);
                                                operators.add(new IntegralChunkedReAvgOperator(mp.left(), runningSum, nonNull));
                                            }

                                            // our final operator is updated if any input changes
                                            inputNames.add(new String[]{nonNullName, runningSumName});
                                        }
                                        return;
                                    } else if(isCountDistinct || isDistinct || isAggUnique) {
                                        final String ssmColName = mp.left() + ROLLUP_DISTINCT_SSM_COLUMN_ID + ROLLUP_COLUMN_SUFFIX;
                                        final ObjectArraySource<SegmentedSortedMultiSet> ssmSource = (ObjectArraySource<SegmentedSortedMultiSet>) table.getColumnSource(ssmColName);
                                        final ColumnSource<?> lastLevelResult = table.getColumnSource(mp.left());
                                        final boolean countNulls;
                                        final IterativeChunkedAggregationOperator op;
                                        if(isDistinct) {
                                            countNulls = ((DistinctStateFactory)inputAggregationStateFactory).countNulls();
                                            op = IterativeOperatorStateFactory.getDistinctChunked(lastLevelResult.getComponentType(), mp.left(), countNulls, true, true);
                                        } else if(isCountDistinct) {
                                            countNulls = ((CountDistinctStateFactory) inputAggregationStateFactory).countNulls();
                                            op = IterativeOperatorStateFactory.getCountDistinctChunked(ssmSource.getComponentType(), mp.left(), countNulls, true, true);
                                        } else {
                                            countNulls = ((UniqueStateFactory) inputAggregationStateFactory).countNulls();
                                            op = IterativeOperatorStateFactory.getUniqueChunked(lastLevelResult.getType(), mp.left(), countNulls, true,
                                                    ((UniqueStateFactory)inputAggregationStateFactory).getNoKeyValue(),
                                                    ((UniqueStateFactory)inputAggregationStateFactory).getNonUniqueValue(), true);
                                        }

                                        inputColumns.add(ssmSource);
                                        inputNames.add(new String[]{ssmColName});
                                        operators.add(op);

                                        return;
                                    }
                                }

                                final ColumnSource columnSource = table.getColumnSource(mp.right());
                                final Class<?> type = columnSource.getType();
                                final ColumnSource inputSource = columnSource.getType() == DBDateTime.class ? ReinterpretUtilities.dateTimeToLongSource(columnSource) : columnSource;

                                final String resultName = mp.left();
                                final boolean hasSource;
                                if (isMinMax) {
                                    final boolean isMinimum = ((MinMaxByStateFactoryImpl) inputAggregationStateFactory).isMinimum();
                                    final OptionalInt priorMinMax = IntStream.range(0, inputColumns.size()).filter(idx -> (inputColumns.get(idx) == inputSource) && (operators.get(idx) instanceof SsmChunkedMinMaxOperator)).findFirst();
                                    if (priorMinMax.isPresent()) {
                                        final SsmChunkedMinMaxOperator ssmChunkedMinMaxOperator = (SsmChunkedMinMaxOperator) operators.get(priorMinMax.getAsInt());
                                        operators.add(ssmChunkedMinMaxOperator.makeSecondaryOperator(isMinimum, resultName));
                                        hasSource = false;
                                    } else {
                                        operators.add(IterativeOperatorStateFactory.getMinMaxChunked(type, isMinimum, isStream || isAddOnly, resultName));
                                        hasSource = true;
                                    }
                                } else if (isPercentile) {
                                    if (isRollup) {
                                        throw new UnsupportedOperationException("Percentile or Median can not be used in a rollup!");
                                    }
                                    operators.add(IterativeOperatorStateFactory.getPercentileChunked(type, ((PercentileByStateFactoryImpl)inputAggregationStateFactory).getPercentile(), ((PercentileByStateFactoryImpl)inputAggregationStateFactory).getAverageMedian(), resultName));
                                    hasSource = true;
                                } else {
                                    operators.add(((IterativeOperatorStateFactory) inputAggregationStateFactory).getChunkedOperator(type, resultName, isRollup));
                                    hasSource = true;
                                }

                                if (hasSource) {
                                    //noinspection unchecked
                                    inputColumns.add(inputSource);
                                } else {
                                    inputColumns.add(null);
                                }
                                inputNames.add(new String[]{mp.right()});
                            });
                        } else if (isFirst || isLast) {
                            inputColumns.add(null);
                            final String exposeRedirectionAs;
                            if(isRollup) {
                                exposeRedirectionAs = makeRedirectionName((IterativeIndexStateFactory) inputAggregationStateFactory);
                            } else if(inputAggregationStateFactory instanceof KeyOnlyFirstOrLastByStateFactory) {
                                exposeRedirectionAs = ((KeyOnlyFirstOrLastByStateFactory) inputAggregationStateFactory).getResultColumn();
                            } else {
                                exposeRedirectionAs = null;
                            }

                            if (table.isLive()) {
                                if (isStream) {
                                    operators.add(isFirst ? new StreamFirstChunkedOperator(comboMatchPairs, table) : new StreamLastChunkedOperator(comboMatchPairs, table));
                                } else if (isAddOnly) {
                                    operators.add(new AddOnlyFirstOrLastChunkedOperator(isFirst, comboMatchPairs, table, exposeRedirectionAs));
                                } else {
                                    if (trackedFirstOrLastIndex >= 0) {
                                        final IterativeChunkedAggregationOperator operator = operators.get(trackedFirstOrLastIndex);
                                        final FirstOrLastChunkedOperator firstOrLastChunkedOperator = (FirstOrLastChunkedOperator)operator;
                                        operators.add(firstOrLastChunkedOperator.makeSecondaryOperator(isFirst, comboMatchPairs, table, exposeRedirectionAs));
                                    } else {
                                        operators.add(new FirstOrLastChunkedOperator(isFirst, comboMatchPairs, table, exposeRedirectionAs));
                                        trackedFirstOrLastIndex = operators.size() - 1;
                                    }
                                }
                            } else {
                                operators.add(new StaticFirstOrLastChunkedOperator(isFirst, comboMatchPairs, table, exposeRedirectionAs));
                            }
                            inputNames.add(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
                        } else if (isAggArray) {
                            if (isStream) {
                                throw streamUnsupported("AggArray");
                            }
                            inputColumns.add(null);
                            operators.add(new ByChunkedOperator((QueryTable)table, true, comboMatchPairs));
                            inputNames.add(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
                        } else if (isFormula) {
                            if (isStream) {
                                throw streamUnsupported("AggFormula");
                            }
                            final AggregationFormulaStateFactory formulaStateFactory = (AggregationFormulaStateFactory)inputAggregationStateFactory;
                            final ByChunkedOperator byChunkedOperator = new ByChunkedOperator((QueryTable) table, false, Arrays.stream(comboMatchPairs).map(MatchPair::right).map(MatchPairFactory::getExpression).toArray(MatchPair[]::new));
                            final FormulaChunkedOperator formulaChunkedOperator = new FormulaChunkedOperator(byChunkedOperator, true, formulaStateFactory.getFormula(), formulaStateFactory.getColumnParamName(), comboMatchPairs);
                            inputColumns.add(null);
                            operators.add(formulaChunkedOperator);
                            inputNames.add(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
                        } else if (isWeightedAverage || isWeightedSum) {
                            final String weightName;

                            if (isWeightedAverage) {
                                weightName = ((WeightedAverageStateFactoryImpl) inputAggregationStateFactory).getWeightName();
                            } else {
                                weightName = ((WeightedSumStateFactoryImpl) inputAggregationStateFactory).getWeightName();
                            }

                            final ColumnSource<?> weightSource = table.getColumnSource(weightName);
                            final DoubleWeightRecordingInternalOperator weightOperator = new DoubleWeightRecordingInternalOperator(weightSource.getChunkType());
                            inputColumns.add(weightSource);
                            operators.add(weightOperator);

                            inputNames.add(Stream.concat(Stream.of(weightName), Arrays.stream(comboMatchPairs).map(MatchPair::right)).toArray(String[]::new));

                            Arrays.stream(comboMatchPairs).forEach(mp -> {
                                final ColumnSource<?> columnSource = table.getColumnSource(mp.right());
                                inputColumns.add(columnSource);
                                inputNames.add(new String[]{weightName, mp.right()});
                                if (isWeightedAverage) {
                                    operators.add(new ChunkedWeightedAverageOperator(columnSource.getChunkType(), weightOperator, mp.left()));
                                } else {
                                    operators.add(new DoubleChunkedWeightedSumOperator(columnSource.getChunkType(), weightOperator, mp.left()));
                                }
                            });
                        } else {
                            throw new UnsupportedOperationException("Unknown ComboByImpl: " + inputAggregationStateFactory.getClass());
                        }
                    }
                }
                else if (comboBy instanceof NullComboBy) {
                    transformers.add(new NullColumnAggregationTransformer(((NullComboBy) comboBy).resultColumns));
                }
                else if (comboBy instanceof ExternalComboBy) {
                    if (!isRollup) {
                        throw new IllegalStateException("ExternalComboBy must be used only with rollups.");
                    }
                    inputColumns.add(null);
                    inputNames.add(CollectionUtil.ZERO_LENGTH_STRING_ARRAY);
                    final boolean includeConstituents = ((ExternalComboBy)comboBy).leafLevel;
                    if (includeConstituents) {
                        if (isStream) {
                            throw streamUnsupported("rollup with included constituents");
                        }
                        Assert.eqFalse(secondLevel, "secondLevel");
                    }

                    final QueryTable parentTable = (QueryTable) table;
                    final QueryTable adjustedTable;
                    final List<String> columnsToDrop = parentTable.getDefinition().getColumnStream().map(ColumnDefinition::getName).filter(cn -> cn.endsWith(ROLLUP_COLUMN_SUFFIX)).collect(Collectors.toList());
                    if (!columnsToDrop.isEmpty()) {
                        adjustedTable = (QueryTable)parentTable.dropColumns(columnsToDrop);
                    } else {
                        if (includeConstituents) {
                            adjustedTable = (QueryTable)parentTable.updateView(RollupInfo.ROLLUP_COLUMN + "=" + null);
                        } else {
                            adjustedTable = parentTable;
                        }
                    }
                    if (adjustedTable != parentTable && parentTable.hasAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE)) {
                        adjustedTable.setAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE, parentTable.getAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE));
                    }
                    final ByExternalChunkedOperator.AttributeCopier copier;
                    if (includeConstituents) {
                        copier = RollupAttributeCopier.LEAF_WITHCONSTITUENTS_INSTANCE;
                    } else {
                        copier = RollupAttributeCopier.DEFAULT_INSTANCE;
                    }
                    final ByExternalChunkedOperator tableMapOperator = new ByExternalChunkedOperator(parentTable, adjustedTable, copier, Collections.emptyList(), groupByColumns);
                    operators.add(tableMapOperator);

                    if (groupByColumns.length == 0) {
                        transformers.add(new StaticColumnSourceTransformer(RollupInfo.ROLLUP_COLUMN, new SingleValueObjectColumnSource<>(SmartKey.EMPTY)));
                    } else if (groupByColumns.length == 1) {
                        transformers.add(new RollupKeyColumnDuplicationTransformer(groupByColumns[0]));
                    } else {
                        transformers.add(new RollupSmartKeyColumnDuplicationTransformer(groupByColumns));
                    }

                    transformers.add(new RollupTableMapAndReverseLookupAttributeSetter(tableMapOperator, this, secondLevel, includeConstituents));

                    externalFound = true;
                } else {
                    throw new UnsupportedOperationException("Unknown ComboBy: " + comboBy.getClass());
                }
            }

            if (!externalFound && isRollup && !secondLevel) {
                transformers.add(new NoKeyLeafRollupAttributeSetter());
            }

            final IterativeChunkedAggregationOperator[] operatorsArray = operators.toArray(IterativeChunkedAggregationOperator.ZERO_LENGTH_ITERATIVE_CHUNKED_AGGREGATION_OPERATOR_ARRAY);
            final AggregationContextTransformer[] transformersArray = transformers.toArray(AggregationContextTransformer.ZERO_LENGTH_AGGREGATION_CONTEXT_TRANSFORMER_ARRAY);
            final String[][] inputNamesArray = inputNames.toArray(CollectionUtil.ZERO_LENGTH_STRING_ARRAY_ARRAY);
            //noinspection unchecked
            final ChunkSource.WithPrev<Values>[] inputColumnsArray = inputColumns.toArray(ChunkSource.WithPrev.ZERO_LENGTH_CHUNK_SOURCE_WITH_PREV_ARRAY);

            return new AggregationContext(operatorsArray, inputNamesArray, inputColumnsArray, transformersArray, true);
        };
    }

    private static UnsupportedOperationException streamUnsupported(@NotNull final String operatorTypeName) {
        return new UnsupportedOperationException("Stream tables do not support " + operatorTypeName
                + "; use StreamTableTools.streamToAppendOnlyTable to accumulate full history");
    }

    @NotNull
    private static String makeRedirectionName(IterativeIndexStateFactory inputAggregationStateFactory) {
        return IterativeIndexStateFactory.REDIRECTION_INDEX_PREFIX + inputAggregationStateFactory.rollupColumnIdentifier + ROLLUP_COLUMN_SUFFIX;
    }

    private static class RollupTableMapAndReverseLookupAttributeSetter implements AggregationContextTransformer {
        private final ByExternalChunkedOperator tableMapOperator;
        private final ComboAggregateFactory factory;
        private final boolean secondLevel;
        private final boolean includeConstituents;
        private ReverseLookup reverseLookup;

        RollupTableMapAndReverseLookupAttributeSetter(ByExternalChunkedOperator tableMapOperator, ComboAggregateFactory factory, boolean secondLevel, boolean includeConstituents) {
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
        table.setAttribute(Table.HIERARCHICAL_CHILDREN_TABLE_MAP_ATTRIBUTE, TableMap.emptyMap());
        table.setAttribute(Table.REVERSE_LOOKUP_ATTRIBUTE, ReverseLookup.NULL);
    }

    private static class ComboByAggregationAdapterOptimizer implements Aggregation.Visitor {

        private final List<Pair> absSums = new ArrayList<>();
        private final List<Pair> arrays = new ArrayList<>();
        private final List<Pair> avgs = new ArrayList<>();
        private final List<ColumnName> counts = new ArrayList<>();
        private final Map<Boolean, List<Pair>> countDistincts = new HashMap<>();
        private final Map<Boolean, List<Pair>> distincts = new HashMap<>();
        private final List<Pair> firsts = new ArrayList<>();
        private final List<Pair> lasts = new ArrayList<>();
        private final List<Pair> maxs = new ArrayList<>();
        private final Map<Boolean, List<Pair>> medians = new HashMap<>();
        private final List<Pair> mins = new ArrayList<>();
        private final Map<ByteDoubleTuple, List<Pair>> pcts = new HashMap<>();
        private final Map<List<SortColumn>, List<Pair>> sortedFirsts = new HashMap<>();
        private final Map<List<SortColumn>, List<Pair>> sortedLasts = new HashMap<>();
        private final List<Pair> stds = new ArrayList<>();
        private final List<Pair> sums = new ArrayList<>();
        private final Map<Boolean, List<Pair>> uniques = new HashMap<>();
        private final List<Pair> vars = new ArrayList<>();
        private final Map<ColumnName, List<Pair>> wAvgs = new HashMap<>();
        private final Map<ColumnName, List<Pair>> wSums = new HashMap<>();

        /**
         * We'll do our best to maintain the original combo ordering. This will maintain the user-specified order as
         * long as the user aggregation types were all next to each other.
         *
         * ie:
         *
         * {@code by(..., [ Sum.of(A), Sum.of(B), Avg.of(C), Avg.of(D) ] )} will not need to be re-ordered
         *
         * {@code by(..., [ Sum.of(A), Avg.of(C), Avg.of(D), Sum.of(B) ] )} will need to be re-ordered
         */
        private final LinkedHashSet<BuildLogic> buildOrder = new LinkedHashSet<>();

        @FunctionalInterface
        private interface BuildLogic {
            void appendTo(List<ComboBy> outs);
        }

        // Unfortunately, it doesn't look like we can add ad-hoc lambdas to buildOrder, they don't appear to be equal
        // across multiple constructions.
        private final BuildLogic buildAbsSums = this::buildAbsSums;
        private final BuildLogic buildArrays = this::buildArrays;
        private final BuildLogic buildAvgs = this::buildAvgs;
        private final BuildLogic buildCounts = this::buildCounts;
        private final BuildLogic buildCountDistincts = this::buildCountDistincts;
        private final BuildLogic buildDistincts = this::buildDistincts;
        private final BuildLogic buildFirsts = this::buildFirsts;
        private final BuildLogic buildLasts = this::buildLasts;
        private final BuildLogic buildMaxes = this::buildMaxes;
        private final BuildLogic buildMedians = this::buildMedians;
        private final BuildLogic buildMins = this::buildMins;
        private final BuildLogic buildPcts = this::buildPcts;
        private final BuildLogic buildSortedFirsts = this::buildSortedFirsts;
        private final BuildLogic buildSortedLasts = this::buildSortedLasts;
        private final BuildLogic buildStds = this::buildStds;
        private final BuildLogic buildSums = this::buildSums;
        private final BuildLogic buildUniques = this::buildUniques;
        private final BuildLogic buildVars = this::buildVars;
        private final BuildLogic buildWAvgs = this::buildWAvgs;
        private final BuildLogic buildWSums = this::buildWSums;


        List<ComboBy> build() {
            List<ComboBy> combos = new ArrayList<>();
            for (BuildLogic buildLogic : buildOrder) {
                buildLogic.appendTo(combos);
            }
            return combos;
        }

        private void buildWSums(List<ComboBy> combos) {
            for (Map.Entry<ColumnName, List<Pair>> e : wSums.entrySet()) {
                combos.add(Agg(new WeightedSumStateFactoryImpl(e.getKey().name()), MatchPair.fromPairs(e.getValue())));
            }
        }

        private void buildWAvgs(List<ComboBy> combos) {
            for (Map.Entry<ColumnName, List<Pair>> e : wAvgs.entrySet()) {
                combos.add(Agg(new WeightedAverageStateFactoryImpl(e.getKey().name()), MatchPair.fromPairs(e.getValue())));
            }
        }

        private void buildVars(List<ComboBy> combos) {
            if (!vars.isEmpty()) {
                combos.add(Agg(AggType.Var, MatchPair.fromPairs(vars)));
            }
        }

        private void buildUniques(List<ComboBy> combos) {
            for (Map.Entry<Boolean, List<Pair>> e : uniques.entrySet()) {
                combos.add(Agg(new UniqueStateFactory(e.getKey()), MatchPair.fromPairs(e.getValue())));
            }
        }

        private void buildSums(List<ComboBy> combos) {
            if (!sums.isEmpty()) {
                combos.add(Agg(AggType.Sum, MatchPair.fromPairs(sums)));
            }
        }

        private void buildStds(List<ComboBy> combos) {
            if (!stds.isEmpty()) {
                combos.add(Agg(AggType.Std, MatchPair.fromPairs(stds)));
            }
        }

        private void buildSortedLasts(List<ComboBy> combos) {
            for (Map.Entry<List<SortColumn>, List<Pair>> e : sortedLasts.entrySet()) {
                // TODO(deephaven-core#821): SortedFirst / SortedLast aggregations with sort direction
                String[] columns = e.getKey().stream().map(SortColumn::column).map(ColumnName::name).toArray(String[]::new);
                combos.add(Agg(new SortedLastBy(columns), MatchPair.fromPairs(e.getValue())));
            }
        }

        private void buildSortedFirsts(List<ComboBy> combos) {
            for (Map.Entry<List<SortColumn>, List<Pair>> e : sortedFirsts.entrySet()) {
                // TODO(deephaven-core#821): SortedFirst / SortedLast aggregations with sort direction
                String[] columns = e.getKey().stream().map(SortColumn::column).map(ColumnName::name).toArray(String[]::new);
                combos.add(Agg(new SortedFirstBy(columns), MatchPair.fromPairs(e.getValue())));
            }
        }

        private void buildPcts(List<ComboBy> combos) {
            for (Map.Entry<ByteDoubleTuple, List<Pair>> e : pcts.entrySet()) {
                combos.add(Agg(new PercentileByStateFactoryImpl(e.getKey().getSecondElement(), e.getKey().getFirstElement() != 0), MatchPair.fromPairs(e.getValue())));
            }
        }

        private void buildMins(List<ComboBy> combos) {
            if (!mins.isEmpty()) {
                combos.add(Agg(AggType.Min, MatchPair.fromPairs(mins)));
            }
        }

        private void buildMedians(List<ComboBy> combos) {
            for (Map.Entry<Boolean, List<Pair>> e : medians.entrySet()) {
                combos.add(Agg(new PercentileByStateFactoryImpl(0.50d, e.getKey()), MatchPair.fromPairs(e.getValue())));
            }
        }

        private void buildMaxes(List<ComboBy> combos) {
            if (!maxs.isEmpty()) {
                combos.add(Agg(AggType.Max, MatchPair.fromPairs(maxs)));
            }
        }

        private void buildLasts(List<ComboBy> combos) {
            if (!lasts.isEmpty()) {
                combos.add(Agg(AggType.Last, MatchPair.fromPairs(lasts)));
            }
        }

        private void buildFirsts(List<ComboBy> combos) {
            if (!firsts.isEmpty()) {
                combos.add(Agg(AggType.First, MatchPair.fromPairs(firsts)));
            }
        }

        private void buildDistincts(List<ComboBy> combos) {
            for (Map.Entry<Boolean, List<Pair>> e : distincts.entrySet()) {
                combos.add(Agg(new DistinctStateFactory(e.getKey()), MatchPair.fromPairs(e.getValue())));
            }
        }

        private void buildCountDistincts(List<ComboBy> combos) {
            for (Map.Entry<Boolean, List<Pair>> e : countDistincts.entrySet()) {
                combos.add(Agg(new CountDistinctStateFactory(e.getKey()), MatchPair.fromPairs(e.getValue())));
            }
        }

        private void buildCounts(List<ComboBy> combos) {
            for (ColumnName count : counts) {
                combos.add(new CountComboBy(count.name()));
            }
        }

        private void buildAvgs(List<ComboBy> combos) {
            if (!avgs.isEmpty()) {
                combos.add(Agg(AggType.Avg, MatchPair.fromPairs(avgs)));
            }
        }

        private void buildArrays(List<ComboBy> combos) {
            if (!arrays.isEmpty()) {
                combos.add(Agg(AggType.Array, MatchPair.fromPairs(arrays)));
            }
        }

        private void buildAbsSums(List<ComboBy> combos) {
            if (!absSums.isEmpty()) {
                combos.add(Agg(AggType.AbsSum, MatchPair.fromPairs(absSums)));
            }
        }

        @Override
        public void visit(AbsSum absSum) {
            absSums.add(absSum.pair());
            buildOrder.add(buildAbsSums);
        }

        @Override
        public void visit(Array array) {
            arrays.add(array.pair());
            buildOrder.add(buildArrays);
        }

        @Override
        public void visit(Avg avg) {
            avgs.add(avg.pair());
            buildOrder.add(buildAvgs);
        }

        @Override
        public void visit(Count count) {
            counts.add(count.column());
            buildOrder.add(buildCounts);
        }

        @Override
        public void visit(CountDistinct countDistinct) {
            countDistincts.computeIfAbsent(countDistinct.countNulls(), b -> new ArrayList<>()).add(countDistinct.pair());
            buildOrder.add(buildCountDistincts);
        }

        @Override
        public void visit(Distinct distinct) {
            distincts.computeIfAbsent(distinct.includeNulls(), b -> new ArrayList<>()).add(distinct.pair());
            buildOrder.add(buildDistincts);
        }

        @Override
        public void visit(First first) {
            firsts.add(first.pair());
            buildOrder.add(buildFirsts);
        }

        @Override
        public void visit(Last last) {
            lasts.add(last.pair());
            buildOrder.add(buildLasts);
        }

        @Override
        public void visit(Max max) {
            maxs.add(max.pair());
            buildOrder.add(buildMaxes);
        }

        @Override
        public void visit(Med med) {
            medians.computeIfAbsent(med.averageMedian(), b -> new ArrayList<>()).add(med.pair());
            buildOrder.add(buildMedians);
        }

        @Override
        public void visit(Min min) {
            mins.add(min.pair());
            buildOrder.add(buildMins);
        }

        @Override
        public void visit(Multi<?> multi) {
            for (Aggregation aggregation : multi.aggregations()) {
                aggregation.walk(this);
            }
        }

        @Override
        public void visit(Pct pct) {
            pcts.computeIfAbsent(new ByteDoubleTuple(pct.averageMedian() ? (byte)1 : (byte)0, pct.percentile()), b -> new ArrayList<>()).add(pct.pair());
            buildOrder.add(buildPcts);
        }

        @Override
        public void visit(SortedFirst sortedFirst) {
            sortedFirsts.computeIfAbsent(sortedFirst.columns(), b -> new ArrayList<>()).add(sortedFirst.pair());
            buildOrder.add(buildSortedFirsts);
        }

        @Override
        public void visit(SortedLast sortedLast) {
            sortedLasts.computeIfAbsent(sortedLast.columns(), b -> new ArrayList<>()).add(sortedLast.pair());
            buildOrder.add(buildSortedLasts);
        }

        @Override
        public void visit(Std std) {
            stds.add(std.pair());
            buildOrder.add(buildStds);
        }

        @Override
        public void visit(Sum sum) {
            sums.add(sum.pair());
            buildOrder.add(buildSums);
        }

        @Override
        public void visit(Unique unique) {
            uniques.computeIfAbsent(unique.includeNulls(), b -> new ArrayList<>()).add(unique.pair());
            buildOrder.add(buildUniques);
        }

        @Override
        public void visit(Var var) {
            vars.add(var.pair());
            buildOrder.add(buildVars);
        }

        @Override
        public void visit(WAvg wAvg) {
            wAvgs.computeIfAbsent(wAvg.weight(), b -> new ArrayList<>()).add(wAvg.pair());
            buildOrder.add(buildWAvgs);
        }

        @Override
        public void visit(WSum wSum) {
            wSums.computeIfAbsent(wSum.weight(), b -> new ArrayList<>()).add(wSum.pair());
            buildOrder.add(buildWSums);
        }
    }
}
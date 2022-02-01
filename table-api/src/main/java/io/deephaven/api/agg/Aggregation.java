package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.agg.spec.AggSpecFreeze;
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

    static ColumnAggregation of(AggSpec spec, String pair) {
        return ColumnAggregation.of(spec, Pair.parse(pair));
    }

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

    static Aggregation of(Aggregation... aggregations) {
        if (aggregations.length == 1) {
            return aggregations[0];
        }
        return Aggregations.builder().addAggregations(aggregations).build();
    }

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

    static Aggregation AggAbsSum(String... pairs) {
        return of(AggSpec.absSum(), pairs);
    }

    static Aggregation AggApproxPct(double percentile, String... pairs) {
        return of(AggSpec.approximatePercentile(percentile), pairs);
    }

    static Aggregation AggApproxPct(double percentile, double compression, String... pairs) {
        return of(AggSpec.approximatePercentile(percentile, compression), pairs);
    }

    static Aggregation AggApproxPct(String inputColumn, PercentileOutput... percentileOutputs) {
        final BiFunction<ColumnName, PercentileOutput, ColumnAggregation> aggFactory = (ic, po) -> ColumnAggregation
                .of(AggSpec.approximatePercentile(po.percentile()), Pair.of(ic, po.output()));
        return of(aggFactory, inputColumn, percentileOutputs);
    }

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

    static Sentinel Sentinel(Object value) {
        return Sentinel.of(value);
    }

    static Sentinel Sentinel() {
        return Sentinel.of();
    }

    <V extends Visitor> V walk(V visitor);

    interface Visitor {
        void visit(Aggregations aggregations);

        void visit(ColumnAggregation columnAgg);

        void visit(ColumnAggregations columnAggs);

        void visit(Count count);

        void visit(FirstRowKey firstRowKey);

        void visit(LastRowKey lastRowKey);
    }
}

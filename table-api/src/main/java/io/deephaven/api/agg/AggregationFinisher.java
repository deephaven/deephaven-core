package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.api.SimpleStyle;
import io.deephaven.api.SortColumn;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Collections;
import java.util.function.Function;

/**
 * The aggregation finisher is a helper to aid in building aggregations whose construction can be
 * finished by a string parameter, {@link #of(String)}. A vararg overload is provided to build a
 * {@link Multi}, {@link #of(String...)}, which can be useful to reduce the syntax required to build
 * multiple aggregations of the same basic type.
 *
 * <p>
 * Not all aggregations may be suitable for construction in this style.
 *
 * @param <AGG> the aggregation type
 */
@Immutable
@SimpleStyle
public abstract class AggregationFinisher<AGG extends Aggregation> {

    public static AggregationFinisher<AbsSum> absSum() {
        return ImmutableAggregationFinisher.of(AbsSum::of);
    }

    public static AggregationFinisher<Array> array() {
        return ImmutableAggregationFinisher.of(Array::of);
    }

    public static AggregationFinisher<Avg> avg() {
        return ImmutableAggregationFinisher.of(Avg::of);
    }

    public static AggregationFinisher<Count> count() {
        return ImmutableAggregationFinisher.of(Count::of);
    }

    public static AggregationFinisher<CountDistinct> countDistinct() {
        return ImmutableAggregationFinisher.of(CountDistinct::of);
    }

    public static AggregationFinisher<Distinct> distinct() {
        return ImmutableAggregationFinisher.of(Distinct::of);
    }

    public static AggregationFinisher<First> first() {
        return ImmutableAggregationFinisher.of(First::of);
    }

    public static AggregationFinisher<Last> last() {
        return ImmutableAggregationFinisher.of(Last::of);
    }

    public static AggregationFinisher<Max> max() {
        return ImmutableAggregationFinisher.of(Max::of);
    }

    public static AggregationFinisher<Med> med() {
        return ImmutableAggregationFinisher.of(Med::of);
    }

    public static AggregationFinisher<Min> min() {
        return ImmutableAggregationFinisher.of(Min::of);
    }

    public static AggregationFinisher<Pct> pct(double percentile) {
        return ImmutableAggregationFinisher.of(s -> Pct.of(percentile, Pair.parse(s)));
    }

    static AggregationFinisher<SortedFirst> sortedFirst(SortColumn sortColumn) {
        return sortedFirst(Collections.singleton(sortColumn));
    }

    static AggregationFinisher<SortedFirst> sortedFirst(Iterable<SortColumn> sortColumns) {
        return ImmutableAggregationFinisher
            .of(s -> SortedFirst.builder().addAllColumns(sortColumns).pair(Pair.parse(s)).build());
    }

    static AggregationFinisher<SortedLast> sortedLast(SortColumn sortColumn) {
        return sortedLast(Collections.singleton(sortColumn));
    }

    static AggregationFinisher<SortedLast> sortedLast(Iterable<SortColumn> sortColumns) {
        return ImmutableAggregationFinisher
            .of(s -> SortedLast.builder().addAllColumns(sortColumns).pair(Pair.parse(s)).build());
    }

    public static AggregationFinisher<Std> std() {
        return ImmutableAggregationFinisher.of(Std::of);
    }

    public static AggregationFinisher<Sum> sum() {
        return ImmutableAggregationFinisher.of(Sum::of);
    }

    public static AggregationFinisher<Unique> unique() {
        return ImmutableAggregationFinisher.of(Unique::of);
    }

    public static AggregationFinisher<Var> var() {
        return ImmutableAggregationFinisher.of(Var::of);
    }

    public static AggregationFinisher<WAvg> wAvg(ColumnName weightColumn) {
        return ImmutableAggregationFinisher.of(s -> WAvg.of(weightColumn, Pair.parse(s)));
    }

    public static AggregationFinisher<WSum> wSum(ColumnName weightColumn) {
        return ImmutableAggregationFinisher.of(s -> WSum.of(weightColumn, Pair.parse(s)));
    }

    @Parameter
    public abstract Function<String, AGG> function();

    public final AGG of(String arg) {
        return function().apply(arg);
    }

    public final Multi<AGG> of(String... arguments) {
        Multi.Builder<AGG> builder = Multi.builder();
        for (String x : arguments) {
            builder.addAggregations(of(x));
        }
        return builder.build();
    }
}

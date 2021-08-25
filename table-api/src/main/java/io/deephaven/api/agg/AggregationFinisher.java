package io.deephaven.api.agg;

import io.deephaven.api.ColumnName;
import io.deephaven.annotations.SimpleStyle;
import io.deephaven.api.SortColumn;
import org.immutables.value.Value.Immutable;
import org.immutables.value.Value.Parameter;

import java.util.Collections;
import java.util.function.Function;

/**
 * The aggregation finisher is a helper to aid in building aggregations whose construction can be finished by a
 * {@link Pair}. A vararg overload is provided to build a {@link Multi}, {@link #of(Pair...)}, which can be useful to
 * reduce the syntax required to build multiple aggregations of the same basic type. Helpers are provided that translate
 * the string-equivalents via {@link Pair#parse(String)}.
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
        return ImmutableAggregationFinisher.of(pair -> Pct.of(percentile, pair));
    }

    static AggregationFinisher<SortedFirst> sortedFirst(SortColumn sortColumn) {
        return sortedFirst(Collections.singleton(sortColumn));
    }

    static AggregationFinisher<SortedFirst> sortedFirst(Iterable<SortColumn> sortColumns) {
        return ImmutableAggregationFinisher
                .of(pair -> SortedFirst.builder().addAllColumns(sortColumns).pair(pair).build());
    }

    static AggregationFinisher<SortedLast> sortedLast(SortColumn sortColumn) {
        return sortedLast(Collections.singleton(sortColumn));
    }

    static AggregationFinisher<SortedLast> sortedLast(Iterable<SortColumn> sortColumns) {
        return ImmutableAggregationFinisher
                .of(pair -> SortedLast.builder().addAllColumns(sortColumns).pair(pair).build());
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
        return ImmutableAggregationFinisher.of(pair -> WAvg.of(weightColumn, pair));
    }

    public static AggregationFinisher<WSum> wSum(ColumnName weightColumn) {
        return ImmutableAggregationFinisher.of(pair -> WSum.of(weightColumn, pair));
    }

    @Parameter
    public abstract Function<Pair, AGG> function();

    public final AGG of(Pair pair) {
        return function().apply(pair);
    }

    public final Multi<AGG> of(Pair... pairs) {
        Multi.Builder<AGG> builder = Multi.builder();
        for (Pair pair : pairs) {
            builder.addAggregations(of(pair));
        }
        return builder.build();
    }

    public final AGG of(String arg) {
        return of(Pair.parse(arg));
    }

    public final Multi<AGG> of(String... arguments) {
        Multi.Builder<AGG> builder = Multi.builder();
        for (String x : arguments) {
            builder.addAggregations(of(x));
        }
        return builder.build();
    }
}

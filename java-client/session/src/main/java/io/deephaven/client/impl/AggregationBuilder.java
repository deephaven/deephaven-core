//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import io.deephaven.api.Strings;
import io.deephaven.api.agg.*;
import io.deephaven.api.Pair;
import io.deephaven.api.filter.Filter;
import io.deephaven.proto.backplane.grpc.Aggregation;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationColumns;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationCount;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationCountWhere;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationFormula;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationPartition;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationRowKey;
import io.deephaven.proto.backplane.grpc.Aggregation.Builder;
import io.deephaven.proto.backplane.grpc.Selectable;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static java.util.Collections.singletonList;

class AggregationBuilder implements io.deephaven.api.agg.Aggregation.Visitor {

    public static List<Aggregation> adapt(io.deephaven.api.agg.Aggregation agg) {
        return agg.walk(new AggregationBuilder()).out();
    }

    private List<Aggregation> out;

    List<Aggregation> out() {
        return Objects.requireNonNull(out);
    }

    private static <T> Aggregation of(BiFunction<Builder, T, Builder> f, T obj) {
        return f.apply(Aggregation.newBuilder(), obj).build();
    }

    @Override
    public void visit(Aggregations aggregations) {
        out = aggregations
                .aggregations()
                .stream()
                .map(AggregationBuilder::adapt)
                .flatMap(Collection::stream)
                .collect(Collectors.toList());
    }

    @Override
    public void visit(ColumnAggregation columnAgg) {
        out = singletonList(of(Builder::setColumns, AggregationColumns.newBuilder()
                .setSpec(AggSpecBuilder.adapt(columnAgg.spec()))
                .addMatchPairs(Strings.of(columnAgg.pair()))));
    }

    @Override
    public void visit(ColumnAggregations columnAggs) {
        AggregationColumns.Builder builder = AggregationColumns.newBuilder()
                .setSpec(AggSpecBuilder.adapt(columnAggs.spec()));
        for (Pair pair : columnAggs.pairs()) {
            builder.addMatchPairs(Strings.of(pair));
        }
        out = singletonList(of(Builder::setColumns, builder));
    }

    @Override
    public void visit(Count count) {
        out = singletonList(of(Builder::setCount, AggregationCount.newBuilder()
                .setColumnName(count.column().name())));
    }

    @Override
    public void visit(CountWhere countWhere) {
        final Collection<String> filters = Filter.extractAnds(countWhere.filter()).stream()
                .map(Strings::of)
                .collect(Collectors.toList());
        out = singletonList(of(Builder::setCountWhere, AggregationCountWhere.newBuilder()
                .setColumnName(countWhere.column().name())
                .addAllFilters(filters)));
    }

    @Override
    public void visit(FirstRowKey firstRowKey) {
        out = singletonList(of(Builder::setFirstRowKey, AggregationRowKey.newBuilder()
                .setColumnName(firstRowKey.column().name())));

    }

    @Override
    public void visit(LastRowKey lastRowKey) {
        out = singletonList(of(Builder::setLastRowKey, AggregationRowKey.newBuilder()
                .setColumnName(lastRowKey.column().name())));
    }

    @Override
    public void visit(Partition partition) {
        out = singletonList(of(Builder::setPartition, AggregationPartition.newBuilder()
                .setColumnName(partition.column().name())
                .setIncludeGroupByColumns(partition.includeGroupByColumns())));
    }

    @Override
    public void visit(Formula formula) {
        final Selectable selectable = Selectable.newBuilder().setRaw(formula.formulaString()).build();
        out = singletonList(of(Builder::setFormula, AggregationFormula.newBuilder()
                .setSelectable(selectable)));
    }
}

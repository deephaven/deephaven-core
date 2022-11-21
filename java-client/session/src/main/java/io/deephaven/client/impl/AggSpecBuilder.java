package io.deephaven.client.impl;

import com.google.protobuf.MessageOrBuilder;
import io.deephaven.api.SortColumn;
import io.deephaven.api.SortColumn.Order;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecApproximatePercentile;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecBlank;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecCountDistinct;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecDistinct;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecFormula;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecMedian;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecPercentile;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecSorted;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecTDigest;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecUnique;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecWeighted;
import io.deephaven.proto.backplane.grpc.AggSpec.Builder;
import io.deephaven.proto.backplane.grpc.SortDescriptor;
import io.deephaven.proto.backplane.grpc.SortDescriptor.SortDirection;

import java.util.Objects;
import java.util.function.BiFunction;

import static io.deephaven.proto.backplane.grpc.AggSpec.newBuilder;

class AggSpecBuilder implements io.deephaven.api.agg.spec.AggSpec.Visitor {

    public static AggSpec adapt(io.deephaven.api.agg.spec.AggSpec aggSpec) {
        return aggSpec.walk(new AggSpecBuilder()).out();
    }

    private AggSpec out;

    AggSpec out() {
        return Objects.requireNonNull(out);
    }

    private static AggSpec blankSpec(BiFunction<Builder, AggSpecBlank, Builder> setter) {
        return setter.apply(newBuilder(), AggSpecBlank.getDefaultInstance()).build();
    }

    private static <T extends MessageOrBuilder> AggSpec otherSpec(BiFunction<Builder, T, Builder> setter, T obj) {
        return setter.apply(newBuilder(), obj).build();
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecAbsSum absSum) {
        out = blankSpec(Builder::setAbsSum);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecApproximatePercentile approxPct) {
        out = otherSpec(Builder::setApproximatePercentile, AggSpecApproximatePercentile.newBuilder()
                .setPercentile(approxPct.percentile())
                .setCompression(approxPct.compression()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecAvg avg) {
        out = blankSpec(Builder::setAvg);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecCountDistinct countDistinct) {
        out = otherSpec(Builder::setCountDistinct, AggSpecCountDistinct.newBuilder()
                .setCountNulls(countDistinct.countNulls()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecDistinct distinct) {
        out = otherSpec(Builder::setDistinct, AggSpecDistinct.newBuilder()
                .setIncludeNulls(distinct.includeNulls()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecFirst first) {
        out = blankSpec(Builder::setFirst);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecFormula formula) {
        out = otherSpec(Builder::setFormula, AggSpecFormula.newBuilder()
                .setFormula(formula.formula())
                .setParamToken(formula.paramToken()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecFreeze freeze) {
        out = blankSpec(Builder::setFreeze);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecGroup group) {
        out = blankSpec(Builder::setGroup);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecLast last) {
        out = blankSpec(Builder::setLast);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecMax max) {
        out = blankSpec(Builder::setMax);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecMedian median) {
        out = otherSpec(Builder::setMedian, AggSpecMedian.newBuilder()
                .setAverageEvenlyDivided(median.averageEvenlyDivided()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecMin min) {
        out = blankSpec(Builder::setMin);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecPercentile pct) {
        out = otherSpec(Builder::setPercentile, AggSpecPercentile.newBuilder()
                .setPercentile(pct.percentile())
                .setAverageEvenlyDivided(pct.averageEvenlyDivided()));
    }

    private static SortDescriptor adapt(SortColumn sortColumn) {
        return SortDescriptor.newBuilder()
                .setColumnName(sortColumn.column().name())
                .setDirection(
                        sortColumn.order() == Order.ASCENDING ? SortDirection.ASCENDING : SortDirection.DESCENDING)
                .build();
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecSortedFirst sortedFirst) {
        final AggSpecSorted.Builder builder = AggSpecSorted.newBuilder();
        for (SortColumn column : sortedFirst.columns()) {
            builder.addColumns(adapt(column));
        }
        out = otherSpec(Builder::setSortedFirst, builder);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecSortedLast sortedLast) {
        final AggSpecSorted.Builder builder = AggSpecSorted.newBuilder();
        for (SortColumn column : sortedLast.columns()) {
            builder.addColumns(adapt(column));
        }
        out = otherSpec(Builder::setSortedLast, builder);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecStd std) {
        out = blankSpec(Builder::setStd);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecSum sum) {
        out = blankSpec(Builder::setSum);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecTDigest tDigest) {
        out = otherSpec(Builder::setTDigest, AggSpecTDigest.newBuilder()
                .setCompression(tDigest.compression()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecUnique unique) {
        out = otherSpec(Builder::setUnique, AggSpecUnique.newBuilder()
                .setIncludeNulls(unique.includeNulls()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecWAvg wAvg) {
        out = otherSpec(Builder::setWeightedAvg, AggSpecWeighted.newBuilder()
                .setWeightColumn(wAvg.weight().name()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecWSum wSum) {
        out = otherSpec(Builder::setWeightedSum, AggSpecWeighted.newBuilder()
                .setWeightColumn(wSum.weight().name()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecVar var) {
        out = blankSpec(Builder::setVar);
    }
}

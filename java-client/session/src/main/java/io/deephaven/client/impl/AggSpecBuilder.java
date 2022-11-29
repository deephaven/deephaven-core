package io.deephaven.client.impl;

import com.google.protobuf.Empty;
import com.google.protobuf.MessageOrBuilder;
import io.deephaven.api.SortColumn;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecApproximatePercentile;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecCountDistinct;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecDistinct;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecFormula;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecMedian;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecNonUniqueSentinel;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecPercentile;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecSorted;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecSortedColumn;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecTDigest;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecUnique;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecWeighted;
import io.deephaven.proto.backplane.grpc.AggSpec.Builder;

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

    private static AggSpec emptySpec(BiFunction<Builder, Empty, Builder> setter) {
        return setter.apply(newBuilder(), Empty.getDefaultInstance()).build();
    }

    private static <T extends MessageOrBuilder> AggSpec spec(BiFunction<Builder, T, Builder> setter, T obj) {
        return setter.apply(newBuilder(), obj).build();
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecAbsSum absSum) {
        out = emptySpec(Builder::setAbsSum);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecApproximatePercentile approxPct) {
        final AggSpecApproximatePercentile.Builder builder = AggSpecApproximatePercentile.newBuilder()
                .setPercentile(approxPct.percentile());
        approxPct.compression().ifPresent(builder::setCompression);
        out = spec(Builder::setApproximatePercentile, builder);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecAvg avg) {
        out = emptySpec(Builder::setAvg);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecCountDistinct countDistinct) {
        out = spec(Builder::setCountDistinct, AggSpecCountDistinct.newBuilder()
                .setCountNulls(countDistinct.countNulls()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecDistinct distinct) {
        out = spec(Builder::setDistinct, AggSpecDistinct.newBuilder()
                .setIncludeNulls(distinct.includeNulls()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecFirst first) {
        out = emptySpec(Builder::setFirst);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecFormula formula) {
        out = spec(Builder::setFormula, AggSpecFormula.newBuilder()
                .setFormula(formula.formula())
                .setParamToken(formula.paramToken()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecFreeze freeze) {
        out = emptySpec(Builder::setFreeze);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecGroup group) {
        out = emptySpec(Builder::setGroup);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecLast last) {
        out = emptySpec(Builder::setLast);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecMax max) {
        out = emptySpec(Builder::setMax);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecMedian median) {
        out = spec(Builder::setMedian, AggSpecMedian.newBuilder()
                .setAverageEvenlyDivided(median.averageEvenlyDivided()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecMin min) {
        out = emptySpec(Builder::setMin);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecPercentile pct) {
        out = spec(Builder::setPercentile, AggSpecPercentile.newBuilder()
                .setPercentile(pct.percentile())
                .setAverageEvenlyDivided(pct.averageEvenlyDivided()));
    }

    private static AggSpecSortedColumn adapt(SortColumn sortColumn) {
        return AggSpecSortedColumn.newBuilder()
                .setColumnName(sortColumn.column().name())
                .build();
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecSortedFirst sortedFirst) {
        final AggSpecSorted.Builder builder = AggSpecSorted.newBuilder();
        for (SortColumn column : sortedFirst.columns()) {
            builder.addColumns(adapt(column));
        }
        out = spec(Builder::setSortedFirst, builder);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecSortedLast sortedLast) {
        final AggSpecSorted.Builder builder = AggSpecSorted.newBuilder();
        for (SortColumn column : sortedLast.columns()) {
            builder.addColumns(adapt(column));
        }
        out = spec(Builder::setSortedLast, builder);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecStd std) {
        out = emptySpec(Builder::setStd);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecSum sum) {
        out = emptySpec(Builder::setSum);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecTDigest tDigest) {
        final AggSpecTDigest.Builder builder = AggSpecTDigest.newBuilder();
        tDigest.compression().ifPresent(builder::setCompression);
        out = spec(Builder::setTDigest, builder);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecUnique unique) {
        final AggSpecUnique.Builder builder = AggSpecUnique.newBuilder().setIncludeNulls(unique.includeNulls());
        unique.nonUniqueSentinel().map(AggSpecBuilder::adapt).ifPresent(builder::setNonUniqueSentinel);
        out = spec(Builder::setUnique, builder);
    }

    private static AggSpecNonUniqueSentinel adapt(Object o) {
        if (o instanceof String) {
            return AggSpecNonUniqueSentinel.newBuilder().setStringValue((String) o).build();
        }
        if (o instanceof Integer) {
            return AggSpecNonUniqueSentinel.newBuilder().setIntValue((Integer) o).build();
        }
        if (o instanceof Long) {
            return AggSpecNonUniqueSentinel.newBuilder().setLongValue((Long) o).build();
        }
        if (o instanceof Float) {
            return AggSpecNonUniqueSentinel.newBuilder().setFloatValue((Float) o).build();
        }
        if (o instanceof Double) {
            return AggSpecNonUniqueSentinel.newBuilder().setDoubleValue((Double) o).build();
        }
        if (o instanceof Boolean) {
            return AggSpecNonUniqueSentinel.newBuilder().setBoolValue((Boolean) o).build();
        }
        throw new IllegalArgumentException(String.format("Unable to adapt '%s' as non unique sentinel", o.getClass()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecWAvg wAvg) {
        out = spec(Builder::setWeightedAvg, AggSpecWeighted.newBuilder()
                .setWeightColumn(wAvg.weight().name()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecWSum wSum) {
        out = spec(Builder::setWeightedSum, AggSpecWeighted.newBuilder()
                .setWeightColumn(wSum.weight().name()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecVar var) {
        out = emptySpec(Builder::setVar);
    }
}

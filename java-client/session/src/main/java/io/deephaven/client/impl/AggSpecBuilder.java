//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.client.impl;

import com.google.protobuf.MessageOrBuilder;
import io.deephaven.api.SortColumn;
import io.deephaven.api.object.UnionObject;
import io.deephaven.api.object.UnionObject.Visitor;
import io.deephaven.proto.backplane.grpc.AggSpec;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecAbsSum;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecApproximatePercentile;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecAvg;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecCountDistinct;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecDistinct;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecFirst;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecFormula;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecFreeze;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecGroup;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecLast;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecMax;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecMedian;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecMin;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecNonUniqueSentinel;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecPercentile;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecSorted;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecSortedColumn;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecStd;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecSum;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecTDigest;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecUnique;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecVar;
import io.deephaven.proto.backplane.grpc.AggSpec.AggSpecWeighted;
import io.deephaven.proto.backplane.grpc.AggSpec.Builder;
import io.deephaven.proto.backplane.grpc.NullValue;

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

    private static <T extends MessageOrBuilder> AggSpec spec(BiFunction<Builder, T, Builder> setter, T obj) {
        return setter.apply(newBuilder(), obj).build();
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecAbsSum absSum) {
        out = spec(Builder::setAbsSum, AggSpecAbsSum.newBuilder());
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
        out = spec(Builder::setAvg, AggSpecAvg.newBuilder());
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
        out = spec(Builder::setFirst, AggSpecFirst.newBuilder());
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecFormula formula) {
        out = spec(Builder::setFormula, AggSpecFormula.newBuilder()
                .setFormula(formula.formula())
                .setParamToken(formula.paramToken()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecFreeze freeze) {
        out = spec(Builder::setFreeze, AggSpecFreeze.newBuilder());
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecGroup group) {
        out = spec(Builder::setGroup, AggSpecGroup.newBuilder());
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecLast last) {
        out = spec(Builder::setLast, AggSpecLast.newBuilder());
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecMax max) {
        out = spec(Builder::setMax, AggSpecMax.newBuilder());
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecMedian median) {
        out = spec(Builder::setMedian, AggSpecMedian.newBuilder()
                .setAverageEvenlyDivided(median.averageEvenlyDivided()));
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecMin min) {
        out = spec(Builder::setMin, AggSpecMin.newBuilder());
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
        out = spec(Builder::setStd, AggSpecStd.newBuilder());
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecSum sum) {
        out = spec(Builder::setSum, AggSpecSum.newBuilder());
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecTDigest tDigest) {
        final AggSpecTDigest.Builder builder = AggSpecTDigest.newBuilder();
        tDigest.compression().ifPresent(builder::setCompression);
        out = spec(Builder::setTDigest, builder);
    }

    @Override
    public void visit(io.deephaven.api.agg.spec.AggSpecUnique unique) {
        final AggSpecUnique.Builder builder = AggSpecUnique.newBuilder()
                .setIncludeNulls(unique.includeNulls())
                .setNonUniqueSentinel(adapt(unique.nonUniqueSentinel().orElse(null)));
        out = spec(Builder::setUnique, builder);
    }

    private static AggSpecNonUniqueSentinel adapt(UnionObject obj) {
        return obj == null ? AggSpecNonUniqueSentinel.newBuilder().setNullValue(NullValue.NULL_VALUE).build()
                : obj.visit(AggSpecNonUniqueSentinelAdapter.INSTANCE);
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
        out = spec(Builder::setVar, AggSpecVar.newBuilder());
    }

    public enum AggSpecNonUniqueSentinelAdapter implements Visitor<AggSpecNonUniqueSentinel> {
        INSTANCE;

        @Override
        public AggSpecNonUniqueSentinel visit(boolean x) {
            return AggSpecNonUniqueSentinel.newBuilder().setBoolValue(x).build();
        }

        @Override
        public AggSpecNonUniqueSentinel visit(char x) {
            return AggSpecNonUniqueSentinel.newBuilder().setCharValue(x).build();
        }

        @Override
        public AggSpecNonUniqueSentinel visit(byte x) {
            return AggSpecNonUniqueSentinel.newBuilder().setByteValue(x).build();
        }

        @Override
        public AggSpecNonUniqueSentinel visit(short x) {
            return AggSpecNonUniqueSentinel.newBuilder().setShortValue(x).build();
        }

        @Override
        public AggSpecNonUniqueSentinel visit(int x) {
            return AggSpecNonUniqueSentinel.newBuilder().setIntValue(x).build();
        }

        @Override
        public AggSpecNonUniqueSentinel visit(long x) {
            return AggSpecNonUniqueSentinel.newBuilder().setLongValue(x).build();
        }

        @Override
        public AggSpecNonUniqueSentinel visit(float x) {
            return AggSpecNonUniqueSentinel.newBuilder().setFloatValue(x).build();
        }

        @Override
        public AggSpecNonUniqueSentinel visit(double x) {
            return AggSpecNonUniqueSentinel.newBuilder().setDoubleValue(x).build();
        }

        @Override
        public AggSpecNonUniqueSentinel visit(Object x) {
            if (x instanceof String) {
                return AggSpecNonUniqueSentinel.newBuilder().setStringValue((String) x).build();
            }
            throw new IllegalArgumentException(
                    String.format("Unable to adapt AggSpecNonUniqueSentinel type %s", x.getClass()));
        }
    }
}

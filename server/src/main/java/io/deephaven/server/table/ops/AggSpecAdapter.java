//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.rpc.Code;
import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.agg.spec.AggSpec.Visitor;
import io.deephaven.api.agg.spec.AggSpecSortedFirst;
import io.deephaven.api.agg.spec.AggSpecSortedLast;
import io.deephaven.api.agg.spec.AggSpecWAvg;
import io.deephaven.api.agg.spec.AggSpecWSum;
import io.deephaven.api.object.UnionObject;
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
import io.deephaven.proto.backplane.grpc.AggSpec.TypeCase;
import io.deephaven.proto.backplane.grpc.NullValue;
import io.deephaven.proto.util.Exceptions;
import io.deephaven.server.grpc.GrpcErrorHelper;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.deephaven.server.grpc.GrpcErrorHelper.extractField;

class AggSpecAdapter {

    enum Singleton {
        INSTANCE;

        private final Adapters adapters;

        Singleton() {
            adapters = new Adapters();
            AggSpec.visitAll(adapters);
        }

        Adapters adapters() {
            return adapters;
        }
    }

    public static void validate(io.deephaven.proto.backplane.grpc.AggSpec spec) {
        // It's a bit unfortunate that generated protobuf objects don't have the names as constants (like it does with
        // field numbers). For example, AggSpec.TYPE_NAME.
        GrpcErrorHelper.checkHasOneOf(spec, "type");
        GrpcErrorHelper.checkHasNoUnknownFields(spec);
        Singleton.INSTANCE.adapters.validate(spec);
    }

    public static void validate(AggSpecUnique aggSpecUnique) {
        GrpcErrorHelper.checkHasNoUnknownFields(aggSpecUnique);
        GrpcErrorHelper.checkHasField(aggSpecUnique, AggSpecUnique.NON_UNIQUE_SENTINEL_FIELD_NUMBER);
        validate(aggSpecUnique.getNonUniqueSentinel());
    }

    public static void validate(AggSpecNonUniqueSentinel nonUniqueSentinel) {
        GrpcErrorHelper.checkHasOneOf(nonUniqueSentinel, "type");
        GrpcErrorHelper.checkHasNoUnknownFieldsRecursive(nonUniqueSentinel);
        final AggSpecNonUniqueSentinel.TypeCase type = nonUniqueSentinel.getTypeCase();
        switch (type) {
            case STRING_VALUE:
            case INT_VALUE:
            case LONG_VALUE:
            case FLOAT_VALUE:
            case DOUBLE_VALUE:
            case BOOL_VALUE:
                // All native proto types valid
                return;
            case NULL_VALUE:
                if (nonUniqueSentinel.getNullValue() != NullValue.NULL_VALUE) {
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "AggSpecNonUniqueSentinel null_value out of range");
                }
                return;
            case BYTE_VALUE:
                if (nonUniqueSentinel.getByteValue() != (byte) nonUniqueSentinel.getByteValue()) {
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "AggSpecNonUniqueSentinel byte_value out of range");
                }
                return;
            case SHORT_VALUE:
                if (nonUniqueSentinel.getShortValue() != (short) nonUniqueSentinel.getShortValue()) {
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "AggSpecNonUniqueSentinel short_value out of range");
                }
                return;
            case CHAR_VALUE:
                if (nonUniqueSentinel.getCharValue() != (char) nonUniqueSentinel.getCharValue()) {
                    throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT,
                            "AggSpecNonUniqueSentinel char_value out of range");
                }
                return;
            case TYPE_NOT_SET:
                // Should be caught by checkHasOneOf, fall-through to internal error if not.
            default:
                throw Exceptions.statusRuntimeException(Code.INTERNAL,
                        String.format("Server missing AggSpecNonUniqueSentinel type %s", type));
        }
    }

    public static AggSpec adapt(io.deephaven.proto.backplane.grpc.AggSpec spec) {
        return Singleton.INSTANCE.adapters.adapt(spec);
    }

    private static io.deephaven.api.agg.spec.AggSpecApproximatePercentile adapt(
            AggSpecApproximatePercentile percentile) {
        return percentile.hasCompression()
                ? AggSpec.approximatePercentile(percentile.getPercentile(), percentile.getCompression())
                : AggSpec.approximatePercentile(percentile.getPercentile());
    }

    private static io.deephaven.api.agg.spec.AggSpecCountDistinct adapt(AggSpecCountDistinct countDistinct) {
        return AggSpec.countDistinct(countDistinct.getCountNulls());
    }

    private static io.deephaven.api.agg.spec.AggSpecDistinct adapt(AggSpecDistinct distinct) {
        return AggSpec.distinct(distinct.getIncludeNulls());
    }

    private static io.deephaven.api.agg.spec.AggSpecFormula adapt(AggSpecFormula formula) {
        return AggSpec.formula(formula.getFormula(), formula.getParamToken());
    }

    private static io.deephaven.api.agg.spec.AggSpecMedian adapt(AggSpecMedian median) {
        return AggSpec.median(median.getAverageEvenlyDivided());
    }

    private static io.deephaven.api.agg.spec.AggSpecPercentile adapt(AggSpecPercentile percentile) {
        return AggSpec.percentile(percentile.getPercentile(), percentile.getAverageEvenlyDivided());
    }

    private static SortColumn adapt(AggSpecSortedColumn sortedColumn) {
        return SortColumn.asc(ColumnName.of(sortedColumn.getColumnName()));
    }

    private static AggSpecSortedFirst adaptFirst(AggSpecSorted sorted) {
        AggSpecSortedFirst.Builder builder = AggSpecSortedFirst.builder();
        for (AggSpecSortedColumn sortedColumn : sorted.getColumnsList()) {
            builder.addColumns(adapt(sortedColumn));
        }
        return builder.build();
    }

    private static AggSpecSortedLast adaptLast(AggSpecSorted sorted) {
        AggSpecSortedLast.Builder builder = AggSpecSortedLast.builder();
        for (AggSpecSortedColumn sortDescriptor : sorted.getColumnsList()) {
            builder.addColumns(adapt(sortDescriptor));
        }
        return builder.build();
    }

    private static io.deephaven.api.agg.spec.AggSpecTDigest adapt(AggSpecTDigest tDigest) {
        return tDigest.hasCompression() ? AggSpec.tDigest(tDigest.getCompression()) : AggSpec.tDigest();
    }

    private static io.deephaven.api.agg.spec.AggSpecUnique adapt(AggSpecUnique unique) {
        UnionObject nonUniqueSentinel = adapt(unique.getNonUniqueSentinel());
        return io.deephaven.api.agg.spec.AggSpecUnique.of(unique.getIncludeNulls(), nonUniqueSentinel);
    }

    private static UnionObject adapt(AggSpecNonUniqueSentinel nonUniqueSentinel) {
        switch (nonUniqueSentinel.getTypeCase()) {
            case NULL_VALUE:
                return null;
            case STRING_VALUE:
                return UnionObject.of(nonUniqueSentinel.getStringValue());
            case INT_VALUE:
                return UnionObject.of(nonUniqueSentinel.getIntValue());
            case LONG_VALUE:
                return UnionObject.of(nonUniqueSentinel.getLongValue());
            case FLOAT_VALUE:
                return UnionObject.of(nonUniqueSentinel.getFloatValue());
            case DOUBLE_VALUE:
                return UnionObject.of(nonUniqueSentinel.getDoubleValue());
            case BOOL_VALUE:
                return UnionObject.of(nonUniqueSentinel.getBoolValue());
            case BYTE_VALUE:
                return UnionObject.of((byte) nonUniqueSentinel.getByteValue());
            case SHORT_VALUE:
                return UnionObject.of((short) nonUniqueSentinel.getShortValue());
            case CHAR_VALUE:
                return UnionObject.of((char) nonUniqueSentinel.getCharValue());
            case TYPE_NOT_SET:
                throw Exceptions.statusRuntimeException(Code.INVALID_ARGUMENT, "AggSpecNonUniqueSentinel type not set");
            default:
                throw Exceptions.statusRuntimeException(Code.INTERNAL, String
                        .format("Server is missing AggSpecNonUniqueSentinel case %s", nonUniqueSentinel.getTypeCase()));
        }
    }

    private static AggSpecWAvg adaptWeightedAvg(AggSpecWeighted weighted) {
        return AggSpec.wavg(weighted.getWeightColumn());
    }

    private static AggSpecWSum adaptWeightedSum(AggSpecWeighted weighted) {
        return AggSpec.wsum(weighted.getWeightColumn());
    }

    static class Adapters implements Visitor {
        final Map<TypeCase, Adapter<?, ?>> adapters = new HashMap<>();
        final Set<TypeCase> unimplemented = new HashSet<>();

        public void validate(io.deephaven.proto.backplane.grpc.AggSpec spec) {
            get(spec.getTypeCase()).validate(spec);
        }

        public AggSpec adapt(io.deephaven.proto.backplane.grpc.AggSpec spec) {
            return get(spec.getTypeCase()).adapt(spec);
        }

        private Adapter<?, ?> get(TypeCase type) {
            final Adapter<?, ?> adapter = adapters.get(type);
            if (adapter != null) {
                return adapter;
            }
            if (unimplemented.contains(type)) {
                throw Exceptions.statusRuntimeException(Code.UNIMPLEMENTED,
                        String.format("AggSpec type %s is unimplemented", type));
            }
            throw Exceptions.statusRuntimeException(Code.INTERNAL,
                    String.format("Server is missing AggSpec type %s", type));
        }

        private <I extends Message, T extends AggSpec> void add(
                TypeCase typeCase,
                Class<I> iClazz,
                // Used to help with type-safety
                @SuppressWarnings("unused") Class<T> tClazz,
                Consumer<I> validator,
                Function<I, T> adapter) {
            try {
                if (adapters.put(typeCase, Adapter.create(typeCase, iClazz, validator, adapter)) != null) {
                    throw new IllegalStateException("Adapters have been constructed incorrectly");
                }
            } catch (NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new IllegalStateException("Adapters logical error", e);
            }
        }

        private <I extends Message, T extends AggSpec> void add(
                TypeCase typeCase,
                Class<I> iClazz,
                // Used to help with type-safety
                @SuppressWarnings("unused") Class<T> tClazz,
                Consumer<I> validator,
                Supplier<T> supplier) {
            add(typeCase, iClazz, tClazz, validator, m -> supplier.get());
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecAbsSum absSum) {
            add(
                    TypeCase.ABS_SUM,
                    AggSpecAbsSum.class,
                    io.deephaven.api.agg.spec.AggSpecAbsSum.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::absSum);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecApproximatePercentile approxPct) {
            add(
                    TypeCase.APPROXIMATE_PERCENTILE,
                    AggSpecApproximatePercentile.class,
                    io.deephaven.api.agg.spec.AggSpecApproximatePercentile.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpecAdapter::adapt);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecAvg avg) {
            add(
                    TypeCase.AVG,
                    AggSpecAvg.class,
                    io.deephaven.api.agg.spec.AggSpecAvg.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::avg);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecCountDistinct countDistinct) {
            add(
                    TypeCase.COUNT_DISTINCT,
                    AggSpecCountDistinct.class,
                    io.deephaven.api.agg.spec.AggSpecCountDistinct.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpecAdapter::adapt);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecDistinct distinct) {
            add(
                    TypeCase.DISTINCT,
                    AggSpecDistinct.class,
                    io.deephaven.api.agg.spec.AggSpecDistinct.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpecAdapter::adapt);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecFirst first) {
            add(
                    TypeCase.FIRST,
                    AggSpecFirst.class,
                    io.deephaven.api.agg.spec.AggSpecFirst.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::first);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecFormula formula) {
            add(
                    TypeCase.FORMULA,
                    AggSpecFormula.class,
                    io.deephaven.api.agg.spec.AggSpecFormula.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpecAdapter::adapt);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecFreeze freeze) {
            add(
                    TypeCase.FREEZE,
                    AggSpecFreeze.class,
                    io.deephaven.api.agg.spec.AggSpecFreeze.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::freeze);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecGroup group) {
            add(
                    TypeCase.GROUP,
                    AggSpecGroup.class,
                    io.deephaven.api.agg.spec.AggSpecGroup.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::group);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecLast last) {
            add(
                    TypeCase.LAST,
                    AggSpecLast.class,
                    io.deephaven.api.agg.spec.AggSpecLast.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::last);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecMax max) {
            add(
                    TypeCase.MAX,
                    AggSpecMax.class,
                    io.deephaven.api.agg.spec.AggSpecMax.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::max);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecMedian median) {
            add(
                    TypeCase.MEDIAN,
                    AggSpecMedian.class,
                    io.deephaven.api.agg.spec.AggSpecMedian.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpecAdapter::adapt);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecMin min) {
            add(
                    TypeCase.MIN,
                    AggSpecMin.class,
                    io.deephaven.api.agg.spec.AggSpecMin.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::min);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecPercentile pct) {
            add(
                    TypeCase.PERCENTILE,
                    AggSpecPercentile.class,
                    io.deephaven.api.agg.spec.AggSpecPercentile.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpecAdapter::adapt);
        }

        @Override
        public void visit(AggSpecSortedFirst sortedFirst) {
            add(
                    TypeCase.SORTED_FIRST,
                    AggSpecSorted.class,
                    AggSpecSortedFirst.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpecAdapter::adaptFirst);
        }

        @Override
        public void visit(AggSpecSortedLast sortedLast) {
            add(
                    TypeCase.SORTED_LAST,
                    AggSpecSorted.class,
                    AggSpecSortedLast.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpecAdapter::adaptLast);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecStd std) {
            add(
                    TypeCase.STD,
                    AggSpecStd.class,
                    io.deephaven.api.agg.spec.AggSpecStd.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::std);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecSum sum) {
            add(
                    TypeCase.SUM,
                    AggSpecSum.class,
                    io.deephaven.api.agg.spec.AggSpecSum.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::sum);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecTDigest tDigest) {
            add(
                    TypeCase.T_DIGEST,
                    AggSpecTDigest.class,
                    io.deephaven.api.agg.spec.AggSpecTDigest.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpecAdapter::adapt);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecUnique unique) {
            add(
                    TypeCase.UNIQUE,
                    AggSpecUnique.class,
                    io.deephaven.api.agg.spec.AggSpecUnique.class,
                    AggSpecAdapter::validate,
                    AggSpecAdapter::adapt);
        }

        @Override
        public void visit(AggSpecWAvg wAvg) {
            add(
                    TypeCase.WEIGHTED_AVG,
                    AggSpecWeighted.class,
                    AggSpecWAvg.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpecAdapter::adaptWeightedAvg);
        }

        @Override
        public void visit(AggSpecWSum wSum) {
            add(
                    TypeCase.WEIGHTED_SUM,
                    AggSpecWeighted.class,
                    AggSpecWSum.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpecAdapter::adaptWeightedSum);
        }

        @Override
        public void visit(io.deephaven.api.agg.spec.AggSpecVar var) {
            add(
                    TypeCase.VAR,
                    AggSpecVar.class,
                    io.deephaven.api.agg.spec.AggSpecVar.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::var);

        }

        // New types _can_ be added as unimplemented.
        // If so, please create and link to a ticket.
        // @Override
        // public void visit(SomeNewType someNewType) {
        // unimplemented.add(TypeCase.SOME_NEW_TYPE);
        // }
    }

    private static class Adapter<I extends Message, T extends AggSpec> {

        public static <I extends Message, T extends AggSpec> Adapter<I, T> create(
                TypeCase typeCase,
                Class<I> clazz,
                Consumer<I> validator,
                Function<I, T> adapter)
                throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            final FieldDescriptor field = extractField(io.deephaven.proto.backplane.grpc.AggSpec.getDescriptor(),
                    typeCase.getNumber(), clazz);
            return new Adapter<>(field, validator, adapter);
        }

        private final FieldDescriptor field;
        private final Consumer<I> validator;
        private final Function<I, T> adapter;

        private Adapter(FieldDescriptor field, Consumer<I> validator, Function<I, T> adapter) {
            this.field = field;
            this.validator = Objects.requireNonNull(validator);
            this.adapter = Objects.requireNonNull(adapter);
        }

        public void validate(io.deephaven.proto.backplane.grpc.AggSpec spec) {
            // noinspection unchecked
            validator.accept((I) spec.getField(field));
        }

        public T adapt(io.deephaven.proto.backplane.grpc.AggSpec spec) {
            // noinspection unchecked
            return adapter.apply((I) spec.getField(field));
        }
    }
}

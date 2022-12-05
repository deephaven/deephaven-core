package io.deephaven.server.table.ops;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.google.rpc.Code;
import io.deephaven.api.ColumnName;
import io.deephaven.api.SortColumn;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.api.agg.spec.AggSpec.Visitor;
import io.deephaven.api.agg.spec.AggSpecAbsSum;
import io.deephaven.api.agg.spec.AggSpecAvg;
import io.deephaven.api.agg.spec.AggSpecFirst;
import io.deephaven.api.agg.spec.AggSpecFreeze;
import io.deephaven.api.agg.spec.AggSpecGroup;
import io.deephaven.api.agg.spec.AggSpecLast;
import io.deephaven.api.agg.spec.AggSpecMax;
import io.deephaven.api.agg.spec.AggSpecMin;
import io.deephaven.api.agg.spec.AggSpecSortedFirst;
import io.deephaven.api.agg.spec.AggSpecSortedLast;
import io.deephaven.api.agg.spec.AggSpecStd;
import io.deephaven.api.agg.spec.AggSpecSum;
import io.deephaven.api.agg.spec.AggSpecVar;
import io.deephaven.api.agg.spec.AggSpecWAvg;
import io.deephaven.api.agg.spec.AggSpecWSum;
import io.deephaven.extensions.barrage.util.GrpcUtil;
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
import io.deephaven.proto.backplane.grpc.AggSpec.TypeCase;
import io.deephaven.proto.backplane.grpc.TableReference;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import static io.deephaven.server.table.ops.GrpcErrorHelper.extractField;

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
        Object nonUniqueSentinel = unique.hasNonUniqueSentinel() ? adapt(unique.getNonUniqueSentinel()) : null;
        return io.deephaven.api.agg.spec.AggSpecUnique.of(unique.getIncludeNulls(), nonUniqueSentinel);
    }

    private static Object adapt(AggSpecNonUniqueSentinel nonUniqueSentinel) {
        switch (nonUniqueSentinel.getTypeCase()) {
            case STRING_VALUE:
                return nonUniqueSentinel.getStringValue();
            case INT_VALUE:
                return nonUniqueSentinel.getIntValue();
            case LONG_VALUE:
                return nonUniqueSentinel.getLongValue();
            case FLOAT_VALUE:
                return nonUniqueSentinel.getFloatValue();
            case DOUBLE_VALUE:
                return nonUniqueSentinel.getDoubleValue();
            case BOOL_VALUE:
                return nonUniqueSentinel.getBoolValue();
            case CELL_VALUE:
                throw GrpcUtil.statusRuntimeException(Code.UNIMPLEMENTED, "AggSpecNonUniqueSentinel cell_value not implemented, see <todo>");
            case TYPE_NOT_SET:
                throw GrpcUtil.statusRuntimeException(Code.INVALID_ARGUMENT, "AggSpecNonUniqueSentinel type not set");
            default:
                throw GrpcUtil.statusRuntimeException(Code.INTERNAL, String
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
                throw GrpcUtil.statusRuntimeException(Code.UNIMPLEMENTED,
                        String.format("AggSpec type %s is unimplemented", type));
            }
            throw GrpcUtil.statusRuntimeException(Code.INTERNAL,
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
        public void visit(AggSpecAbsSum absSum) {
            add(
                    TypeCase.ABS_SUM,
                    Empty.class,
                    AggSpecAbsSum.class,
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
        public void visit(AggSpecAvg avg) {
            add(
                    TypeCase.AVG,
                    Empty.class,
                    AggSpecAvg.class,
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
        public void visit(AggSpecFirst first) {
            add(
                    TypeCase.FIRST,
                    Empty.class,
                    AggSpecFirst.class,
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
        public void visit(AggSpecFreeze freeze) {
            add(
                    TypeCase.FREEZE,
                    Empty.class,
                    AggSpecFreeze.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::freeze);
        }

        @Override
        public void visit(AggSpecGroup group) {
            add(
                    TypeCase.GROUP,
                    Empty.class,
                    AggSpecGroup.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::group);
        }

        @Override
        public void visit(AggSpecLast last) {
            add(
                    TypeCase.LAST,
                    Empty.class,
                    AggSpecLast.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::last);
        }

        @Override
        public void visit(AggSpecMax max) {
            add(
                    TypeCase.MAX,
                    Empty.class,
                    AggSpecMax.class,
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
        public void visit(AggSpecMin min) {
            add(
                    TypeCase.MIN,
                    Empty.class,
                    AggSpecMin.class,
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
        public void visit(AggSpecStd std) {
            add(
                    TypeCase.STD,
                    Empty.class,
                    AggSpecStd.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggSpec::std);
        }

        @Override
        public void visit(AggSpecSum sum) {
            add(
                    TypeCase.SUM,
                    Empty.class,
                    AggSpecSum.class,
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
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
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
        public void visit(AggSpecVar var) {
            add(
                    TypeCase.VAR,
                    Empty.class,
                    AggSpecVar.class,
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

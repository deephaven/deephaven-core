//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.table.ops;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import com.google.rpc.Code;
import io.deephaven.api.agg.*;
import io.deephaven.api.agg.spec.AggSpec;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationColumns;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationCount;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationCountWhere;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationFormula;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationPartition;
import io.deephaven.proto.backplane.grpc.Aggregation.AggregationRowKey;
import io.deephaven.proto.backplane.grpc.Aggregation.TypeCase;
import io.deephaven.proto.backplane.grpc.Selectable;
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

import static io.deephaven.server.grpc.GrpcErrorHelper.extractField;

public class AggregationAdapter {

    enum Singleton {
        INSTANCE;

        private final Adapters adapters;

        Singleton() {
            adapters = new Adapters();
            Aggregation.visitAll(adapters);
        }

        Adapters adapters() {
            return adapters;
        }
    }

    public static void validate(io.deephaven.proto.backplane.grpc.Aggregation aggregation) {
        // It's a bit unfortunate that generated protobuf objects don't have the names as constants (like it does with
        // field numbers). For example, Aggregation.TYPE_NAME.
        GrpcErrorHelper.checkHasOneOf(aggregation, "type");
        GrpcErrorHelper.checkHasNoUnknownFields(aggregation);
        Singleton.INSTANCE.adapters().validate(aggregation);
    }

    private static io.deephaven.api.Selectable adapt(Selectable selectable) {
        switch (selectable.getTypeCase()) {
            case RAW:
                return io.deephaven.api.Selectable.parse(selectable.getRaw());
            default:
                throw new IllegalArgumentException(String.format("Unsupported Selectable type (%s) in Aggregation.",
                        selectable.getTypeCase()));
        }
    }

    public static Aggregation adapt(io.deephaven.proto.backplane.grpc.Aggregation aggregation) {
        return Singleton.INSTANCE.adapters().adapt(aggregation);
    }

    public static void validate(AggregationColumns columns) {
        GrpcErrorHelper.checkHasField(columns, AggregationColumns.SPEC_FIELD_NUMBER);
        GrpcErrorHelper.checkRepeatedFieldNonEmpty(columns, AggregationColumns.MATCH_PAIRS_FIELD_NUMBER);
        GrpcErrorHelper.checkHasNoUnknownFields(columns);
        AggSpecAdapter.validate(columns.getSpec());
    }

    public static Aggregation adapt(AggregationColumns aggregationColumns) {
        final AggSpec spec = AggSpecAdapter.adapt(aggregationColumns.getSpec());
        return Aggregation.of(spec, aggregationColumns.getMatchPairsList());
    }

    public static Count adapt(AggregationCount count) {
        return Aggregation.AggCount(count.getColumnName());
    }

    public static CountWhere adapt(AggregationCountWhere count) {
        return Aggregation.AggCountWhere(count.getColumnName(), count.getFiltersList().toArray(String[]::new));
    }

    public static Formula adapt(AggregationFormula formula) {
        return Formula.of(adapt(formula.getSelectable()));
    }

    public static FirstRowKey adaptFirst(AggregationRowKey key) {
        return Aggregation.AggFirstRowKey(key.getColumnName());
    }

    public static LastRowKey adaptLast(AggregationRowKey key) {
        return Aggregation.AggLastRowKey(key.getColumnName());
    }

    public static Partition adapt(AggregationPartition partition) {
        return Aggregation.AggPartition(partition.getColumnName(), partition.getIncludeGroupByColumns());
    }

    static class Adapters implements Aggregation.Visitor {

        final Map<TypeCase, Adapter<?, ?>> adapters = new HashMap<>();
        final Set<TypeCase> unimplemented = new HashSet<>();

        public void validate(io.deephaven.proto.backplane.grpc.Aggregation aggregation) {
            get(aggregation.getTypeCase()).validate(aggregation);
        }

        public Aggregation adapt(io.deephaven.proto.backplane.grpc.Aggregation aggregation) {
            return get(aggregation.getTypeCase()).adapt(aggregation);
        }

        private Adapter<?, ?> get(io.deephaven.proto.backplane.grpc.Aggregation.TypeCase type) {
            final Adapter<?, ?> adapter = adapters.get(type);
            if (adapter != null) {
                return adapter;
            }
            if (unimplemented.contains(type)) {
                throw Exceptions.statusRuntimeException(Code.UNIMPLEMENTED,
                        String.format("Aggregation type %s is unimplemented", type));
            }
            throw Exceptions.statusRuntimeException(Code.INTERNAL,
                    String.format("Server is missing Aggregation type %s", type));
        }

        private <I extends Message, T extends Aggregation> void add(
                io.deephaven.proto.backplane.grpc.Aggregation.TypeCase typeCase,
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

        @Override
        public void visit(Aggregations aggregations) {
            // no direct equivalent, handled by higher layers
        }

        @Override
        public void visit(ColumnAggregation columnAgg) {
            // see ColumnAggregations
        }

        @Override
        public void visit(ColumnAggregations columnAggs) {
            add(
                    TypeCase.COLUMNS,
                    AggregationColumns.class,
                    // may be ColumnAggregation or ColumnAggregations
                    Aggregation.class,
                    AggregationAdapter::validate,
                    AggregationAdapter::adapt);
        }

        @Override
        public void visit(Count count) {
            add(
                    TypeCase.COUNT,
                    AggregationCount.class,
                    Count.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggregationAdapter::adapt);
        }

        public void visit(CountWhere countWhere) {
            add(
                    TypeCase.COUNT_WHERE,
                    AggregationCountWhere.class,
                    CountWhere.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggregationAdapter::adapt);
        }

        @Override
        public void visit(FirstRowKey firstRowKey) {
            add(
                    TypeCase.FIRST_ROW_KEY,
                    AggregationRowKey.class,
                    FirstRowKey.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggregationAdapter::adaptFirst);
        }

        @Override
        public void visit(LastRowKey lastRowKey) {
            add(
                    TypeCase.LAST_ROW_KEY,
                    AggregationRowKey.class,
                    LastRowKey.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggregationAdapter::adaptLast);
        }

        @Override
        public void visit(Partition partition) {
            add(
                    TypeCase.PARTITION,
                    AggregationPartition.class,
                    Partition.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggregationAdapter::adapt);
        }

        @Override
        public void visit(Formula formula) {
            add(
                    TypeCase.FORMULA,
                    AggregationFormula.class,
                    Formula.class,
                    GrpcErrorHelper::checkHasNoUnknownFieldsRecursive,
                    AggregationAdapter::adapt);
        }

        // New types _can_ be added as unimplemented.
        // If so, please create and link to a ticket.
        // @Override
        // public void visit(SomeNewType someNewType) {
        // unimplemented.add(TypeCase.SOME_NEW_TYPE);
        // }
    }

    private static class Adapter<I extends Message, T extends Aggregation> {

        public static <I extends Message, T extends Aggregation> Adapter<I, T> create(
                TypeCase typeCase,
                Class<I> clazz,
                Consumer<I> validator,
                Function<I, T> adapter)
                throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
            final FieldDescriptor field = extractField(io.deephaven.proto.backplane.grpc.Aggregation.getDescriptor(),
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

        public void validate(io.deephaven.proto.backplane.grpc.Aggregation aggregation) {
            // noinspection unchecked
            validator.accept((I) aggregation.getField(field));
        }

        public T adapt(io.deephaven.proto.backplane.grpc.Aggregation aggregation) {
            // noinspection unchecked
            return adapter.apply((I) aggregation.getField(field));
        }
    }
}

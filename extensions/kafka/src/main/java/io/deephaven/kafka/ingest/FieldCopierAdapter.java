/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka.ingest;

import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.functions.ToBooleanFunction;
import io.deephaven.functions.ToByteFunction;
import io.deephaven.functions.ToCharFunction;
import io.deephaven.functions.ToDoubleFunction;
import io.deephaven.functions.ToFloatFunction;
import io.deephaven.functions.ToIntFunction;
import io.deephaven.functions.ToLongFunction;
import io.deephaven.functions.ToObjectFunction;
import io.deephaven.functions.ToPrimitiveFunction;
import io.deephaven.functions.ToShortFunction;
import io.deephaven.functions.TypedFunction;

public enum FieldCopierAdapter
        implements TypedFunction.Visitor<Object, FieldCopier>, ToPrimitiveFunction.Visitor<Object, FieldCopier> {
    INSTANCE;

    public static FieldCopier of(TypedFunction<Object> f) {
        return f.walk(INSTANCE);
    }

    public static FieldCopier of(ToPrimitiveFunction<Object> f) {
        return f.walk((ToPrimitiveFunction.Visitor<Object, FieldCopier>) INSTANCE);
    }

    public static FieldCopier of(ToBooleanFunction<Object> f) {
        return BooleanFieldCopier.of(f);
    }

    public static FieldCopier of(ToCharFunction<Object> f) {
        return CharFieldCopier.of(f);
    }

    public static FieldCopier of(ToByteFunction<Object> f) {
        return ByteFieldCopier.of(f);
    }

    public static FieldCopier of(ToShortFunction<Object> f) {
        return ShortFieldCopier.of(f);
    }

    public static FieldCopier of(ToIntFunction<Object> f) {
        return IntFieldCopier.of(f);
    }

    public static FieldCopier of(ToLongFunction<Object> f) {
        return LongFieldCopier.of(f);
    }

    public static FieldCopier of(ToFloatFunction<Object> f) {
        return FloatFieldCopier.of(f);
    }

    public static FieldCopier of(ToDoubleFunction<Object> f) {
        return DoubleFieldCopier.of(f);
    }

    public static FieldCopier of(ToObjectFunction<Object, ?> f) {
        if (f.returnType().equals(BoxedBooleanType.of())) {
            return ByteFieldCopier.ofBoolean(f.cast(BoxedBooleanType.of()));
        }
        return ObjectFieldCopier.of(f);
    }

    @Override
    public FieldCopier visit(ToPrimitiveFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(ToBooleanFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(ToCharFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(ToByteFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(ToShortFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(ToIntFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(ToLongFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(ToFloatFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(ToDoubleFunction<Object> f) {
        return of(f);
    }

    @Override
    public FieldCopier visit(ToObjectFunction<Object, ?> f) {
        return of(f);
    }
}

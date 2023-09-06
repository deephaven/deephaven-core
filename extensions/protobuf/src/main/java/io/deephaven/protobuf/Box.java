/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

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
import io.deephaven.functions.TypedFunction.Visitor;

class Box<T> implements Visitor<T, Object>, ToPrimitiveFunction.Visitor<T, Object> {
    public static <T> Object apply(TypedFunction<T> f, T value) {
        return f.walk(new Box<>(value));
    }

    public static <T> Object apply(ToPrimitiveFunction<T> f, T value) {
        return f.walk((ToPrimitiveFunction.Visitor<T, Object>) new Box<>(value));
    }

    private final T value;

    private Box(T value) {
        this.value = value;
    }

    @Override
    public Object visit(ToObjectFunction<T, ?> f) {
        return f.apply(value);
    }

    @Override
    public Object visit(ToPrimitiveFunction<T> f) {
        return f.walk((ToPrimitiveFunction.Visitor<T, Object>) this);
    }

    @Override
    public Boolean visit(ToBooleanFunction<T> f) {
        return f.test(value);
    }

    @Override
    public Character visit(ToCharFunction<T> f) {
        return f.applyAsChar(value);
    }

    @Override
    public Byte visit(ToByteFunction<T> f) {
        return f.applyAsByte(value);
    }

    @Override
    public Short visit(ToShortFunction<T> f) {
        return f.applyAsShort(value);
    }

    @Override
    public Integer visit(ToIntFunction<T> f) {
        return f.applyAsInt(value);
    }

    @Override
    public Long visit(ToLongFunction<T> f) {
        return f.applyAsLong(value);
    }

    @Override
    public Float visit(ToFloatFunction<T> f) {
        return f.applyAsFloat(value);
    }

    @Override
    public Double visit(ToDoubleFunction<T> f) {
        return f.applyAsDouble(value);
    }
}

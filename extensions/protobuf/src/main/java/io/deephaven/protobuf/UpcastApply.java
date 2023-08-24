/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import io.deephaven.functions.BooleanFunction;
import io.deephaven.functions.ByteFunction;
import io.deephaven.functions.CharFunction;
import io.deephaven.functions.DoubleFunction;
import io.deephaven.functions.FloatFunction;
import io.deephaven.functions.IntFunction;
import io.deephaven.functions.LongFunction;
import io.deephaven.functions.ObjectFunction;
import io.deephaven.functions.PrimitiveFunction;
import io.deephaven.functions.ShortFunction;
import io.deephaven.functions.TypedFunction;
import io.deephaven.functions.TypedFunction.Visitor;

class UpcastApply<T> implements Visitor<T, Object>, PrimitiveFunction.Visitor<T, Object> {
    public static <T> Object apply(TypedFunction<T> f, T value) {
        return f.walk(new UpcastApply<>(value));
    }

    public static <T> Object apply(PrimitiveFunction<T> f, T value) {
        return f.walk((PrimitiveFunction.Visitor<T, Object>) new UpcastApply<>(value));
    }

    private final T value;

    private UpcastApply(T value) {
        this.value = value;
    }

    @Override
    public Object visit(ObjectFunction<T, ?> f) {
        return f.apply(value);
    }

    @Override
    public Object visit(PrimitiveFunction<T> f) {
        return f.walk((PrimitiveFunction.Visitor<T, Object>) this);
    }

    @Override
    public Object visit(BooleanFunction<T> f) {
        return f.test(value);
    }

    @Override
    public Object visit(CharFunction<T> f) {
        return f.applyAsChar(value);
    }

    @Override
    public Object visit(ByteFunction<T> f) {
        return f.applyAsByte(value);
    }

    @Override
    public Object visit(ShortFunction<T> f) {
        return f.applyAsShort(value);
    }

    @Override
    public Object visit(IntFunction<T> f) {
        return f.applyAsInt(value);
    }

    @Override
    public Object visit(LongFunction<T> f) {
        return f.applyAsLong(value);
    }

    @Override
    public Object visit(FloatFunction<T> f) {
        return f.applyAsFloat(value);
    }

    @Override
    public Object visit(DoubleFunction<T> f) {
        return f.applyAsDouble(value);
    }
}

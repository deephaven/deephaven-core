/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.protobuf;

import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
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

enum BypassOnNull implements
        TypedFunction.Visitor<Object, ObjectFunction<Object, ?>>,
        PrimitiveFunction.Visitor<Object, ObjectFunction<Object, ?>> {
    INSTANCE;

    private static <T> TypedFunction.Visitor<T, ObjectFunction<T, ?>> visitor() {
        // noinspection unchecked,rawtypes
        return (TypedFunction.Visitor<T, ObjectFunction<T, ?>>) (TypedFunction.Visitor) INSTANCE;
    }

    private static <T> PrimitiveFunction.Visitor<T, ObjectFunction<T, ?>> primitiveVisitor() {
        // noinspection unchecked,rawtypes
        return (PrimitiveFunction.Visitor<T, ObjectFunction<T, ?>>) (PrimitiveFunction.Visitor) INSTANCE;
    }

    public static <T> ObjectFunction<T, ?> of(TypedFunction<T> x) {
        return x.walk(visitor());
    }

    public static <T, R> ObjectFunction<T, R> of(ObjectFunction<T, R> f) {
        return ObjectFunction.of(t -> t == null ? null : f.apply(t), f.returnType());
    }

    public static <T> ObjectFunction<T, ?> of(PrimitiveFunction<T> x) {
        return x.walk(primitiveVisitor());
    }

    public static <T> ObjectFunction<T, Boolean> of(BooleanFunction<T> f) {
        return ObjectFunction.of(x -> x == null ? null : f.test(x), BoxedBooleanType.of());
    }

    public static <T> ObjectFunction<T, Character> of(CharFunction<T> f) {
        return ObjectFunction.of(x -> x == null ? null : f.applyAsChar(x), BoxedCharType.of());
    }

    public static <T> ObjectFunction<T, Byte> of(ByteFunction<T> f) {
        return ObjectFunction.of(x -> x == null ? null : f.applyAsByte(x), BoxedByteType.of());
    }

    public static <T> ObjectFunction<T, Short> of(ShortFunction<T> f) {
        return ObjectFunction.of(x -> x == null ? null : f.applyAsShort(x), BoxedShortType.of());
    }

    public static <T> ObjectFunction<T, Integer> of(IntFunction<T> f) {
        return ObjectFunction.of(t -> t == null ? null : f.applyAsInt(t), BoxedIntType.of());
    }

    public static <T> ObjectFunction<T, Long> of(LongFunction<T> f) {
        return ObjectFunction.of(x -> x == null ? null : f.applyAsLong(x), BoxedLongType.of());
    }

    public static <T> ObjectFunction<T, Float> of(FloatFunction<T> f) {
        return ObjectFunction.of(x -> x == null ? null : f.applyAsFloat(x), BoxedFloatType.of());
    }

    public static <T> ObjectFunction<T, Double> of(DoubleFunction<T> f) {
        return ObjectFunction.of(x -> x == null ? null : f.applyAsDouble(x), BoxedDoubleType.of());
    }

    @Override
    public ObjectFunction<Object, ?> visit(ObjectFunction<Object, ?> f) {
        return of(f);
    }

    @Override
    public ObjectFunction<Object, ?> visit(PrimitiveFunction<Object> f) {
        return f.walk((PrimitiveFunction.Visitor<Object, ObjectFunction<Object, ?>>) this);
    }

    @Override
    public ObjectFunction<Object, Boolean> visit(BooleanFunction<Object> f) {
        return of(f);
    }

    @Override
    public ObjectFunction<Object, Character> visit(CharFunction<Object> f) {
        return of(f);
    }

    @Override
    public ObjectFunction<Object, Byte> visit(ByteFunction<Object> f) {
        return of(f);
    }

    @Override
    public ObjectFunction<Object, Short> visit(ShortFunction<Object> f) {
        return of(f);
    }

    @Override
    public ObjectFunction<Object, Integer> visit(IntFunction<Object> f) {
        return of(f);
    }

    @Override
    public ObjectFunction<Object, Long> visit(LongFunction<Object> f) {
        return of(f);
    }

    @Override
    public ObjectFunction<Object, Float> visit(FloatFunction<Object> f) {
        return of(f);
    }

    @Override
    public ObjectFunction<Object, Double> visit(DoubleFunction<Object> f) {
        return of(f);
    }
}

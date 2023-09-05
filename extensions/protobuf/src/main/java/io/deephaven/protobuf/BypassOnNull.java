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

enum BypassOnNull implements
        TypedFunction.Visitor<Object, ToObjectFunction<Object, ?>>,
        ToPrimitiveFunction.Visitor<Object, ToObjectFunction<Object, ?>> {
    INSTANCE;

    private static <T> TypedFunction.Visitor<T, ToObjectFunction<T, ?>> visitor() {
        // noinspection unchecked,rawtypes
        return (TypedFunction.Visitor<T, ToObjectFunction<T, ?>>) (TypedFunction.Visitor) INSTANCE;
    }

    private static <T> ToPrimitiveFunction.Visitor<T, ToObjectFunction<T, ?>> primitiveVisitor() {
        // noinspection unchecked,rawtypes
        return (ToPrimitiveFunction.Visitor<T, ToObjectFunction<T, ?>>) (ToPrimitiveFunction.Visitor) INSTANCE;
    }

    public static <T> ToObjectFunction<T, ?> of(TypedFunction<T> x) {
        return x.walk(visitor());
    }

    public static <T, R> ToObjectFunction<T, R> of(ToObjectFunction<T, R> f) {
        return ToObjectFunction.of(t -> t == null ? null : f.apply(t), f.returnType());
    }

    public static <T> ToObjectFunction<T, ?> of(ToPrimitiveFunction<T> x) {
        return x.walk(primitiveVisitor());
    }

    public static <T> ToObjectFunction<T, Boolean> of(ToBooleanFunction<T> f) {
        return ToObjectFunction.of(x -> x == null ? null : f.test(x), BoxedBooleanType.of());
    }

    public static <T> ToObjectFunction<T, Character> of(ToCharFunction<T> f) {
        return ToObjectFunction.of(x -> x == null ? null : f.applyAsChar(x), BoxedCharType.of());
    }

    public static <T> ToObjectFunction<T, Byte> of(ToByteFunction<T> f) {
        return ToObjectFunction.of(x -> x == null ? null : f.applyAsByte(x), BoxedByteType.of());
    }

    public static <T> ToObjectFunction<T, Short> of(ToShortFunction<T> f) {
        return ToObjectFunction.of(x -> x == null ? null : f.applyAsShort(x), BoxedShortType.of());
    }

    public static <T> ToObjectFunction<T, Integer> of(ToIntFunction<T> f) {
        return ToObjectFunction.of(t -> t == null ? null : f.applyAsInt(t), BoxedIntType.of());
    }

    public static <T> ToObjectFunction<T, Long> of(ToLongFunction<T> f) {
        return ToObjectFunction.of(x -> x == null ? null : f.applyAsLong(x), BoxedLongType.of());
    }

    public static <T> ToObjectFunction<T, Float> of(ToFloatFunction<T> f) {
        return ToObjectFunction.of(x -> x == null ? null : f.applyAsFloat(x), BoxedFloatType.of());
    }

    public static <T> ToObjectFunction<T, Double> of(ToDoubleFunction<T> f) {
        return ToObjectFunction.of(x -> x == null ? null : f.applyAsDouble(x), BoxedDoubleType.of());
    }

    @Override
    public ToObjectFunction<Object, ?> visit(ToObjectFunction<Object, ?> f) {
        return of(f);
    }

    @Override
    public ToObjectFunction<Object, ?> visit(ToPrimitiveFunction<Object> f) {
        return f.walk((ToPrimitiveFunction.Visitor<Object, ToObjectFunction<Object, ?>>) this);
    }

    @Override
    public ToObjectFunction<Object, Boolean> visit(ToBooleanFunction<Object> f) {
        return of(f);
    }

    @Override
    public ToObjectFunction<Object, Character> visit(ToCharFunction<Object> f) {
        return of(f);
    }

    @Override
    public ToObjectFunction<Object, Byte> visit(ToByteFunction<Object> f) {
        return of(f);
    }

    @Override
    public ToObjectFunction<Object, Short> visit(ToShortFunction<Object> f) {
        return of(f);
    }

    @Override
    public ToObjectFunction<Object, Integer> visit(ToIntFunction<Object> f) {
        return of(f);
    }

    @Override
    public ToObjectFunction<Object, Long> visit(ToLongFunction<Object> f) {
        return of(f);
    }

    @Override
    public ToObjectFunction<Object, Float> visit(ToFloatFunction<Object> f) {
        return of(f);
    }

    @Override
    public ToObjectFunction<Object, Double> visit(ToDoubleFunction<Object> f) {
        return of(f);
    }
}

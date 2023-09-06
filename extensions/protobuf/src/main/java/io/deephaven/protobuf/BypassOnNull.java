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
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.util.QueryConstants;

/**
 * Wraps existing functions to guard against null inputs; essentially, implementing
 * {@code x -> x == null ? QueryConstants.NULL_X : f.theApplyFunction(x)} for primitive functions (except for boolean
 * which goes to Boolean), and {@code x -> x == null ? null : f.apply(x)} for object functions.
 */
enum BypassOnNull implements
        TypedFunction.Visitor<Object, TypedFunction<Object>>,
        ToPrimitiveFunction.Visitor<Object, TypedFunction<Object>> {
    INSTANCE;

    private static <T> TypedFunction.Visitor<T, TypedFunction<T>> visitor() {
        // noinspection unchecked,rawtypes
        return (TypedFunction.Visitor<T, TypedFunction<T>>) (TypedFunction.Visitor) INSTANCE;
    }

    private static <T> ToPrimitiveFunction.Visitor<T, TypedFunction<T>> primitiveVisitor() {
        // noinspection unchecked,rawtypes
        return (ToPrimitiveFunction.Visitor<T, TypedFunction<T>>) (ToPrimitiveFunction.Visitor) INSTANCE;
    }

    public static <T> TypedFunction<T> of(TypedFunction<T> x) {
        return x.walk(visitor());
    }

    public static <T> TypedFunction<T> of(ToPrimitiveFunction<T> x) {
        return x.walk(primitiveVisitor());
    }

    public static <T, R> ToObjectFunction<T, R> of(ToObjectFunction<T, R> f) {
        return ToObjectFunction.of(x -> apply(f, x), f.returnType());
    }

    public static <T> ToObjectFunction<T, Boolean> of(ToBooleanFunction<T> f) {
        // Note: it's important that we don't adapt to byte at this level; we need to preserve the nominal boolean type
        // as long as we can (until it needs to get adapted into a chunk, that's where the transform to byte can happen)
        return ToObjectFunction.of(x -> applyAsBoolean(f, x), BoxedBooleanType.of());
    }

    public static <T> ToCharFunction<T> of(ToCharFunction<T> f) {
        return x -> applyAsChar(f, x);
    }

    public static <T> ToByteFunction<T> of(ToByteFunction<T> f) {
        return x -> applyAsByte(f, x);
    }

    public static <T> ToShortFunction<T> of(ToShortFunction<T> f) {
        return x -> applyAsShort(f, x);
    }

    public static <T> ToIntFunction<T> of(ToIntFunction<T> f) {
        return x -> applyAsInt(f, x);
    }

    public static <T> ToLongFunction<T> of(ToLongFunction<T> f) {
        return x -> applyAsLong(f, x);
    }

    public static <T> ToFloatFunction<T> of(ToFloatFunction<T> f) {
        return x -> applyAsFloat(f, x);
    }

    public static <T> ToDoubleFunction<T> of(ToDoubleFunction<T> f) {
        return x -> applyAsDouble(f, x);
    }

    @Override
    public ToObjectFunction<Object, ?> visit(ToObjectFunction<Object, ?> f) {
        return of(f);
    }

    @Override
    public TypedFunction<Object> visit(ToPrimitiveFunction<Object> f) {
        return f.walk((ToPrimitiveFunction.Visitor<Object, TypedFunction<Object>>) this);
    }

    @Override
    public ToObjectFunction<Object, Boolean> visit(ToBooleanFunction<Object> f) {
        return of(f);
    }

    @Override
    public ToCharFunction<Object> visit(ToCharFunction<Object> f) {
        return of(f);
    }

    @Override
    public ToByteFunction<Object> visit(ToByteFunction<Object> f) {
        return of(f);
    }

    @Override
    public ToShortFunction<Object> visit(ToShortFunction<Object> f) {
        return of(f);
    }

    @Override
    public ToIntFunction<Object> visit(ToIntFunction<Object> f) {
        return of(f);
    }

    @Override
    public ToLongFunction<Object> visit(ToLongFunction<Object> f) {
        return of(f);
    }

    @Override
    public ToFloatFunction<Object> visit(ToFloatFunction<Object> f) {
        return of(f);
    }

    @Override
    public ToDoubleFunction<Object> visit(ToDoubleFunction<Object> f) {
        return of(f);
    }

    private static <T, R> R apply(ToObjectFunction<T, R> f, T x) {
        return x == null ? null : f.apply(x);
    }

    private static <T> Boolean applyAsBoolean(ToBooleanFunction<T> f, T x) {
        return x == null ? null : f.test(x);
    }

    private static <T> char applyAsChar(ToCharFunction<T> f, T x) {
        return x == null ? QueryConstants.NULL_CHAR : f.applyAsChar(x);
    }

    private static <T> byte applyAsByte(ToByteFunction<T> f, T x) {
        return x == null ? QueryConstants.NULL_BYTE : f.applyAsByte(x);
    }

    private static <T> short applyAsShort(ToShortFunction<T> f, T x) {
        return x == null ? QueryConstants.NULL_SHORT : f.applyAsShort(x);
    }

    private static <T> int applyAsInt(ToIntFunction<T> f, T x) {
        return x == null ? QueryConstants.NULL_INT : f.applyAsInt(x);
    }

    private static <T> long applyAsLong(ToLongFunction<T> f, T x) {
        return x == null ? QueryConstants.NULL_LONG : f.applyAsLong(x);
    }

    private static <T> float applyAsFloat(ToFloatFunction<T> f, T x) {
        return x == null ? QueryConstants.NULL_FLOAT : f.applyAsFloat(x);
    }

    private static <T> double applyAsDouble(ToDoubleFunction<T> f, T x) {
        return x == null ? QueryConstants.NULL_DOUBLE : f.applyAsDouble(x);
    }
}

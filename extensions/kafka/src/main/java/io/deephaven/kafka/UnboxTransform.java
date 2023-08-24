/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

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
import io.deephaven.qst.type.ArrayType;
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
import io.deephaven.qst.type.BoxedType;
import io.deephaven.qst.type.CustomType;
import io.deephaven.qst.type.GenericType;
import io.deephaven.qst.type.InstantType;
import io.deephaven.qst.type.StringType;
import io.deephaven.util.type.TypeUtils;

import java.util.Objects;
import java.util.Optional;

class UnboxTransform {

    private static final ByteFunction<Byte> UNBOX_BYTE = TypeUtils::unbox;
    private static final CharFunction<Character> UNBOX_CHAR = TypeUtils::unbox;
    private static final ShortFunction<Short> UNBOX_SHORT = TypeUtils::unbox;
    private static final IntFunction<Integer> UNBOX_INT = TypeUtils::unbox;
    private static final LongFunction<Long> UNBOX_LONG = TypeUtils::unbox;
    private static final FloatFunction<Float> UNBOX_FLOAT = TypeUtils::unbox;
    private static final DoubleFunction<Double> UNBOX_DOULE = TypeUtils::unbox;

    /**
     * Returns the Deephaven unboxed equivalent of {@code f}. Relevant for all {@link BoxedType boxed types} except the
     * {@link BoxedBooleanType boxed Boolean type}. All other functions will be return unchanged.
     *
     * @param f the function
     * @return the unboxed equivalent
     * @param <T> the input type
     * @see #unboxByte(ObjectFunction)
     * @see #unboxChar(ObjectFunction)
     * @see #unboxDouble(ObjectFunction)
     * @see #unboxFloat(ObjectFunction)
     * @see #unboxInt(ObjectFunction)
     * @see #unboxLong(ObjectFunction)
     * @see #unboxShort(ObjectFunction)
     */
    public static <T> Optional<PrimitiveFunction<T>> of(TypedFunction<T> f) {
        return UnboxFunctionVisitor.of(f);
    }

    /**
     * Returns the Deephaven unboxed equivalent of {@code f}. Relevant for all {@link BoxedType boxed types} except the
     * {@link BoxedBooleanType boxed Boolean type}. All other functions will be return unchanged.
     *
     * @param f the object function
     * @return the unboxed equivalent
     * @param <T> the input type
     * @see #unboxByte(ObjectFunction)
     * @see #unboxChar(ObjectFunction)
     * @see #unboxDouble(ObjectFunction)
     * @see #unboxFloat(ObjectFunction)
     * @see #unboxInt(ObjectFunction)
     * @see #unboxLong(ObjectFunction)
     * @see #unboxShort(ObjectFunction)
     */
    public static <T> Optional<PrimitiveFunction<T>> of(ObjectFunction<T, ?> f) {
        return UnboxObjectFunctionVisitor.of(f);
    }

    /**
     * Equivalent to {@code f.mapByte(TypeUtils::unbox)}.
     *
     * @param f the Byte function
     * @return the byte function
     * @param <T> the input type
     * @see TypeUtils#unbox(Byte)
     */
    public static <T> ByteFunction<T> unboxByte(ObjectFunction<T, Byte> f) {
        return f.mapByte(UNBOX_BYTE);
    }

    /**
     * Equivalent to {@code f.mapChar(TypeUtils::unbox)}.
     *
     * @param f the Character function
     * @return the char function
     * @param <T> the input type
     * @see TypeUtils#unbox(Character)
     */
    public static <T> CharFunction<T> unboxChar(ObjectFunction<T, Character> f) {
        return f.mapChar(UNBOX_CHAR);
    }

    /**
     * Equivalent to {@code f.mapShort(TypeUtils::unbox)}.
     *
     * @param f the Short function
     * @return the short function
     * @param <T> the input type
     * @see TypeUtils#unbox(Short)
     */
    public static <T> ShortFunction<T> unboxShort(ObjectFunction<T, Short> f) {
        return f.mapShort(UNBOX_SHORT);
    }

    /**
     * Equivalent to {@code f.mapInt(TypeUtils::unbox)}.
     *
     * @param f the Integer function
     * @return the int function
     * @param <T> the input type
     * @see TypeUtils#unbox(Integer)
     */
    public static <T> IntFunction<T> unboxInt(ObjectFunction<T, Integer> f) {
        return f.mapInt(UNBOX_INT);
    }

    /**
     * Equivalent to {@code f.mapLong(TypeUtils::unbox)}.
     *
     * @param f the Long function
     * @return the long function
     * @param <T> the input type
     * @see TypeUtils#unbox(Long)
     */
    public static <T> LongFunction<T> unboxLong(ObjectFunction<T, Long> f) {
        return f.mapLong(UNBOX_LONG);
    }

    /**
     * Equivalent to {@code f.mapFloat(TypeUtils::unbox)}.
     *
     * @param f the Float function
     * @return the float function
     * @param <T> the input type
     * @see TypeUtils#unbox(Float)
     */
    public static <T> FloatFunction<T> unboxFloat(ObjectFunction<T, Float> f) {
        return f.mapFloat(UNBOX_FLOAT);
    }

    /**
     * Equivalent to {@code f.mapDouble(TypeUtils::unbox)}.
     *
     * @param f the Double function
     * @return the double function
     * @param <T> the input type
     * @see TypeUtils#unbox(Double)
     */
    public static <T> DoubleFunction<T> unboxDouble(ObjectFunction<T, Double> f) {
        return f.mapDouble(UNBOX_DOULE);
    }

    private enum UnboxFunctionVisitor implements TypedFunction.Visitor<Object, PrimitiveFunction<Object>> {
        INSTANCE;

        public static <T> Optional<PrimitiveFunction<T>> of(TypedFunction<T> f) {
            // noinspection unchecked
            return Optional.ofNullable(
                    f.walk((TypedFunction.Visitor<T, PrimitiveFunction<T>>) (TypedFunction.Visitor<?, ?>) INSTANCE));
        }

        @Override
        public PrimitiveFunction<Object> visit(PrimitiveFunction<Object> f) {
            return f;
        }

        @Override
        public PrimitiveFunction<Object> visit(ObjectFunction<Object, ?> f) {
            return UnboxTransform.of(f).orElse(null);
        }
    }

    private static class UnboxObjectFunctionVisitor<T>
            implements GenericType.Visitor<PrimitiveFunction<T>>, BoxedType.Visitor<PrimitiveFunction<T>> {

        public static <T> Optional<PrimitiveFunction<T>> of(ObjectFunction<T, ?> f) {
            return Optional.ofNullable(f.returnType().walk(new UnboxObjectFunctionVisitor<>(f)));
        }

        private final ObjectFunction<T, ?> f;

        public UnboxObjectFunctionVisitor(ObjectFunction<T, ?> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public PrimitiveFunction<T> visit(BoxedType<?> boxedType) {
            return boxedType.walk((BoxedType.Visitor<PrimitiveFunction<T>>) this);
        }

        @Override
        public PrimitiveFunction<T> visit(StringType stringType) {
            return null;
        }

        @Override
        public PrimitiveFunction<T> visit(InstantType instantType) {
            return null;
        }

        @Override
        public PrimitiveFunction<T> visit(ArrayType<?, ?> arrayType) {
            return null;
        }

        @Override
        public PrimitiveFunction<T> visit(CustomType<?> customType) {
            return null;
        }

        @Override
        public PrimitiveFunction<T> visit(BoxedBooleanType booleanType) {
            // We don't have an "unboxed boolean".
            // We _can_ transform it to a byte, but that's a separate operation.
            return null;
        }

        @Override
        public PrimitiveFunction<T> visit(BoxedByteType byteType) {
            return unboxByte(f.cast(byteType));
        }

        @Override
        public PrimitiveFunction<T> visit(BoxedCharType charType) {
            return unboxChar(f.cast(charType));
        }

        @Override
        public PrimitiveFunction<T> visit(BoxedShortType shortType) {
            return unboxShort(f.cast(shortType));
        }

        @Override
        public PrimitiveFunction<T> visit(BoxedIntType intType) {
            return unboxInt(f.cast(intType));
        }

        @Override
        public PrimitiveFunction<T> visit(BoxedLongType longType) {
            return unboxLong(f.cast(longType));
        }

        @Override
        public PrimitiveFunction<T> visit(BoxedFloatType floatType) {
            return unboxFloat(f.cast(floatType));
        }

        @Override
        public PrimitiveFunction<T> visit(BoxedDoubleType doubleType) {
            return unboxDouble(f.cast(doubleType));
        }
    }
}

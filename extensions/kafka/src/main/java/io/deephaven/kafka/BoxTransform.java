/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

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
import io.deephaven.qst.type.BoxedBooleanType;
import io.deephaven.qst.type.BoxedByteType;
import io.deephaven.qst.type.BoxedCharType;
import io.deephaven.qst.type.BoxedDoubleType;
import io.deephaven.qst.type.BoxedFloatType;
import io.deephaven.qst.type.BoxedIntType;
import io.deephaven.qst.type.BoxedLongType;
import io.deephaven.qst.type.BoxedShortType;
import io.deephaven.util.type.TypeUtils;

class BoxTransform {

    /**
     * Creates the function composition {@code box ∘ f}.
     *
     * <p>
     * For primitive functions {@code f}, see {@link #of(PrimitiveFunction)}.
     *
     * <p>
     * For object functions {@code f}, {@code box} is the identity function (and {@code f} will simply be returned).
     *
     * @param f the function
     * @return the object function
     * @param <T> the input type
     * @see #of(BooleanFunction)
     * @see #of(CharFunction)
     * @see #of(ByteFunction)
     * @see #of(ShortFunction)
     * @see #of(IntFunction)
     * @see #of(LongFunction)
     * @see #of(FloatFunction)
     * @see #of(DoubleFunction)
     */
    public static <T> ObjectFunction<T, ?> of(TypedFunction<T> f) {
        return BoxedVisitor.of(f);
    }

    /**
     * Creates the function composition {@code box ∘ f}.
     *
     * <p>
     * The returned object function will have return type {@code f.returnType().boxedType()}.
     *
     * @param f the primitive function
     * @return the object function
     * @param <T> the input type
     * @see #of(BooleanFunction)
     * @see #of(CharFunction)
     * @see #of(ByteFunction)
     * @see #of(ShortFunction)
     * @see #of(IntFunction)
     * @see #of(LongFunction)
     * @see #of(FloatFunction)
     * @see #of(DoubleFunction)
     */
    public static <T> ObjectFunction<T, ?> of(PrimitiveFunction<T> f) {
        return BoxedVisitor.of(f);
    }

    /**
     * Creates the function composition {@code box ∘ f}, where {@code box} is simply a cast from {@code boolean} to
     * {@code Boolean}.
     *
     * <p>
     * Equivalent to {@code x -> (Boolean)f.test(x)}.
     *
     * @param f the boolean function
     * @return the object function
     * @param <T> the input type
     */
    public static <T> ObjectFunction<T, Boolean> of(BooleanFunction<T> f) {
        return ObjectFunction.of(f::test, BoxedBooleanType.of());
    }

    /**
     * Creates the function composition {@code box ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> TypeUtils.box(f.applyAsChar(x))}.
     *
     * @param f the char function
     * @return the object function
     * @param <T> the input type
     * @see TypeUtils#box(char)
     */
    public static <T> ObjectFunction<T, Character> of(CharFunction<T> f) {
        return ObjectFunction.of(t -> box(f, t), BoxedCharType.of());
    }

    /**
     * Creates the function composition {@code box ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> TypeUtils.box(f.applyAsByte(x))}.
     *
     * @param f the byte function
     * @return the object function
     * @param <T> the input type
     * @see TypeUtils#box(byte)
     */
    public static <T> ObjectFunction<T, Byte> of(ByteFunction<T> f) {
        return ObjectFunction.of(t -> box(f, t), BoxedByteType.of());
    }

    /**
     * Creates the function composition {@code box ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> TypeUtils.box(f.applyAsShort(x))}.
     *
     * @param f the short function
     * @return the object function
     * @param <T> the input type
     * @see TypeUtils#box(short)
     */
    public static <T> ObjectFunction<T, Short> of(ShortFunction<T> f) {
        return ObjectFunction.of(t -> box(f, t), BoxedShortType.of());
    }

    /**
     * Creates the function composition {@code box ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> TypeUtils.box(f.applyAsInt(x))}.
     *
     * @param f the int function
     * @return the object function
     * @param <T> the input type
     * @see TypeUtils#box(int)
     */
    public static <T> ObjectFunction<T, Integer> of(IntFunction<T> f) {
        return ObjectFunction.of(t -> box(f, t), BoxedIntType.of());
    }

    /**
     * Creates the function composition {@code box ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> TypeUtils.box(f.applyAsLong(x))}.
     *
     * @param f the long function
     * @return the object function
     * @param <T> the input type
     * @see TypeUtils#box(long)
     */
    public static <T> ObjectFunction<T, Long> of(LongFunction<T> f) {
        return ObjectFunction.of(t -> box(f, t), BoxedLongType.of());
    }

    /**
     * Creates the function composition {@code box ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> TypeUtils.box(f.applyAsFloat(x))}.
     *
     * @param f the float function
     * @return the object function
     * @param <T> the input type
     * @see TypeUtils#box(float)
     */
    public static <T> ObjectFunction<T, Float> of(FloatFunction<T> f) {
        return ObjectFunction.of(t -> box(f, t), BoxedFloatType.of());
    }

    /**
     * Creates the function composition {@code box ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> TypeUtils.box(f.applyAsDouble(x))}.
     *
     * @param f the double function
     * @return the object function
     * @param <T> the input type
     * @see TypeUtils#box(double)
     */
    public static <T> ObjectFunction<T, Double> of(DoubleFunction<T> f) {
        return ObjectFunction.of(t -> box(f, t), BoxedDoubleType.of());
    }

    private enum BoxedVisitor implements TypedFunction.Visitor<Object, ObjectFunction<Object, ?>>,
            PrimitiveFunction.Visitor<Object, ObjectFunction<Object, ?>> {
        INSTANCE;

        public static <T> ObjectFunction<T, ?> of(TypedFunction<T> f) {
            // noinspection unchecked
            return f.walk((TypedFunction.Visitor<T, ObjectFunction<T, ?>>) (TypedFunction.Visitor<?, ?>) INSTANCE);
        }

        public static <T> ObjectFunction<T, ?> of(PrimitiveFunction<T> f) {
            // noinspection unchecked
            return f.walk(
                    (PrimitiveFunction.Visitor<T, ObjectFunction<T, ?>>) (PrimitiveFunction.Visitor<?, ?>) INSTANCE);
        }

        @Override
        public ObjectFunction<Object, ?> visit(PrimitiveFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(ObjectFunction<Object, ?> f) {
            return f;
        }

        @Override
        public ObjectFunction<Object, ?> visit(BooleanFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(CharFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(ByteFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(ShortFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(IntFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(LongFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(FloatFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ObjectFunction<Object, ?> visit(DoubleFunction<Object> f) {
            return BoxTransform.of(f);
        }
    }

    private static <T> Character box(CharFunction<T> f, T x) {
        return TypeUtils.box(f.applyAsChar(x));
    }

    private static <T> Byte box(ByteFunction<T> f, T x) {
        return TypeUtils.box(f.applyAsByte(x));
    }

    private static <T> Short box(ShortFunction<T> f, T x) {
        return TypeUtils.box(f.applyAsShort(x));
    }

    private static <T> Integer box(IntFunction<T> f, T x) {
        return TypeUtils.box(f.applyAsInt(x));
    }

    private static <T> Long box(LongFunction<T> f, T x) {
        return TypeUtils.box(f.applyAsLong(x));
    }

    private static <T> Float box(FloatFunction<T> f, T x) {
        return TypeUtils.box(f.applyAsFloat(x));
    }

    private static <T> Double box(DoubleFunction<T> f, T x) {
        return TypeUtils.box(f.applyAsDouble(x));
    }
}

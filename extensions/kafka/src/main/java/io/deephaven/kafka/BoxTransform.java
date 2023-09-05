/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.kafka;

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
     * For primitive functions {@code f}, see {@link #of(ToPrimitiveFunction)}.
     *
     * <p>
     * For object functions {@code f}, {@code box} is the identity function (and {@code f} will simply be returned).
     *
     * @param f the function
     * @return the object function
     * @param <T> the input type
     * @see #of(ToBooleanFunction)
     * @see #of(ToCharFunction)
     * @see #of(ToByteFunction)
     * @see #of(ToShortFunction)
     * @see #of(ToIntFunction)
     * @see #of(ToLongFunction)
     * @see #of(ToFloatFunction)
     * @see #of(ToDoubleFunction)
     */
    public static <T> ToObjectFunction<T, ?> of(TypedFunction<T> f) {
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
     * @see #of(ToBooleanFunction)
     * @see #of(ToCharFunction)
     * @see #of(ToByteFunction)
     * @see #of(ToShortFunction)
     * @see #of(ToIntFunction)
     * @see #of(ToLongFunction)
     * @see #of(ToFloatFunction)
     * @see #of(ToDoubleFunction)
     */
    public static <T> ToObjectFunction<T, ?> of(ToPrimitiveFunction<T> f) {
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
    public static <T> ToObjectFunction<T, Boolean> of(ToBooleanFunction<T> f) {
        return ToObjectFunction.of(f::test, BoxedBooleanType.of());
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
    public static <T> ToObjectFunction<T, Character> of(ToCharFunction<T> f) {
        return ToObjectFunction.of(t -> box(f, t), BoxedCharType.of());
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
    public static <T> ToObjectFunction<T, Byte> of(ToByteFunction<T> f) {
        return ToObjectFunction.of(t -> box(f, t), BoxedByteType.of());
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
    public static <T> ToObjectFunction<T, Short> of(ToShortFunction<T> f) {
        return ToObjectFunction.of(t -> box(f, t), BoxedShortType.of());
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
    public static <T> ToObjectFunction<T, Integer> of(ToIntFunction<T> f) {
        return ToObjectFunction.of(t -> box(f, t), BoxedIntType.of());
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
    public static <T> ToObjectFunction<T, Long> of(ToLongFunction<T> f) {
        return ToObjectFunction.of(t -> box(f, t), BoxedLongType.of());
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
    public static <T> ToObjectFunction<T, Float> of(ToFloatFunction<T> f) {
        return ToObjectFunction.of(t -> box(f, t), BoxedFloatType.of());
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
    public static <T> ToObjectFunction<T, Double> of(ToDoubleFunction<T> f) {
        return ToObjectFunction.of(t -> box(f, t), BoxedDoubleType.of());
    }

    private enum BoxedVisitor implements TypedFunction.Visitor<Object, ToObjectFunction<Object, ?>>,
            ToPrimitiveFunction.Visitor<Object, ToObjectFunction<Object, ?>> {
        INSTANCE;

        public static <T> ToObjectFunction<T, ?> of(TypedFunction<T> f) {
            // noinspection unchecked
            return f.walk((TypedFunction.Visitor<T, ToObjectFunction<T, ?>>) (TypedFunction.Visitor<?, ?>) INSTANCE);
        }

        public static <T> ToObjectFunction<T, ?> of(ToPrimitiveFunction<T> f) {
            // noinspection unchecked
            return f.walk(
                    (ToPrimitiveFunction.Visitor<T, ToObjectFunction<T, ?>>) (ToPrimitiveFunction.Visitor<?, ?>) INSTANCE);
        }

        @Override
        public ToObjectFunction<Object, ?> visit(ToPrimitiveFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ToObjectFunction<Object, ?> visit(ToObjectFunction<Object, ?> f) {
            return f;
        }

        @Override
        public ToObjectFunction<Object, Boolean> visit(ToBooleanFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ToObjectFunction<Object, Character> visit(ToCharFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ToObjectFunction<Object, Byte> visit(ToByteFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ToObjectFunction<Object, Short> visit(ToShortFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ToObjectFunction<Object, Integer> visit(ToIntFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ToObjectFunction<Object, Long> visit(ToLongFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ToObjectFunction<Object, Float> visit(ToFloatFunction<Object> f) {
            return BoxTransform.of(f);
        }

        @Override
        public ToObjectFunction<Object, Double> visit(ToDoubleFunction<Object> f) {
            return BoxTransform.of(f);
        }
    }

    private static <T> Character box(ToCharFunction<T> f, T x) {
        return TypeUtils.box(f.applyAsChar(x));
    }

    private static <T> Byte box(ToByteFunction<T> f, T x) {
        return TypeUtils.box(f.applyAsByte(x));
    }

    private static <T> Short box(ToShortFunction<T> f, T x) {
        return TypeUtils.box(f.applyAsShort(x));
    }

    private static <T> Integer box(ToIntFunction<T> f, T x) {
        return TypeUtils.box(f.applyAsInt(x));
    }

    private static <T> Long box(ToLongFunction<T> f, T x) {
        return TypeUtils.box(f.applyAsLong(x));
    }

    private static <T> Float box(ToFloatFunction<T> f, T x) {
        return TypeUtils.box(f.applyAsFloat(x));
    }

    private static <T> Double box(ToDoubleFunction<T> f, T x) {
        return TypeUtils.box(f.applyAsDouble(x));
    }
}

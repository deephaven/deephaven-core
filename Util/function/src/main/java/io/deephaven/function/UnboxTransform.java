//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

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

import java.util.Objects;
import java.util.Optional;

import static io.deephaven.util.QueryConstants.*;

public class UnboxTransform {

    private static final ToByteFunction<Byte> UNBOX_BYTE = TypeUtils::unbox;
    private static final ToCharFunction<Character> UNBOX_CHAR = TypeUtils::unbox;
    private static final ToShortFunction<Short> UNBOX_SHORT = TypeUtils::unbox;
    private static final ToIntFunction<Integer> UNBOX_INT = TypeUtils::unbox;
    private static final ToLongFunction<Long> UNBOX_LONG = TypeUtils::unbox;
    private static final ToFloatFunction<Float> UNBOX_FLOAT = TypeUtils::unbox;
    private static final ToDoubleFunction<Double> UNBOX_DOUBLE = TypeUtils::unbox;

    /**
     * Returns the Deephaven unboxed equivalent of {@code f}. Relevant for all {@link BoxedType boxed types} except the
     * {@link BoxedBooleanType boxed Boolean type}. All other functions will be return unchanged.
     *
     * @param f the function
     * @return the unboxed equivalent
     * @param <T> the input type
     * @see #unboxByte(ToObjectFunction)
     * @see #unboxChar(ToObjectFunction)
     * @see #unboxDouble(ToObjectFunction)
     * @see #unboxFloat(ToObjectFunction)
     * @see #unboxInt(ToObjectFunction)
     * @see #unboxLong(ToObjectFunction)
     * @see #unboxShort(ToObjectFunction)
     */
    public static <T> Optional<ToPrimitiveFunction<T>> of(TypedFunction<T> f) {
        return UnboxFunctionVisitor.of(f);
    }

    /**
     * Returns the Deephaven unboxed equivalent of {@code f}. Relevant for all {@link BoxedType boxed types} except the
     * {@link BoxedBooleanType boxed Boolean type}. All other functions will be return unchanged.
     *
     * @param f the object function
     * @return the unboxed equivalent
     * @param <T> the input type
     * @see #unboxByte(ToObjectFunction)
     * @see #unboxChar(ToObjectFunction)
     * @see #unboxDouble(ToObjectFunction)
     * @see #unboxFloat(ToObjectFunction)
     * @see #unboxInt(ToObjectFunction)
     * @see #unboxLong(ToObjectFunction)
     * @see #unboxShort(ToObjectFunction)
     */
    public static <T> Optional<ToPrimitiveFunction<T>> of(ToObjectFunction<T, ?> f) {
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
    public static <T> ToByteFunction<T> unboxByte(ToObjectFunction<T, Byte> f) {
        return f.mapToByte(UNBOX_BYTE);
    }

    /**
     * Equivalent to {@code f.mapChar(TypeUtils::unbox)}.
     *
     * @param f the Character function
     * @return the char function
     * @param <T> the input type
     * @see TypeUtils#unbox(Character)
     */
    public static <T> ToCharFunction<T> unboxChar(ToObjectFunction<T, Character> f) {
        return f.mapToChar(UNBOX_CHAR);
    }

    /**
     * Equivalent to {@code f.mapShort(TypeUtils::unbox)}.
     *
     * @param f the Short function
     * @return the short function
     * @param <T> the input type
     * @see TypeUtils#unbox(Short)
     */
    public static <T> ToShortFunction<T> unboxShort(ToObjectFunction<T, Short> f) {
        return f.mapToShort(UNBOX_SHORT);
    }

    /**
     * Equivalent to {@code f.mapInt(TypeUtils::unbox)}.
     *
     * @param f the Integer function
     * @return the int function
     * @param <T> the input type
     * @see TypeUtils#unbox(Integer)
     */
    public static <T> ToIntFunction<T> unboxInt(ToObjectFunction<T, Integer> f) {
        return f.mapToInt(UNBOX_INT);
    }

    /**
     * Equivalent to {@code f.mapLong(TypeUtils::unbox)}.
     *
     * @param f the Long function
     * @return the long function
     * @param <T> the input type
     * @see TypeUtils#unbox(Long)
     */
    public static <T> ToLongFunction<T> unboxLong(ToObjectFunction<T, Long> f) {
        return f.mapToLong(UNBOX_LONG);
    }

    /**
     * Equivalent to {@code f.mapFloat(TypeUtils::unbox)}.
     *
     * @param f the Float function
     * @return the float function
     * @param <T> the input type
     * @see TypeUtils#unbox(Float)
     */
    public static <T> ToFloatFunction<T> unboxFloat(ToObjectFunction<T, Float> f) {
        return f.mapToFloat(UNBOX_FLOAT);
    }

    /**
     * Equivalent to {@code f.mapDouble(TypeUtils::unbox)}.
     *
     * @param f the Double function
     * @return the double function
     * @param <T> the input type
     * @see TypeUtils#unbox(Double)
     */
    public static <T> ToDoubleFunction<T> unboxDouble(ToObjectFunction<T, Double> f) {
        return f.mapToDouble(UNBOX_DOUBLE);
    }

    private enum UnboxFunctionVisitor implements TypedFunction.Visitor<Object, ToPrimitiveFunction<Object>> {
        INSTANCE;

        public static <T> Optional<ToPrimitiveFunction<T>> of(TypedFunction<T> f) {
            // noinspection unchecked
            return Optional.ofNullable(
                    f.walk((TypedFunction.Visitor<T, ToPrimitiveFunction<T>>) (TypedFunction.Visitor<?, ?>) INSTANCE));
        }

        @Override
        public ToPrimitiveFunction<Object> visit(ToPrimitiveFunction<Object> f) {
            return f;
        }

        @Override
        public ToPrimitiveFunction<Object> visit(ToObjectFunction<Object, ?> f) {
            return UnboxTransform.of(f).orElse(null);
        }
    }

    private static class UnboxObjectFunctionVisitor<T>
            implements GenericType.Visitor<ToPrimitiveFunction<T>>, BoxedType.Visitor<ToPrimitiveFunction<T>> {

        public static <T> Optional<ToPrimitiveFunction<T>> of(ToObjectFunction<T, ?> f) {
            return Optional.ofNullable(f.returnType().walk(new UnboxObjectFunctionVisitor<>(f)));
        }

        private final ToObjectFunction<T, ?> f;

        public UnboxObjectFunctionVisitor(ToObjectFunction<T, ?> f) {
            this.f = Objects.requireNonNull(f);
        }

        @Override
        public ToPrimitiveFunction<T> visit(BoxedType<?> boxedType) {
            return boxedType.walk((BoxedType.Visitor<ToPrimitiveFunction<T>>) this);
        }

        @Override
        public ToPrimitiveFunction<T> visit(StringType stringType) {
            return null;
        }

        @Override
        public ToPrimitiveFunction<T> visit(InstantType instantType) {
            return null;
        }

        @Override
        public ToPrimitiveFunction<T> visit(ArrayType<?, ?> arrayType) {
            return null;
        }

        @Override
        public ToPrimitiveFunction<T> visit(CustomType<?> customType) {
            return null;
        }

        @Override
        public ToPrimitiveFunction<T> visit(BoxedBooleanType booleanType) {
            // We don't have an "unboxed boolean".
            // We _can_ transform it to a byte, but that's a separate operation.
            return null;
        }

        @Override
        public ToPrimitiveFunction<T> visit(BoxedByteType byteType) {
            return unboxByte(f.cast(byteType));
        }

        @Override
        public ToPrimitiveFunction<T> visit(BoxedCharType charType) {
            return unboxChar(f.cast(charType));
        }

        @Override
        public ToPrimitiveFunction<T> visit(BoxedShortType shortType) {
            return unboxShort(f.cast(shortType));
        }

        @Override
        public ToPrimitiveFunction<T> visit(BoxedIntType intType) {
            return unboxInt(f.cast(intType));
        }

        @Override
        public ToPrimitiveFunction<T> visit(BoxedLongType longType) {
            return unboxLong(f.cast(longType));
        }

        @Override
        public ToPrimitiveFunction<T> visit(BoxedFloatType floatType) {
            return unboxFloat(f.cast(floatType));
        }

        @Override
        public ToPrimitiveFunction<T> visit(BoxedDoubleType doubleType) {
            return unboxDouble(f.cast(doubleType));
        }
    }

    // TODO: Clean up dependencies to be able to use io.deephaven.util.type.TypeUtils.
    private static class TypeUtils {
        public static byte unbox(Byte value) {
            return (value == null ? NULL_BYTE : value);
        }

        public static char unbox(Character value) {
            return (value == null ? NULL_CHAR : value);
        }

        public static double unbox(Double value) {
            return (value == null ? NULL_DOUBLE : value);
        }

        public static float unbox(Float value) {
            return (value == null ? NULL_FLOAT : value);
        }

        public static int unbox(Integer value) {
            return (value == null ? NULL_INT : value);
        }

        public static long unbox(Long value) {
            return (value == null ? NULL_LONG : value);
        }

        public static short unbox(Short value) {
            return (value == null ? NULL_SHORT : value);
        }

    }
}

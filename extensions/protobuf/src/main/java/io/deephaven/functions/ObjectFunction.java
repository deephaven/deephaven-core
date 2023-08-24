/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import io.deephaven.qst.type.GenericType;

import java.util.function.Function;

/**
 * An object function.
 *
 * <p>
 * Note: this is not a {@link FunctionalInterface}, as {@link #returnType()} must be provided.
 *
 * @param <T> the input type
 * @param <R> the return type
 */
public interface ObjectFunction<T, R> extends TypedFunction<T>, Function<T, R> {

    /**
     * Creates an object function from {@code f} and {@code returnType}.
     *
     * @param f the function
     * @param returnType the return type
     * @return the object function
     * @param <T> the input type
     * @param <R> the return type
     */
    static <T, R> ObjectFunction<T, R> of(Function<T, R> f, GenericType<R> returnType) {
        return ObjectFunctions.of(f, returnType);
    }

    /**
     * Creates an object function that casts each input to {@link Object}. Equivalent to {@code x -> (Object)x}.
     *
     * @return the object function
     * @param <T> the input type
     */
    static <T> ObjectFunction<T, Object> identity() {
        return ObjectFunctions.identity();
    }

    /**
     * Creates an object function that casts each input to {@code returnType}. Equivalent to {@code x -> (R)x}.
     *
     * @param returnType the return type
     * @return the object function
     * @param <T> the input type
     * @param <R> the return type
     */
    static <T, R> ObjectFunction<T, R> identity(GenericType<R> returnType) {
        return ObjectFunctions.<T>identity().cast(returnType);
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.apply(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the object function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R, Z> ObjectFunction<T, Z> map(Function<T, R> f, ObjectFunction<R, Z> g) {
        return ObjectFunctions.map(f, g);
    }

    GenericType<R> returnType();

    R apply(T value);

    @Override
    default ObjectFunction<T, R> mapInput(Function<T, T> f) {
        return map(f, this);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.test(this.apply(x))}.
     *
     * @param g the outer function
     * @return the boolean function
     */
    default BooleanFunction<T> mapBoolean(BooleanFunction<R> g) {
        return BooleanFunction.map(this, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsChar(this.apply(x))}.
     *
     * @param g the outer function
     * @return the char function
     */
    default CharFunction<T> mapChar(CharFunction<R> g) {
        return CharFunction.map(this, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsByte(this.apply(x))}.
     *
     * @param g the outer function
     * @return the byte function
     */
    default ByteFunction<T> mapByte(ByteFunction<R> g) {
        return ByteFunction.map(this, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsShort(this.apply(x))}.
     *
     * @param g the outer function
     * @return the short function
     */
    default ShortFunction<T> mapShort(ShortFunction<R> g) {
        return ShortFunction.map(this, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsInt(this.apply(x))}.
     *
     * @param g the outer function
     * @return the int function
     */
    default IntFunction<T> mapInt(IntFunction<R> g) {
        return IntFunction.map(this, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsLong(this.apply(x))}.
     *
     * @param g the outer function
     * @return the long function
     */
    default LongFunction<T> mapLong(LongFunction<R> g) {
        return LongFunction.map(this, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsFloat(this.apply(x))}.
     *
     * @param g the outer function
     * @return the float function
     */
    default FloatFunction<T> mapFloat(FloatFunction<R> g) {
        return FloatFunction.map(this, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.applyAsDouble(this.apply(x))}.
     *
     * @param g the outer function
     * @return the double function
     */
    default DoubleFunction<T> mapDouble(DoubleFunction<R> g) {
        return DoubleFunction.map(this, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.apply(this.apply(x))}.
     *
     * @param g the outer function
     * @return the object function
     */
    default <R2> ObjectFunction<T, R2> mapObj(ObjectFunction<R, R2> g) {
        return map(this, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.apply(this.apply(x))}.
     *
     * @param g the outer function
     * @return the object function
     */
    default <R2> ObjectFunction<T, R2> mapObj(Function<R, R2> g, GenericType<R2> returnType) {
        return map(this, of(g, returnType));
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Prefer to call one of the more strongly-typed map methods if you have a more specific function type.
     *
     * @param g the outer function
     * @return the function
     * @see #mapBoolean(BooleanFunction)
     * @see #mapChar(CharFunction)
     * @see #mapByte(ByteFunction)
     * @see #mapShort(ShortFunction)
     * @see #mapInt(IntFunction)
     * @see #mapLong(LongFunction)
     * @see #mapFloat(FloatFunction)
     * @see #mapDouble(DoubleFunction)
     */
    default PrimitiveFunction<T> mapPrimitive(PrimitiveFunction<R> g) {
        return ObjectFunctions.mapPrimitive(this, g);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Prefer to call one of the more strongly-typed map methods if you have a more specific function type.
     *
     * @param g the outer function
     * @return the function
     * @see #mapPrimitive(PrimitiveFunction)
     * @see #mapObj(ObjectFunction)
     */
    default TypedFunction<T> map(TypedFunction<R> g) {
        return ObjectFunctions.map(this, g);
    }

    /**
     * Creates a function by casting to {@code returnType}.
     *
     * <p>
     * In the case where {@code returnType().equals(returnType)}, the result is {@code (ObjectFunction<T, R2>) this}.
     * Otherwise, the result is equivalent to {@code x -> (R2) this.apply(x)}.
     *
     * @param returnType the return type
     * @return the object function
     * @param <R2> the return type
     */
    default <R2> ObjectFunction<T, R2> cast(GenericType<R2> returnType) {
        // noinspection unchecked
        return returnType().equals(returnType)
                ? (ObjectFunction<T, R2>) this
                : mapObj(ObjectFunctions.cast(returnType));
    }

    @Override
    default <Z> Z walk(Visitor<T, Z> visitor) {
        return visitor.visit(this);
    }
}

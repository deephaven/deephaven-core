/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import io.deephaven.qst.type.GenericType;

import java.util.function.Function;
import java.util.function.Predicate;

/**
 * An object function.
 *
 * <p>
 * Note: this is not a {@link FunctionalInterface}, as {@link #returnType()} must be provided.
 *
 * @param <T> the input type
 * @param <R> the return type
 */
public interface ToObjectFunction<T, R> extends TypedFunction<T>, Function<T, R> {

    /**
     * Creates an object function from {@code f} and {@code returnType}.
     *
     * @param f the function
     * @param returnType the return type
     * @return the object function
     * @param <T> the input type
     * @param <R> the return type
     */
    static <T, R> ToObjectFunction<T, R> of(Function<T, R> f, GenericType<R> returnType) {
        return ObjectFunctions.of(f, returnType);
    }

    /**
     * Creates an object function that casts each input to {@link Object}. Equivalent to {@code x -> (Object)x}.
     *
     * @return the object function
     * @param <T> the input type
     */
    static <T> ToObjectFunction<T, Object> identity() {
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
    static <T, R extends T> ToObjectFunction<T, R> identity(GenericType<R> returnType) {
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
    static <T, R, Z> ToObjectFunction<T, Z> map(Function<T, R> f, ToObjectFunction<R, Z> g) {
        return ObjectFunctions.map(f, g);
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.apply(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @param returnType the return type
     * @return the object function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R, Z> ToObjectFunction<T, Z> map(Function<T, R> f, Function<R, Z> g, GenericType<Z> returnType) {
        return ObjectFunctions.map(f, g, returnType);
    }

    GenericType<R> returnType();

    R apply(T value);

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Equivalent to {@code x -> g.test(this.apply(x))}.
     *
     * @param g the outer function
     * @return the boolean function
     */
    default ToBooleanFunction<T> mapToBoolean(Predicate<R> g) {
        return ToBooleanFunction.map(this, g);
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
    default ToCharFunction<T> mapToChar(ToCharFunction<R> g) {
        return ToCharFunction.map(this, g);
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
    default ToByteFunction<T> mapToByte(ToByteFunction<R> g) {
        return ToByteFunction.map(this, g);
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
    default ToShortFunction<T> mapToShort(ToShortFunction<R> g) {
        return ToShortFunction.map(this, g);
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
    default ToIntFunction<T> mapToInt(java.util.function.ToIntFunction<R> g) {
        return ToIntFunction.map(this, g);
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
    default ToLongFunction<T> mapToLong(java.util.function.ToLongFunction<R> g) {
        return ToLongFunction.map(this, g);
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
    default ToFloatFunction<T> mapToFloat(ToFloatFunction<R> g) {
        return ToFloatFunction.map(this, g);
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
    default ToDoubleFunction<T> mapToDouble(java.util.function.ToDoubleFunction<R> g) {
        return ToDoubleFunction.map(this, g);
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
    default <R2> ToObjectFunction<T, R2> mapToObj(ToObjectFunction<R, R2> g) {
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
    default <R2> ToObjectFunction<T, R2> mapToObj(Function<R, R2> g, GenericType<R2> returnType) {
        return map(this, g, returnType);
    }

    /**
     * Creates the function composition {@code g ∘ this}.
     *
     * <p>
     * Prefer to call one of the more strongly-typed map methods if you have a more specific function type.
     *
     * @param g the outer function
     * @return the function
     * @see #mapToBoolean(Predicate)
     * @see #mapToChar(ToCharFunction)
     * @see #mapToByte(ToByteFunction)
     * @see #mapToShort(ToShortFunction)
     * @see #mapToInt(java.util.function.ToIntFunction)
     * @see #mapToLong(java.util.function.ToLongFunction)
     * @see #mapToFloat(ToFloatFunction)
     * @see #mapToDouble(java.util.function.ToDoubleFunction)
     */
    default ToPrimitiveFunction<T> mapToPrimitive(ToPrimitiveFunction<R> g) {
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
     * @see #mapToPrimitive(ToPrimitiveFunction)
     * @see #mapToObj(ToObjectFunction)
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
    default <R2> ToObjectFunction<T, R2> cast(GenericType<R2> returnType) {
        // noinspection unchecked
        return returnType().equals(returnType)
                ? (ToObjectFunction<T, R2>) this
                : mapToObj(ObjectFunctions.cast(returnType));
    }

    @Override
    default <Z> Z walk(Visitor<T, Z> visitor) {
        return visitor.visit(this);
    }
}

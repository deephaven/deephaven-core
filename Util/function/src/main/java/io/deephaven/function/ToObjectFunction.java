//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

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
    static <T, R> ToObjectFunction<T, R> of(Function<? super T, ? extends R> f, GenericType<R> returnType) {
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
     * @param <Z> the return type
     */
    static <T, R, Z> ToObjectFunction<T, Z> map(
            Function<? super T, ? extends R> f,
            ToObjectFunction<? super R, Z> g) {
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
     * @param <Z> the return type
     */
    static <T, R, Z> ToObjectFunction<T, Z> map(
            Function<? super T, ? extends R> f,
            Function<? super R, ? extends Z> g,
            GenericType<Z> returnType) {
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
     * @param <T2> the returned function input type
     */
    default <T2 extends T> ToBooleanFunction<T2> mapToBoolean(Predicate<? super R> g) {
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
     * @param <T2> the returned function input type
     */
    default <T2 extends T> ToCharFunction<T2> mapToChar(ToCharFunction<? super R> g) {
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
     * @param <T2> the returned function input type
     */
    default <T2 extends T> ToByteFunction<T2> mapToByte(ToByteFunction<? super R> g) {
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
     * @param <T2> the returned function input type
     */
    default <T2 extends T> ToShortFunction<T2> mapToShort(ToShortFunction<? super R> g) {
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
     * @param <T2> the returned function input type
     */
    default <T2 extends T> ToIntFunction<T2> mapToInt(java.util.function.ToIntFunction<? super R> g) {
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
     * @param <T2> the returned function input type
     */
    default <T2 extends T> ToLongFunction<T2> mapToLong(java.util.function.ToLongFunction<? super R> g) {
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
     * @param <T2> the returned function input type
     */
    default <T2 extends T> ToFloatFunction<T2> mapToFloat(ToFloatFunction<? super R> g) {
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
     * @param <T2> the returned function input type
     */
    default <T2 extends T> ToDoubleFunction<T2> mapToDouble(java.util.function.ToDoubleFunction<? super R> g) {
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
     * @param <T2> the returned function input type
     * @param <R2> the returned function return type
     */
    default <T2 extends T, R2> ToObjectFunction<T2, R2> mapToObj(ToObjectFunction<? super R, R2> g) {
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
     * @param <T2> the returned function input type
     * @param <R2> the returned function return type
     **/
    default <T2 extends T, R2> ToObjectFunction<T2, R2> mapToObj(
            Function<? super R, ? extends R2> g,
            GenericType<R2> returnType) {
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
     * @param <T2> the returned function input type
     */
    default <T2 extends T> ToPrimitiveFunction<T2> mapToPrimitive(ToPrimitiveFunction<? super R> g) {
        return PrimitiveFunctions.map(this, g);
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
     * @param <T2> the returned function input type
     */
    default <T2 extends T> TypedFunction<T2> map(TypedFunction<? super R> g) {
        return TypedFunctions.map(this, g);
    }

    /**
     * Creates a function by casting to {@code returnType}.
     *
     * <p>
     * In the case where {@code returnType().equals(returnType)}, the result is {@code (ObjectFunction<T2, R2>) this}.
     * Otherwise, the result is equivalent to {@code x -> (R2) this.apply(x)}.
     *
     * @param returnType the return type
     * @return the object function
     * @param <T2> the returned function input type
     * @param <R2> the returned function return type
     */
    default <T2 extends T, R2> ToObjectFunction<T2, R2> cast(GenericType<R2> returnType) {
        return ObjectFunctions.castOrMapCast(this, returnType);
    }

    @Override
    default <Z> Z walk(Visitor<T, Z> visitor) {
        return visitor.visit(this);
    }
}

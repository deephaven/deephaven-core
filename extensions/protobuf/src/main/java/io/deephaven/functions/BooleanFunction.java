/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.functions;

import io.deephaven.qst.type.BooleanType;
import io.deephaven.qst.type.Type;

import java.util.Collection;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * A {@code boolean} function.
 *
 * @param <T> the input type
 */
@FunctionalInterface
public interface BooleanFunction<T> extends PrimitiveFunction<T>, Predicate<T> {

    /**
     * Assumes the object value is directly castable to a boolean. Equivalent to {@code x -> (boolean)x}.
     *
     * @return the boolean function
     * @param <T> the value type
     */
    static <T> BooleanFunction<T> primitive() {
        return BooleanFunctions.primitive();
    }

    /**
     * A function that always returns {@code true}.
     *
     * @return the true function
     * @param <T> the input type
     */
    static <T> BooleanFunction<T> ofTrue() {
        return BooleanFunctions.ofTrue();
    }

    /**
     * A function that always returns {@code false}.
     *
     * @return the false function
     * @param <T> the input type
     */
    static <T> BooleanFunction<T> ofFalse() {
        return BooleanFunctions.ofFalse();
    }

    /**
     * Creates the function composition {@code g ∘ f}.
     *
     * <p>
     * Equivalent to {@code x -> g.test(f.apply(x))}.
     *
     * @param f the inner function
     * @param g the outer function
     * @return the boolean function
     * @param <T> the input type
     * @param <R> the intermediate type
     */
    static <T, R> BooleanFunction<T> map(Function<T, R> f, BooleanFunction<R> g) {
        return BooleanFunctions.map(f, g);
    }

    /**
     * Creates a function that returns {@code true} if any function in {@code functions} returns {@code true}. If
     * {@code functions} is empty, returns {@link #ofFalse()}.
     *
     * @param functions the functions
     * @return the or-function
     * @param <T> the input type
     */
    static <T> BooleanFunction<T> or(Collection<BooleanFunction<T>> functions) {
        return BooleanFunctions.or(functions);
    }

    /**
     * Creates a function that returns {@code true} if all functions in {@code functions} returns {@code true}. If
     * {@code functions} is empty, returns {@link #ofTrue()}.
     *
     * @param functions the functions
     * @return the and-function
     * @param <T> the input type
     */
    static <T> BooleanFunction<T> and(Collection<BooleanFunction<T>> functions) {
        return BooleanFunctions.and(functions);
    }

    /**
     * Creates a function that is the opposite of {@code f}. Equivalent to {@code x -> !x}.
     *
     * @param f the function
     * @return the not-function
     * @param <T> the input type
     */
    static <T> BooleanFunction<T> not(BooleanFunction<T> f) {
        return BooleanFunctions.not(f);
    }

    @Override
    boolean test(T value);

    @Override
    default BooleanType returnType() {
        return Type.booleanType();
    }

    @Override
    default BooleanFunction<T> mapInput(Function<T, T> f) {
        return map(f, this);
    }

    @Override
    default <R> R walk(Visitor<T, R> visitor) {
        return visitor.visit(this);
    }
}

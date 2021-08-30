/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.db.plot.util.functions;

import groovy.lang.Closure;

/**
 * Wraps a {@link SerializableBiFunction} with the API of a function. <br/>
 */
public class ClosureBiFunction<T, U, R> extends SerializableClosure<R> implements SerializableBiFunction<T, U, R> {
    private static final long serialVersionUID = 697974379939190730L;

    /**
     * Creates a SerializableClosure instance with the two-argument {@code closure}.
     *
     * @param closure closure
     */
    public ClosureBiFunction(Closure<R> closure) {
        super(closure);
    }

    /**
     * Applies this function to the given arguments.
     *
     * @param t the first function argument
     * @param u the second function argument
     * @return the function result
     */
    @Override
    public R apply(T t, U u) {
        return getClosure().call(t, u);
    }
}

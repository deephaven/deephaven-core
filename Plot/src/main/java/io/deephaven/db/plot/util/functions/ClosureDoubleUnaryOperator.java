/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.util.functions;

import groovy.lang.Closure;

import java.util.function.DoubleUnaryOperator;

/**
 * A serializable closure which maps doubles to doubles.
 */
public class ClosureDoubleUnaryOperator<T extends Number> extends SerializableClosure<T>
    implements DoubleUnaryOperator {

    private static final long serialVersionUID = -4092987117189101803L;

    /**
     * Constructs a ClosureDoubleUnaryOperator instance.
     *
     * @param closure closure
     */
    public ClosureDoubleUnaryOperator(final Closure<T> closure) {
        super(closure);
    }

    @Override
    public double applyAsDouble(final double operand) {
        final T v = getClosure().call(operand);
        return v == null ? Double.NaN : v.doubleValue();
    }
}

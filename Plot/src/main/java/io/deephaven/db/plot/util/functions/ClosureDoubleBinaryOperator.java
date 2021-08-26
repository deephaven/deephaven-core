/*
 *
 * * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 *
 */

package io.deephaven.db.plot.util.functions;

import groovy.lang.Closure;

import java.util.function.DoubleBinaryOperator;

/**
 * A serializable closure which maps pair of doubles to doubles.
 */
public class ClosureDoubleBinaryOperator<T extends Number> extends SerializableClosure<T>
        implements DoubleBinaryOperator {

    private static final long serialVersionUID = -6533578879266557626L;

    /**
     * Constructs a ClosureDoubleBinaryOperator instance.
     *
     * @param closure closure
     */
    public ClosureDoubleBinaryOperator(final Closure<T> closure) {
        super(closure);
    }

    @Override
    public double applyAsDouble(final double left, final double right) {
        final T v = getClosure().call(left, right);
        return v == null ? Double.NaN : v.doubleValue();
    }
}

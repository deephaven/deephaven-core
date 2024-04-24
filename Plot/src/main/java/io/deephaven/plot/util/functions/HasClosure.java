//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.util.functions;

import io.deephaven.base.verify.Require;
import groovy.lang.Closure;

/**
 * A serializable closure.
 */
public class HasClosure<T> {

    private final Closure<T> closure;

    /**
     * Creates a SerializableClosure instance with the {@code closure}.
     *
     * @param closure closure
     */
    public HasClosure(final Closure<T> closure) {
        Require.neqNull(closure, "closure");
        this.closure = closure.dehydrate();
        this.closure.setResolveStrategy(Closure.DELEGATE_ONLY);
    }

    /**
     * Gets this SerializableClosure's closure.
     *
     * @return this SerializableClosure's closure
     */
    public Closure<T> getClosure() {
        return closure;
    }
}

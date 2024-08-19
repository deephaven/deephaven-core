//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.util.functions;

import io.deephaven.base.testing.BaseArrayTestCase;
import groovy.lang.Closure;

public class TestHasClosure extends BaseArrayTestCase {

    private final String value = "S";

    private final Closure<String> closure = new Closure<String>(null) {
        @Override
        public String call() {
            return value;
        }

        @Override
        public String call(Object... args) {
            return value;
        }

        @Override
        public String call(Object arguments) {
            return value;
        }
    };

    public void testSerializableClosure() {
        HasClosure<String> hasClosure = new ClosureFunction<>(closure);

        assertEquals(value, hasClosure.getClosure().call());
        assertEquals(value, hasClosure.getClosure().call("T"));
        assertEquals(value, hasClosure.getClosure().call("A", "B"));
    }
}

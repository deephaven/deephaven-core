//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.util.functions;

import groovy.lang.Closure;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestHasClosure {

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

    @Test
    public void testSerializableClosure() {
        HasClosure<String> hasClosure = new ClosureFunction<>(closure);

        assertEquals(value, hasClosure.getClosure().call());
        assertEquals(value, hasClosure.getClosure().call("T"));
        assertEquals(value, hasClosure.getClosure().call("A", "B"));
    }
}

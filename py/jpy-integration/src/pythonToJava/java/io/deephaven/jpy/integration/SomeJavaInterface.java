/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.jpy.integration;

import org.jpy.PyObject;

public interface SomeJavaInterface {

    // see test_jpy.py
    @SuppressWarnings("unused")
    static SomeJavaInterface proxy(PyObject object) {
        return object.createProxy(SomeJavaInterface.class);
    }

    int foo(int bar, int baz);
}

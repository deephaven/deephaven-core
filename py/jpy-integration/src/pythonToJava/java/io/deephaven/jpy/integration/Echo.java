package io.deephaven.jpy.integration;

import org.jpy.PyObject;

public class Echo {
    // see test_jpy.py
    @SuppressWarnings("unused")
    public static PyObject echo(PyObject pyObject) {
        return pyObject;
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.integrations.python;

import org.jpy.PyObject;

import java.util.Objects;

/**
 * A {@link Runnable} implementation which calls a Python callable.
 */
public class PythonRunnable implements Runnable {
    private final PyObject pyCallable;

    public PythonRunnable(PyObject pyCallable) {
        this.pyCallable = Objects.requireNonNull(pyCallable);
    }

    @Override
    public void run() {
        pyCallable.call("__call__");
    }
}

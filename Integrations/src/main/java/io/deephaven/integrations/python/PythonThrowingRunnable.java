/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.integrations.python;

import io.deephaven.util.FunctionalInterfaces;
import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;

/**
 * A {@link FunctionalInterfaces.ThrowingRunnable} implementation which executes a Python callable.
 */
@ScriptApi
public class PythonThrowingRunnable implements FunctionalInterfaces.ThrowingRunnable<Exception> {
    private final PyObject pyCallable;

    /**
     * Creates a new runnable.
     *
     * @param pyObjectIn the python object providing a function - must either be callable or have an "apply" attribute
     *        which is callable.
     */
    public PythonThrowingRunnable(final PyObject pyObjectIn) {
        if (pyObjectIn.hasAttribute("apply")) {
            pyCallable = pyObjectIn.getAttribute("apply");
            if (!pyCallable.hasAttribute("__call__")) {
                throw new IllegalArgumentException("The Python object provided has an apply attribute " +
                        "which is not callable");
            }
        } else if (pyObjectIn.hasAttribute("__call__")) {
            pyCallable = pyObjectIn;
        } else {
            throw new IllegalArgumentException("The Python object specified should either be callable, or a " +
                    "class instance with an apply method");
        }
    }

    @Override
    public void run() throws Exception {
        pyCallable.call("__call__");
    }
}

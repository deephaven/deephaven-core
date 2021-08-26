/*
 * Copyright (c) 2016-2021. Deephaven Data Labs and Patent Pending.
 */

package io.deephaven.integrations.python;

import org.jpy.PyObject;

/**
 * Utilities for implementing python functionality.
 */
class PythonUtilities {

    /**
     * Gets the python function that should be called by a listener. The input can be either (1) a
     * callable or (2) an object which provides an "onUpdate" method.
     *
     * @param pyObject python listener object. This should either be a callable or an object which
     *        provides an "onUpdate" method.
     * @return python function that should be called by a listener.
     * @throws IllegalArgumentException python listener object is not a valid listener.
     */
    static PyObject pyListenerFunc(final PyObject pyObject) {
        if (pyObject.hasAttribute("onUpdate")) {
            PyObject pyCallable = pyObject.getAttribute("onUpdate");

            if (!pyCallable.hasAttribute("__call__")) {
                throw new IllegalArgumentException(
                    "The Python object provided has an onUpdate attribute " +
                        "which is not callable");
            }

            return pyCallable;
        } else if (pyObject.hasAttribute("__call__")) {
            return pyObject;
        } else {
            throw new IllegalArgumentException(
                "The Python object specified should either be callable, or a " +
                    "class instance with an onUpdate method");
        }
    }
}

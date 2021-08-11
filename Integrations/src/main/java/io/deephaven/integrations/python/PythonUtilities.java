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
     * Gets the python function that should be called by a listener.  The input can be either (1) a callable or
     * (2) an object which provides an "onUpdate" method.
     *
     * @param pyObject python listener object.  This should either be a callable or an object which provides an "onUpdate" method.
     * @return python function that should be called by a listener.
     * @throws IllegalArgumentException python listener object is not a valid listener.
     */
    static PyObject pyListenerFunc(final PyObject pyObject) {
        if (pyObject.hasAttribute("onUpdate")) {
            PyObject pyCallable = pyObject.getAttribute("onUpdate");

            if (!pyCallable.hasAttribute("__call__")) {
                throw new IllegalArgumentException("The Python object provided has an onUpdate attribute " +
                        "which is not callable");
            }

            return pyCallable;
        } else if (pyObject.hasAttribute("__call__")) {
            return pyObject;
        } else {
            throw new IllegalArgumentException("The Python object specified should either be callable, or a " +
                    "class instance with an onUpdate method");
        }
    }

    /**
     * Creates a callable PyObject, either using method.apply() or __call__(), if the pyObjectIn has such methods available.
     *
     * @param pyObjectIn the python object providing the function - must either be callable or have an `apply` attribute
     *                   which is callable.
     * @return pyCallable that can be called directly with arguments using pyCallable.call(...).
     */
    static PyObject pyCallableGenerator(PyObject pyObjectIn) {
        PyObject pyCallable;
        if(pyObjectIn.hasAttribute("apply")){
            pyCallable = pyObjectIn.getAttribute("apply");
            if (!pyCallable.hasAttribute("__call__")){
                throw new IllegalArgumentException("The Python object provided has an apply attribute " +
                        "which is not callable");
            }
        }else if (pyObjectIn.hasAttribute("__call__")){
            pyCallable = pyObjectIn;
        }else{
            throw new IllegalArgumentException("The Python object specified should either be callable, or a " +
                    "class instance with an apply method");
        }

        return pyCallable;
    }
}

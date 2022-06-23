/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.integrations.python;

import org.jpy.PyModule;
import org.jpy.PyObject;

/**
 * This is a helper class for wrapping a Java object in a Python wrapper and unwrapping a Python wrapper to a raw Java
 * object.
 */
public class PythonObjectWrapper {
    private static final PyModule PY_WRAPPER_MODULE = PyModule.importModule("deephaven._wrapper");

    /**
     * Unwrap a Python object to return the wrapped Java object.
     * 
     * @param t An instance of {@link PyObject}.
     * @return The wrapped Java object.
     */
    public static Object unwrap(PyObject t) {
        // noinspection ConstantConditions
        return PY_WRAPPER_MODULE.call("unwrap", t).getObjectValue();
    }

    /**
     * Wrap a raw Java object with a Python wrapper if one exists for its type, otherwise return a JPY mapped Python
     * object.
     * 
     * @param t A Java object.
     * @return A {@link PyObject} instance representing the Python wrapper object.
     */
    public static PyObject wrap(Object t) {
        // noinspection ConstantConditions
        return PY_WRAPPER_MODULE.call("wrap_j_object", t);
    }
}

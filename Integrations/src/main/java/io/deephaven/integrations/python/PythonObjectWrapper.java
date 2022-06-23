package io.deephaven.integrations.python;

import org.jpy.PyModule;
import org.jpy.PyObject;

public class PythonObjectWrapper {
    private static final PyModule PY_WRAPPER_MODULE = PyModule.importModule("deephaven._wrapper");

    static PyObject unwrap(PyObject t) {
        return pyWrapperModule.call("unwrap", t);
    }

    static PyObject wrap(Object t) {
        return pyWrapperModule.call("wrap_j_object", t);
    }
}

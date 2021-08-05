package io.deephaven.integrations.learn;

import io.deephaven.integrations.python.PythonFunction;
import org.jpy.PyObject;

/**
 * This class is strictly for internal tests. Specifically used for testing the passThrough() method in the
 * PythonFunction class.
 */

class CallPyFunc {

    PyObject func;
    PyObject[] args;

    CallPyFunc(PyObject func, PyObject ... args) {
        this.func = func;
        this.args = args;
    }

    PyObject call() {
        PythonFunction<PyObject> funcCaller = new PythonFunction<PyObject>(this.func, PyObject.class);
        return funcCaller.passThrough(args);
    }
}

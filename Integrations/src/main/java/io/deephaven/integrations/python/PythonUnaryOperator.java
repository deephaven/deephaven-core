package io.deephaven.integrations.python;

import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;

import java.util.function.Function;
import java.util.function.UnaryOperator;

@ScriptApi
public class PythonUnaryOperator<T> extends PythonFunction implements UnaryOperator {

    /**
     * Creates a {@link UnaryOperator} which calls a Python function.
     *
     * @param pyObjectIn the python object providing the function - must either be callable or have an `apply` attribute
     *                   which is callable.
     * @param classOut   the specific java class to interpret the return for the method. Note that this is probably only
     *                   really useful if `classOut` is one of String, double, float, long, int, short, byte, or boolean.
     *                   Otherwise, the return element will likely just remain PyObject, and not be particularly usable inside
     *                   Java.
     */
    public PythonUnaryOperator(PyObject pyObjectIn, Class classOut) {
        super(pyObjectIn, classOut);
    }
}

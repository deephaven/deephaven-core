/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.integrations.python;

import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;

import java.util.function.Function;

import static io.deephaven.integrations.python.PythonUtils.pyApplyFunc;

/**
 * A {@link Function} implementation which calls a Python callable.
 * 
 * @param <T> input argument class
 */
@ScriptApi
public class PythonFunction<T> implements Function<T, Object> {
    private final PyObject pyCallable;
    private final Class classOut;

    /**
     * Creates a {@link Function} which calls a Python function.
     *
     * @param pyObjectIn the python object providing the function - must either be callable or have an `apply` attribute
     *        which is callable.
     * @param classOut the specific java class to interpret the return for the method. Note that this is probably only
     *        really useful if `classOut` is one of String, double, float, long, int, short, byte, or boolean.
     *        Otherwise, the return element will likely just remain PyObject, and not be particularly usable inside
     *        Java.
     */
    public PythonFunction(final PyObject pyObjectIn, final Class classOut) {

        pyCallable = pyApplyFunc(pyObjectIn);
        this.classOut = classOut;

    }

    @Override
    public Object apply(T t) {
        PyObject wrapped = PythonObjectWrapper.wrap(t);
        PyObject out = pyCallable.call("__call__", wrapped);
        return PythonValueGetter.getValue(out, classOut);
    }

}

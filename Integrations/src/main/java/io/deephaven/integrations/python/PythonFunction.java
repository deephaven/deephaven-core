/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.integrations.python;

import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;

import java.util.function.Function;
import java.util.function.UnaryOperator;

import static io.deephaven.integrations.python.PythonUtils.pyApplyFunc;

/**
 * A {@link Function} implementation which calls a Python callable.
 * 
 * @param <T> input argument class
 */
@ScriptApi
public class PythonFunction<T, R> implements Function<T, R> {
    private final PyObject pyCallable;
    private final Class classOut;

    /**
     * Creates a {@link Function} which calls a Python function.
     *
     * @param pyCallable the python object providing the function - must either be callable or have an `apply` attribute
     *        which is callable.
     * @param classOut the specific java class to interpret the return for the method. This can be one of String,
     *        double, float, long, int, short, byte, or boolean; or in the case of Python wrapper objects, PyObject,
     *        such objects then can be unwrapped to be used inside Java.
     */
    public PythonFunction(final PyObject pyCallable, final Class<R> classOut) {

        this.pyCallable = pyApplyFunc(pyCallable);
        this.classOut = classOut;

    }

    @Override
    public R apply(T t) {
        PyObject wrapped = PythonObjectWrapper.wrap(t);
        PyObject out = pyCallable.call("__call__", wrapped);
        return (R) PythonValueGetter.getValue(out, classOut);
    }

    public static class PythonUnaryOperator<T> extends PythonFunction<T, T> implements UnaryOperator<T> {

        /**
         * Creates a {@link UnaryOperator} which calls a Python function.
         *
         * @param pyCallable the python object providing the function - must either be callable or have an `apply`
         *        attribute which is callable.
         * @param classOut the specific java class to interpret the return for the method. This can be one of String,
         *        double, float, long, int, short, byte, or boolean; or in the case of Python wrapper objects, PyObject,
         *        such objects then can be unwrapped to be used inside Java.
         */
        public PythonUnaryOperator(PyObject pyCallable, Class classOut) {
            super(pyCallable, classOut);
        }
    }
}

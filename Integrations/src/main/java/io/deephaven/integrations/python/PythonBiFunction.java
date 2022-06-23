/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.integrations.python;

import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;

import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

import static io.deephaven.integrations.python.PythonUtils.pyApplyFunc;

/**
 * A {@link BiFunction} implementation which calls a Python callable.
 * 
 * @param <T> input argument class
 * @param <U> input argument class
 */
@ScriptApi
public class PythonBiFunction<T, U, R> implements BiFunction<T, U, R> {
    private final PyObject pyCallable;
    private final Class<R> classOut;

    /**
     * Creates a {@link BiFunction} which calls a Python function.
     *
     * @param pyCallable the python object providing the function - must either be callable or have an `apply` attribute
     *        which is callable.
     * @param classOut the specific java class to interpret the return for the method. This can be one of String,
     *        double, float, long, int, short, byte, or boolean; or in the case of Python wrapper objects, PyObject,
     *        such objects then can be unwrapped to be used inside Java.
     */
    public PythonBiFunction(final PyObject pyCallable, final Class<R> classOut) {
        this.pyCallable = pyApplyFunc(pyCallable);
        this.classOut = classOut;
    }

    @Override
    public R apply(T t, U u) {
        PyObject wrapped = PythonObjectWrapper.wrap(t);
        PyObject wrappedOther = PythonObjectWrapper.wrap(u);
        PyObject out = pyCallable.call("__call__", wrapped, wrappedOther);
        return PythonValueGetter.getValue(out, classOut);
    }


    public static class PythonBinaryOperator<T> extends PythonBiFunction<T, T, T> implements BinaryOperator<T> {
        /**
         * Creates a {@link PythonBinaryOperator} which calls a Python function.
         *
         * @param pyCallable the python object providing the function - must either be callable or have an `apply`
         *        attribute which is callable.
         * @param classOut the specific java class to interpret the return for the method. This can be one of String,
         *        double, float, long, int, short, byte, or boolean; or in the case of Python wrapper objects, PyObject,
         *        such objects then can be unwrapped to be used inside Java.
         */
        public PythonBinaryOperator(PyObject pyCallable, Class<T> classOut) {
            super(pyCallable, classOut);
        }
    }
}

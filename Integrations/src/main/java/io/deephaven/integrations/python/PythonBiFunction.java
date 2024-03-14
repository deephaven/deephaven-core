//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
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
@SuppressWarnings("unused")
@ScriptApi
public class PythonBiFunction<T, U, R> implements BiFunction<T, U, R> {
    private final PyObject pyCallable;
    private final Class<R> classOut;

    /**
     * Creates a {@link BiFunction} which calls a Python function.
     *
     * @param pyCallable The python object providing the function - must either be callable or have an {@code apply}
     *        attribute which is callable.
     * @param classOut The specific Java class expected to be returned by the {@link #apply(Object, Object)} method.
     *        This should be the result of converting or unwrapping the output of {@code pyCallable}.
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
         * @param pyCallable The python object providing the function - must either be callable or have an {@code apply}
         *        attribute which is callable.
         * @param classOut The specific Java class expected to be returned by the {@link #apply(Object, Object)} method.
         *        This should be the result of converting or unwrapping the output of {@code pyCallable}.
         */
        public PythonBinaryOperator(PyObject pyCallable, Class<T> classOut) {
            super(pyCallable, classOut);
        }
    }
}

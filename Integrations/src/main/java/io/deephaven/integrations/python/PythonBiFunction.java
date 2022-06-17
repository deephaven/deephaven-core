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
public class PythonBiFunction<T, U> implements BiFunction<T, U, Object> {
    private final PyObject pyCallable;
    private final Class classOut;

    /**
     * Creates a {@link BiFunction} which calls a Python function.
     *
     * @param pyCallable the python object providing the function - must either be callable or have an `apply` attribute
     *        which is callable.
     * @param classOut the specific java class to interpret the return for the method. Note that this is probably only
     *        really useful if `classOut` is one of String, double, float, long, int, short, byte, or boolean.
     *        Otherwise, the return element will likely just remain PyObject, and not be particularly usable inside
     *        Java.
     */
    public PythonBiFunction(final PyObject pyCallable, final Class classOut) {
        this.pyCallable = pyApplyFunc(pyCallable);
        this.classOut = classOut;
    }

    @Override
    public Object apply(T t, U u) {
        PyObject wrapped = PythonObjectWrapper.wrap(t);
        PyObject wrappedOther = PythonObjectWrapper.wrap(u);
        PyObject out = pyCallable.call("__call__", wrapped, wrappedOther);
        return PythonValueGetter.getValue(out, classOut);
    }


    public static class PythonBinaryOperator extends PythonBiFunction implements BinaryOperator {
        /**
         * Creates a {@link PythonBinaryOperator} which calls a Python function.
         *
         * @param pyCallable the python object providing the function - must either be callable or have an `apply`
         *        attribute which is callable.
         * @param classOut the specific java class to interpret the return for the method. Note that this is probably
         *        only really useful if `classOut` is one of String, double, float, long, int, short, byte, or boolean.
         *        Otherwise, the return element will likely just remain PyObject, and not be particularly usable inside
         *        Java.
         */
        public PythonBinaryOperator(PyObject pyCallable, Class classOut) {
            super(pyCallable, classOut);
        }
    }
}

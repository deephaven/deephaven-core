/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.integrations.python;

import io.deephaven.util.annotations.ScriptApi;
import org.jpy.PyObject;
import java.util.function.Function;
import io.deephaven.util.QueryConstants;

import static io.deephaven.integrations.python.PythonUtilities.pyApplyFunc;

/**
 * A {@link Function} implementation which calls a Python callable.
 * 
 * @param <T> input argument class
 */
@ScriptApi
public class PythonFunction<T> implements Function<T, Object> {
    private final PyObject pyCallable;
    private final Function<PyObject, Object> getter;

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

        // Note: Potentially important types omitted -simply because handling from python is not super clear:
        // Character/char, BigInteger, BigDecimal
        if (CharSequence.class.isAssignableFrom(classOut)) {
            getter = new StringValueGetter();
        } else if (classOut.equals(Double.class) || classOut.equals(double.class)) {
            getter = new DoubleValueGetter();
        } else if (classOut.equals(Float.class) || classOut.equals(float.class)) {
            getter = new FloatValueGetter();
        } else if (classOut.equals(Long.class) || classOut.equals(long.class)) {
            getter = new LongValueGetter();
        } else if (classOut.equals(Integer.class) || classOut.equals(int.class)) {
            getter = new IntValueGetter();
        } else if (classOut.equals(Short.class) || classOut.equals(short.class)) {
            getter = new ShortValueGetter();
        } else if (classOut.equals(Byte.class) || classOut.equals(byte.class)) {
            getter = new ByteValueGetter();
        } else if (classOut.equals(Boolean.class) || classOut.equals(boolean.class)) {
            getter = new BoolValueGetter();
        } else {
            getter = new ObjectValueGetter(); // warning or something? This will likely be useless.
        }
    }

    @Override
    public Object apply(T t) {
        PyObject out = pyCallable.call("__call__", t);
        return getter.apply(out);
    }

    static class ObjectValueGetter implements Function<PyObject, Object> {
        @Override
        public Object apply(PyObject valueIn) {
            if (valueIn == null) {
                return null;
            }
            return valueIn.getObjectValue();
        }
    }

    static class StringValueGetter implements Function<PyObject, Object> {
        @Override
        public Object apply(PyObject valueIn) {
            if (valueIn == null) {
                return null;
            }
            return valueIn.getStringValue();
        }
    }

    static class DoubleValueGetter implements Function<PyObject, Object> {
        @Override
        public Object apply(PyObject valueIn) {
            if (valueIn == null) {
                return QueryConstants.NULL_DOUBLE;
            }
            return valueIn.getDoubleValue();
        }
    }

    static class FloatValueGetter implements Function<PyObject, Object> {
        @Override
        public Object apply(PyObject valueIn) {
            if (valueIn == null) {
                return QueryConstants.NULL_FLOAT;
            }
            return (float) valueIn.getDoubleValue(); // NB: should there be a getFloatValue() in jpy?
        }
    }

    static class LongValueGetter implements Function<PyObject, Object> {
        @Override
        public Object apply(PyObject valueIn) {
            if (valueIn == null) {
                return QueryConstants.NULL_LONG;
            }
            return valueIn.getLongValue();
        }
    }

    static class IntValueGetter implements Function<PyObject, Object> {
        @Override
        public Object apply(PyObject valueIn) {
            if (valueIn == null) {
                return QueryConstants.NULL_INT;
            }
            return valueIn.getIntValue();
        }
    }

    static class ShortValueGetter implements Function<PyObject, Object> {
        @Override
        public Object apply(PyObject valueIn) {
            if (valueIn == null) {
                return QueryConstants.NULL_SHORT;
            }
            return (short) valueIn.getIntValue(); // NB: should there be a getShortValue() in jpy?
        }
    }

    static class ByteValueGetter implements Function<PyObject, Object> {
        @Override
        public Object apply(PyObject valueIn) {
            if (valueIn == null) {
                return QueryConstants.NULL_BYTE; // NB: should there be a getByteValue() in jpy?
            }
            return (byte) valueIn.getIntValue();
        }
    }

    static class BoolValueGetter implements Function<PyObject, Object> {
        @Override
        public Object apply(PyObject valueIn) {
            if (valueIn == null) {
                return null;
            }
            return valueIn.getBooleanValue();
        }
    }
}

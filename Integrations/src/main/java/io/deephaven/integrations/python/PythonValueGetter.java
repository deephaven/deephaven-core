package io.deephaven.integrations.python;

import io.deephaven.util.QueryConstants;
import org.jpy.PyObject;

/**
 * This helper class implements safe methods which return a value of a specified Java type from a {@link PyObject}
 * instance. In particular, if the instance is a Deephaven Python wrapper object, it will be unwrapped to reveal the
 * Java object.
 */
class PythonValueGetter {

    static Object getObject(PyObject valueIn) {
        if (valueIn == null) {
            return null;
        }
        return PythonObjectWrapper.unwrap(valueIn);
    }

    static String getString(PyObject valueIn) {
        if (valueIn == null) {
            return null;
        }
        return valueIn.getStringValue();
    }

    static double getDouble(PyObject valueIn) {
        if (valueIn == null) {
            return QueryConstants.NULL_DOUBLE;
        }
        return valueIn.getDoubleValue();
    }

    static float getFloat(PyObject valueIn) {
        if (valueIn == null) {
            return QueryConstants.NULL_FLOAT;
        }
        return (float) valueIn.getDoubleValue(); // NB: should there be a getFloatValue() in jpy?
    }

    static long getLong(PyObject valueIn) {
        if (valueIn == null) {
            return QueryConstants.NULL_LONG;
        }
        return valueIn.getLongValue();
    }

    static int getInt(PyObject valueIn) {
        if (valueIn == null) {
            return QueryConstants.NULL_INT;
        }
        return valueIn.getIntValue();
    }

    static short getShort(PyObject valueIn) {
        if (valueIn == null) {
            return QueryConstants.NULL_SHORT;
        }
        return (short) valueIn.getIntValue(); // NB: should there be a getShortValue() in jpy?
    }

    static byte getByte(PyObject valueIn) {
        if (valueIn == null) {
            return QueryConstants.NULL_BYTE; // NB: should there be a getByteValue() in jpy?
        }
        return (byte) valueIn.getIntValue();
    }

    static Boolean getBoolean(PyObject valueIn) {
        if (valueIn == null) {
            return null;
        }
        return valueIn.getBooleanValue();
    }

    static <R> R getValue(PyObject pyObject, Class<R> classOut) {
        final Object result;
        // Note: Potentially important types omitted -simply because handling from python is not super clear:
        // Character/char, BigInteger, BigDecimal
        if (CharSequence.class.isAssignableFrom(classOut)) {
            result = getString(pyObject);
        } else if (classOut.equals(Double.class) || classOut.equals(double.class)) {
            result = getDouble(pyObject);
        } else if (classOut.equals(Float.class) || classOut.equals(float.class)) {
            result = getFloat(pyObject);
        } else if (classOut.equals(Long.class) || classOut.equals(long.class)) {
            result = getLong(pyObject);
        } else if (classOut.equals(Integer.class) || classOut.equals(int.class)) {
            result = getInt(pyObject);
        } else if (classOut.equals(Short.class) || classOut.equals(short.class)) {
            result = getShort(pyObject);
        } else if (classOut.equals(Byte.class) || classOut.equals(byte.class)) {
            result = getByte(pyObject);
        } else if (classOut.equals(Boolean.class) || classOut.equals(boolean.class)) {
            result = getBoolean(pyObject);
        } else {
            result = getObject(pyObject);
        }
        // noinspection unchecked
        return (R) result;
    }
}

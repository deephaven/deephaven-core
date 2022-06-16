package io.deephaven.integrations.python;

import io.deephaven.util.QueryConstants;
import org.jpy.PyObject;

public class PythonValueGetter {

    static Object getObject(PyObject valueIn) {
        if (valueIn == null) {
            return null;
        }
        return PythonObjectWrapper.unwrap(valueIn).getObjectValue();
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

    static Object getValue(PyObject pyObject, Class classOut) {
        // Note: Potentially important types omitted -simply because handling from python is not super clear:
        // Character/char, BigInteger, BigDecimal
        if (CharSequence.class.isAssignableFrom(classOut)) {
            return getString(pyObject);
        } else if (classOut.equals(Double.class) || classOut.equals(double.class)) {
            return getDouble(pyObject);
        } else if (classOut.equals(Float.class) || classOut.equals(float.class)) {
            return getFloat(pyObject);
        } else if (classOut.equals(Long.class) || classOut.equals(long.class)) {
            return getLong(pyObject);
        } else if (classOut.equals(Integer.class) || classOut.equals(int.class)) {
            return getInt(pyObject);
        } else if (classOut.equals(Short.class) || classOut.equals(short.class)) {
            return getShort(pyObject);
        } else if (classOut.equals(Byte.class) || classOut.equals(byte.class)) {
            return getByte(pyObject);
        } else if (classOut.equals(Boolean.class) || classOut.equals(boolean.class)) {
            return getBoolean(pyObject);
        } else {
            return getObject(pyObject);
        }
    }
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.tables.utils;

import io.deephaven.base.verify.Require;
import io.deephaven.db.tables.dbarrays.*;
import io.deephaven.util.type.TypeUtils;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import static io.deephaven.util.QueryConstants.NULL_BOOLEAN;
import static io.deephaven.util.QueryConstants.NULL_BYTE;
import static io.deephaven.util.QueryConstants.NULL_CHAR;
import static io.deephaven.util.QueryConstants.NULL_DOUBLE;
import static io.deephaven.util.QueryConstants.NULL_FLOAT;
import static io.deephaven.util.QueryConstants.NULL_INT;
import static io.deephaven.util.QueryConstants.NULL_LONG;
import static io.deephaven.util.QueryConstants.NULL_SHORT;

/**
 * Common utilities for interacting generically with arrays.
 */
public class ArrayUtils {
    public static final char[] EMPTY_CHAR_ARRAY = new char[0];
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final short[] EMPTY_SHORT_ARRAY = new short[0];
    public static final int[] EMPTY_INT_ARRAY = new int[0];
    public static final long[] EMPTY_LONG_ARRAY = new long[0];
    public static final float[] EMPTY_FLOAT_ARRAY = new float[0];
    public static final double[] EMPTY_DOUBLE_ARRAY = new double[0];
    public static final boolean[] EMPTY_BOOLEAN_ARRAY = new boolean[0];

    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    public static final DBDateTime[] EMPTY_DATETIME_ARRAY = new DBDateTime[0];

    public static ArrayAccessor getArrayAccessor(Object array) {
        final Class<?> c = array.getClass();
        if (c.equals(Boolean[].class)) {
            return new BooleanArrayAccessor((Boolean[]) array);
        } else if (c.equals(byte[].class)) {
            return new ByteArrayAccessor((byte[]) array);
        } else if (c.equals(char[].class)) {
            return new CharArrayAccessor((char[]) array);
        } else if (c.equals(double[].class)) {
            return new DoubleArrayAccessor((double[]) array);
        } else if (c.equals(float[].class)) {
            return new FloatArrayAccessor((float[]) array);
        } else if (c.equals(int[].class)) {
            return new IntArrayAccessor((int[]) array);
        } else if (c.equals(long[].class)) {
            return new LongArrayAccessor((long[]) array);
        } else if (c.equals(short[].class)) {
            return new ShortArrayAccessor((short[]) array);
        } else {
            return new ObjectArrayAccessor((Object[]) array);
        }
    }

    public static ArrayAccessor createArrayAccessor(Object element, int size) {
        if (element == null) {
            return new ObjectArrayAccessor(new Object[size]);
        }
        final Class<?> c = element.getClass();
        if (c.equals(boolean.class) || c.equals(Boolean.class)) {
            return new BooleanArrayAccessor(booleanNullArray(size));
        } else if (c.equals(byte.class) || c.equals(Byte.class)) {
            return new ByteArrayAccessor(byteNullArray(size));
        } else if (c.equals(char.class) || c.equals(Character.class)) {
            return new CharArrayAccessor(charNullArray(size));
        } else if (c.equals(double.class) || c.equals(Double.class)) {
            return new DoubleArrayAccessor(doubleNullArray(size));
        } else if (c.equals(float.class) || c.equals(Float.class)) {
            return new FloatArrayAccessor(floatNullArray(size));
        } else if (c.equals(int.class) || c.equals(Integer.class)) {
            return new IntArrayAccessor(intNullArray(size));
        } else if (c.equals(long.class) || c.equals(Long.class)) {
            return new LongArrayAccessor(longNullArray(size));
        } else if (c.equals(short.class) || c.equals(Short.class)) {
            return new ShortArrayAccessor(shortNullArray(size));
        } else {
            return new ObjectArrayAccessor((Object[]) Array.newInstance(c, size));
        }
    }

    public static int[] intNullArray(int size) {
        int[] result = new int[size];
        Arrays.fill(result, NULL_INT);
        return result;
    }

    public static Boolean[] booleanNullArray(int size) {
        Boolean[] result = new Boolean[size];
        Arrays.fill(result, NULL_BOOLEAN);
        return result;
    }

    public static byte[] byteNullArray(int size) {
        byte[] result = new byte[size];
        Arrays.fill(result, NULL_BYTE);
        return result;
    }

    public static char[] charNullArray(int size) {
        char[] result = new char[size];
        Arrays.fill(result, NULL_CHAR);
        return result;
    }

    public static double[] doubleNullArray(int size) {
        double[] result = new double[size];
        Arrays.fill(result, NULL_DOUBLE);
        return result;
    }

    public static float[] floatNullArray(int size) {
        float[] result = new float[size];
        Arrays.fill(result, NULL_FLOAT);
        return result;
    }

    public static long[] longNullArray(int size) {
        long[] result = new long[size];
        Arrays.fill(result, NULL_LONG);
        return result;
    }

    public static short[] shortNullArray(int size) {
        short[] result = new short[size];
        Arrays.fill(result, NULL_SHORT);
        return result;
    }

    public static Object toArray(Collection<?> objects, Class elementType) {
        if (elementType == boolean.class) {
            elementType = Boolean.class;
        }
        Object result = Array.newInstance(elementType, objects.size());
        ArrayAccessor accessor = getArrayAccessor(result);
        int i = 0;
        for (Object object : objects) {
            accessor.set(i++, object);
        }
        return result;
    }

    public static Object boxedToPrimitive(Set<?> objects, Class type) {
        Iterator<?> it = objects.iterator();
        if (objects.isEmpty()) {
            Class primitiveType = io.deephaven.util.type.TypeUtils.getUnboxedType(type);
            if (primitiveType == null) {
                return Array.newInstance(type, 0);
            } else {
                return Array.newInstance(primitiveType, 0);
            }
        }
        Object current = it.next();
        ArrayAccessor resultAccessor = createArrayAccessor(current, objects.size());
        int i = 0;
        resultAccessor.set(i++, current);
        while (it.hasNext()) {
            current = it.next();
            resultAccessor.set(i++, current);
        }
        return resultAccessor.getArray();
    }

    public static ArrayAccessor getArrayAccessorFromArray(Object arrayPrototype, int size) {
        final Class<?> c = arrayPrototype.getClass();
        if (c.equals(boolean[].class)) {
            return new BooleanArrayAccessor(booleanNullArray(size));
        } else if (c.equals(byte[].class)) {
            return new ByteArrayAccessor(byteNullArray(size));
        } else if (c.equals(char[].class)) {
            return new CharArrayAccessor(charNullArray(size));
        } else if (c.equals(double[].class)) {
            return new DoubleArrayAccessor(doubleNullArray(size));
        } else if (c.equals(float[].class)) {
            return new FloatArrayAccessor(floatNullArray(size));
        } else if (c.equals(int[].class)) {
            return new IntArrayAccessor(intNullArray(size));
        } else if (c.equals(long[].class)) {
            return new LongArrayAccessor(longNullArray(size));
        } else if (c.equals(short[].class)) {
            return new ShortArrayAccessor(shortNullArray(size));
        } else {
            return new ObjectArrayAccessor((Object[]) Array.newInstance(c.getComponentType(), size));
        }
    }

    public static Object toArray(Collection<?> objects) {
        if (objects.size() == 0) {
            return toArray(objects, Object.class);
        }
        Object prototype = objects.iterator().next();
        if (prototype == null) {
            return toArray(objects, Object.class);
        }
        Class ubType = TypeUtils.getUnboxedType(prototype.getClass());

        return toArray(objects, (ubType == null ? prototype.getClass() : ubType));
    }

    public static ArrayAccessor getAccessorForElementType(Class componentType, int size) {
        if (componentType.equals(boolean.class) || componentType.equals(Boolean.class)) {
            return new BooleanArrayAccessor(booleanNullArray(size));
        } else if (componentType.equals(byte.class) || componentType.equals(Byte.class)) {
            return new ByteArrayAccessor(byteNullArray(size));
        } else if (componentType.equals(char.class) || componentType.equals(Character.class)) {
            return new CharArrayAccessor(charNullArray(size));
        } else if (componentType.equals(double.class) || componentType.equals(Double.class)) {
            return new DoubleArrayAccessor(doubleNullArray(size));
        } else if (componentType.equals(float.class) || componentType.equals(Float.class)) {
            return new FloatArrayAccessor(floatNullArray(size));
        } else if (componentType.equals(int.class) || componentType.equals(Integer.class)) {
            return new IntArrayAccessor(intNullArray(size));
        } else if (componentType.equals(long.class) || componentType.equals(Long.class)) {
            return new LongArrayAccessor(longNullArray(size));
        } else if (componentType.equals(short.class) || componentType.equals(Short.class)) {
            return new ShortArrayAccessor(shortNullArray(size));
        } else {
            return new ObjectArrayAccessor((Object[]) Array.newInstance(componentType, size));
        }

    }

    public static Character[] getBoxedArray(char[] referenceData) {
        Character[] result = new Character[referenceData.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (referenceData[i] == NULL_CHAR ? null : referenceData[i]);
        }
        return result;
    }

    public static Boolean[] getBoxedArray(boolean[] referenceData) {
        Boolean[] result = new Boolean[referenceData.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = referenceData[i];
        }
        return result;
    }

    public static Integer[] getBoxedArray(int[] referenceData) {
        Integer[] result = new Integer[referenceData.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (referenceData[i] == NULL_INT ? null : referenceData[i]);
        }
        return result;
    }

    public static Byte[] getBoxedArray(byte[] referenceData) {
        Byte[] result = new Byte[referenceData.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (referenceData[i] == NULL_BYTE ? null : referenceData[i]);
        }
        return result;
    }

    public static Double[] getBoxedArray(double[] referenceData) {
        Double[] result = new Double[referenceData.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (referenceData[i] == NULL_DOUBLE ? null : referenceData[i]);
        }
        return result;
    }

    public static Float[] getBoxedArray(float[] referenceData) {
        Float[] result = new Float[referenceData.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (referenceData[i] == NULL_FLOAT ? null : referenceData[i]);
        }
        return result;
    }

    public static Long[] getBoxedArray(long[] referenceData) {
        Long[] result = new Long[referenceData.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (referenceData[i] == NULL_LONG ? null : referenceData[i]);
        }
        return result;
    }

    public static Short[] getBoxedArray(short[] referenceData) {
        Short[] result = new Short[referenceData.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (referenceData[i] == NULL_SHORT ? null : referenceData[i]);
        }
        return result;
    }

    public static Object getUnboxedArray(Object value) {
        if (value == null) {
            return null;
        }
        final Class<?> c = value.getClass();
        if (c.equals(Byte[].class)) {
            return getUnboxedArray((Byte[]) value);
        } else if (c.equals(Character[].class)) {
            return getUnboxedArray((Character[]) value);
        } else if (c.equals(Double[].class)) {
            return getUnboxedArray((Double[]) value);
        } else if (c.equals(Float[].class)) {
            return getUnboxedArray((Float[]) value);
        } else if (c.equals(Integer[].class)) {
            return getUnboxedArray((Integer[]) value);
        } else if (c.equals(Long[].class)) {
            return getUnboxedArray((Long[]) value);
        } else if (c.equals(Short[].class)) {
            return getUnboxedArray((Short[]) value);
        }
        return value;

    }

    public static byte[] getUnboxedArray(Byte[] boxedArray) {
        final byte[] result = new byte[boxedArray.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (boxedArray[i] != null ? boxedArray[i] : NULL_BYTE);
        }
        return result;
    }

    public static char[] getUnboxedArray(Character[] boxedArray) {
        final char[] result = new char[boxedArray.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (boxedArray[i] != null ? boxedArray[i] : NULL_CHAR);
        }
        return result;
    }

    public static double[] getUnboxedArray(Double[] boxedArray) {
        final double[] result = new double[boxedArray.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (boxedArray[i] != null ? boxedArray[i] : NULL_DOUBLE);
        }
        return result;
    }

    public static float[] getUnboxedArray(Float[] boxedArray) {
        final float[] result = new float[boxedArray.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (boxedArray[i] != null ? boxedArray[i] : NULL_FLOAT);
        }
        return result;
    }

    public static int[] getUnboxedArray(Integer[] boxedArray) {
        final int[] result = new int[boxedArray.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (boxedArray[i] != null ? boxedArray[i] : NULL_INT);
        }
        return result;
    }

    public static long[] getUnboxedArray(Long[] boxedArray) {
        final long[] result = new long[boxedArray.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (boxedArray[i] != null ? boxedArray[i] : NULL_LONG);
        }
        return result;
    }

    public static short[] getUnboxedArray(Short[] boxedArray) {
        final short[] result = new short[boxedArray.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (boxedArray[i] != null ? boxedArray[i] : NULL_SHORT);
        }
        return result;
    }

    public static byte[] getUnboxedByteArray(Object[] boxedArray) {
        final byte[] result = new byte[boxedArray.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (boxedArray[i] != null ? (((Number) boxedArray[i]).byteValue()) : NULL_BYTE);
        }
        return result;
    }

    public static char[] getUnboxedCharArray(Object[] boxedArray) {
        final char[] result = new char[boxedArray.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (boxedArray[i] != null ? ((char) boxedArray[i]) : NULL_CHAR);
        }
        return result;
    }

    public static short[] getUnboxedShortArray(Object[] boxedArray) {
        final short[] result = new short[boxedArray.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (boxedArray[i] != null ? (((Number) boxedArray[i]).shortValue()) : NULL_SHORT);
        }
        return result;
    }

    public static int[] getUnboxedIntArray(Object[] boxedArray) {
        final int[] result = new int[boxedArray.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (boxedArray[i] != null ? (((Number) boxedArray[i]).intValue()) : NULL_INT);
        }
        return result;
    }

    public static long[] getUnboxedLongArray(Object[] boxedArray) {
        final long[] result = new long[boxedArray.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (boxedArray[i] != null ? ((Number) boxedArray[i]).longValue() : NULL_LONG);
        }
        return result;
    }

    public static float[] getUnboxedFloatArray(Object[] boxedArray) {
        final float[] result = new float[boxedArray.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (boxedArray[i] != null ? ((Number) boxedArray[i]).floatValue() : NULL_FLOAT);
        }
        return result;
    }

    public static double[] getUnboxedDoubleArray(Object[] boxedArray) {
        final double[] result = new double[boxedArray.length];
        for (int i = 0; i < result.length; i++) {
            result[i] = (boxedArray[i] != null ? ((Number) boxedArray[i]).doubleValue() : NULL_DOUBLE);
        }
        return result;
    }

    public static Object[] getBoxedArray(Object value) {
        if (value == null) {
            return null;
        }

        final Class<?> c = value.getClass();
        if (c.equals(boolean[].class)) {
            return getBoxedArray((boolean[]) value);
        } else if (c.equals(byte[].class)) {
            return getBoxedArray((byte[]) value);
        } else if (c.equals(char[].class)) {
            return getBoxedArray((char[]) value);
        } else if (c.equals(double[].class)) {
            return getBoxedArray((double[]) value);
        } else if (c.equals(float[].class)) {
            return getBoxedArray((float[]) value);
        } else if (c.equals(int[].class)) {
            return getBoxedArray((int[]) value);
        } else if (c.equals(long[].class)) {
            return getBoxedArray((long[]) value);
        } else if (c.equals(short[].class)) {
            return getBoxedArray((short[]) value);
        } else {
            return (Object[]) value;
        }
    }

    public static <T> T[] nullSafeDbArrayToArray(final DbArray<T> dbArray) {
        return dbArray == null ? null : dbArray.toArray();
    }

    public static Boolean[] nullSafeDbArrayToArray(final DbBooleanArray dbArray) {
        return dbArray == null ? null : dbArray.toArray();
    }

    public static byte[] nullSafeDbArrayToArray(final DbByteArray dbArray) {
        return dbArray == null ? null : dbArray.toArray();
    }

    public static char[] nullSafeDbArrayToArray(final DbCharArray dbArray) {
        return dbArray == null ? null : dbArray.toArray();
    }

    public static double[] nullSafeDbArrayToArray(final DbDoubleArray dbArray) {
        return dbArray == null ? null : dbArray.toArray();
    }

    public static float[] nullSafeDbArrayToArray(final DbFloatArray dbArray) {
        return dbArray == null ? null : dbArray.toArray();
    }

    public static int[] nullSafeDbArrayToArray(final DbIntArray dbArray) {
        return dbArray == null ? null : dbArray.toArray();
    }

    public static long[] nullSafeDbArrayToArray(final DbLongArray dbArray) {
        return dbArray == null ? null : dbArray.toArray();
    }

    public static short[] nullSafeDbArrayToArray(final DbShortArray dbArray) {
        return dbArray == null ? null : dbArray.toArray();
    }

    public static boolean equals(Object actualValue, Object expectedValue) {
        final Class<?> ct = actualValue.getClass().getComponentType();
        if (Object.class.isAssignableFrom(ct)) {
            return Arrays.equals((Object[]) actualValue, (Object[]) expectedValue);
        } else if (byte.class.isAssignableFrom(ct)) {
            return Arrays.equals((byte[]) actualValue, (byte[]) expectedValue);
        } else if (char.class.isAssignableFrom(ct)) {
            return Arrays.equals((char[]) actualValue, (char[]) expectedValue);
        } else if (double.class.isAssignableFrom(ct)) {
            return Arrays.equals((double[]) actualValue, (double[]) expectedValue);
        } else if (float.class.isAssignableFrom(ct)) {
            return Arrays.equals((float[]) actualValue, (float[]) expectedValue);
        } else if (int.class.isAssignableFrom(ct)) {
            return Arrays.equals((int[]) actualValue, (int[]) expectedValue);
        } else if (long.class.isAssignableFrom(ct)) {
            return Arrays.equals((long[]) actualValue, (long[]) expectedValue);
        } else if (short.class.isAssignableFrom(ct)) {
            return Arrays.equals((short[]) actualValue, (short[]) expectedValue);
        }
        return false;
    }

    public static String toString(Object actualValue) {
        final Class<?> ct = actualValue.getClass().getComponentType();
        if (Object.class.isAssignableFrom(ct)) {
            return Arrays.toString((Object[]) actualValue);
        } else if (byte.class.isAssignableFrom(ct)) {
            return Arrays.toString((byte[]) actualValue);
        } else if (char.class.isAssignableFrom(ct)) {
            return Arrays.toString((char[]) actualValue);
        } else if (double.class.isAssignableFrom(ct)) {
            return Arrays.toString((double[]) actualValue);
        } else if (float.class.isAssignableFrom(ct)) {
            return Arrays.toString((float[]) actualValue);
        } else if (int.class.isAssignableFrom(ct)) {
            return Arrays.toString((int[]) actualValue);
        } else if (long.class.isAssignableFrom(ct)) {
            return Arrays.toString((long[]) actualValue);
        } else if (short.class.isAssignableFrom(ct)) {
            return Arrays.toString((short[]) actualValue);
        }
        return null;
    }

    public static String toString(boolean[] a, int offset, int length) {
        if (a == null)
            return "(null)";

        int iMax = offset + length - 1;
        if (iMax < 0)
            return "[]";

        final StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = offset;; i++) {
            b.append(a[i]);
            if (i == iMax)
                return b.append(']').toString();
            b.append(", ");
        }
    }

    public static String toString(byte[] a, int offset, int length) {
        if (a == null)
            return "(null)";

        int iMax = offset + length - 1;
        if (iMax < 0)
            return "[]";

        final StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = offset;; i++) {
            b.append(a[i] == NULL_BYTE ? "(null)" : a[i]);
            if (i == iMax)
                return b.append(']').toString();
            b.append(", ");
        }
    }

    public static String toString(char[] a, int offset, int length) {
        if (a == null)
            return "(null)";

        int iMax = offset + length - 1;
        if (iMax < 0)
            return "[]";

        final StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = offset;; i++) {
            b.append(a[i] == NULL_CHAR ? "(null)" : a[i]);
            if (i == iMax)
                return b.append(']').toString();
            b.append(", ");
        }
    }

    public static String toString(short[] a, int offset, int length) {
        if (a == null)
            return "(null)";

        int iMax = offset + length - 1;
        if (iMax < 0)
            return "[]";

        final StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = offset;; i++) {
            b.append(a[i] == NULL_SHORT ? "(null)" : a[i]);
            if (i == iMax)
                return b.append(']').toString();
            b.append(", ");
        }
    }

    public static String toString(int[] a, int offset, int length) {
        if (a == null)
            return "(null)";

        int iMax = offset + length - 1;
        if (iMax < 0)
            return "[]";

        final StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = offset;; i++) {
            b.append(a[i] == NULL_INT ? "(null)" : a[i]);
            if (i == iMax)
                return b.append(']').toString();
            b.append(", ");
        }
    }

    public static String toString(long[] a, int offset, int length) {
        if (a == null)
            return "(null)";

        int iMax = offset + length - 1;
        if (iMax < 0)
            return "[]";

        final StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = offset;; i++) {
            b.append(a[i] == NULL_LONG ? "(null)" : a[i]);
            if (i == iMax)
                return b.append(']').toString();
            b.append(", ");
        }
    }

    public static String toString(float[] a, int offset, int length) {
        if (a == null)
            return "(null)";

        int iMax = offset + length - 1;
        if (iMax < 0)
            return "[]";

        final StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = offset;; i++) {
            b.append(a[i] == NULL_FLOAT ? "(null)" : a[i]);
            if (i == iMax)
                return b.append(']').toString();
            b.append(", ");
        }
    }

    public static String toString(double[] a, int offset, int length) {
        if (a == null)
            return "(null)";

        int iMax = offset + length - 1;
        if (iMax < 0)
            return "[]";

        final StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = offset;; i++) {
            b.append(a[i] == NULL_DOUBLE ? "(null)" : a[i]);
            if (i == iMax)
                return b.append(']').toString();
            b.append(", ");
        }
    }

    public static String toString(Object[] a, int offset, int length) {
        if (a == null)
            return "(null)";

        int iMax = offset + length - 1;
        if (iMax < 0)
            return "[]";

        final StringBuilder b = new StringBuilder();
        b.append('[');
        for (int i = offset;; i++) {
            if (a[i] == null) {
                b.append("(null)");
            } else {
                b.append('\'').append(a[i]).append('\'');
            }
            if (i == iMax)
                return b.append(']').toString();
            b.append(", ");
        }
    }

    public interface ArrayAccessor<T> {
        void set(int index, T value);

        T get(int index);

        int length();

        Object getArray();

        void fill(int from, int to, T value);

        void copyArray(Object sourceArray, int pos, int length);
    }

    public static class ObjectArrayAccessor<T> implements ArrayAccessor<T> {

        private T array[];


        public ObjectArrayAccessor(T array[]) {
            this.array = array;
        }

        @Override
        public void set(int index, T value) {
            array[index] = value;
        }

        @Override
        public T get(int index) {
            return array[index];
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Object getArray() {
            return array;
        }

        @Override
        public void fill(int from, int to, T value) {
            Arrays.fill(array, from, to, value);
        }

        @Override
        public void copyArray(Object sourceArray, int pos, int length) {
            if (sourceArray == null) {
                throw new NullPointerException();
            }
            System.arraycopy(sourceArray, 0, array, pos, length);
        }
    }

    public static class BooleanArrayAccessor implements ArrayAccessor<Boolean> {

        private Boolean array[];

        public BooleanArrayAccessor(Boolean array[]) {
            this.array = array;
        }

        @Override
        public void set(int index, Boolean value) {
            array[index] = (value == null ? NULL_BOOLEAN : value);
        }

        @Override
        public Boolean get(int index) {
            return array[index];
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Object getArray() {
            return array;
        }

        @Override
        public void fill(int from, int to, Boolean value) {
            if (value == null) {
                Arrays.fill(array, from, to, NULL_BOOLEAN);
            } else {
                Arrays.fill(array, from, to, value);
            }
        }

        @Override
        public void copyArray(Object sourceArray, int pos, int length) {
            System.arraycopy(sourceArray, 0, array, pos, length);
        }

    }

    public static class ByteArrayAccessor implements ArrayAccessor<Byte> {

        private byte array[];

        public ByteArrayAccessor(byte array[]) {
            this.array = array;
        }

        @Override
        public void set(int index, Byte value) {
            array[index] = (value == null ? NULL_BYTE : value);
        }

        @Override
        public Byte get(int index) {
            return array[index];
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Object getArray() {
            return array;
        }

        @Override
        public void fill(int from, int to, Byte value) {
            if (value == null) {
                Arrays.fill(array, from, to, NULL_BYTE);
            } else {
                Arrays.fill(array, from, to, value);
            }
        }

        @Override
        public void copyArray(Object sourceArray, int pos, int length) {
            System.arraycopy(sourceArray, 0, array, pos, length);
        }

    }
    public static class CharArrayAccessor implements ArrayAccessor<Character> {

        private char array[];

        public CharArrayAccessor(char array[]) {
            this.array = array;
        }

        @Override
        public void set(int index, Character value) {
            array[index] = (value == null ? NULL_CHAR : value);
        }

        @Override
        public Character get(int index) {
            return array[index];
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Object getArray() {
            return array;
        }

        @Override
        public void fill(int from, int to, Character value) {
            if (value == null) {
                Arrays.fill(array, from, to, NULL_CHAR);
            } else {
                Arrays.fill(array, from, to, value);
            }
        }

        @Override
        public void copyArray(Object sourceArray, int pos, int length) {
            System.arraycopy(sourceArray, 0, array, pos, length);
        }

    }
    public static class DoubleArrayAccessor implements ArrayAccessor<Double> {

        private double array[];

        public DoubleArrayAccessor(double array[]) {
            this.array = array;
        }

        @Override
        public void set(int index, Double value) {
            array[index] = (value == null ? NULL_DOUBLE : value);
        }

        @Override
        public Double get(int index) {
            return array[index];
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Object getArray() {
            return array;
        }

        @Override
        public void fill(int from, int to, Double value) {
            if (value == null) {
                Arrays.fill(array, from, to, NULL_DOUBLE);
            } else {
                Arrays.fill(array, from, to, value);
            }
        }

        @Override
        public void copyArray(Object sourceArray, int pos, int length) {
            System.arraycopy(sourceArray, 0, array, pos, length);
        }

    }
    public static class FloatArrayAccessor implements ArrayAccessor<Float> {

        private float array[];

        public FloatArrayAccessor(float array[]) {
            this.array = array;
        }

        @Override
        public void set(int index, Float value) {
            array[index] = (value == null ? NULL_FLOAT : value);
        }

        @Override
        public Float get(int index) {
            return array[index];
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Object getArray() {
            return array;
        }

        @Override
        public void fill(int from, int to, Float value) {
            if (value == null) {
                Arrays.fill(array, from, to, NULL_FLOAT);
            } else {
                Arrays.fill(array, from, to, value);
            }
        }

        @Override
        public void copyArray(Object sourceArray, int pos, int length) {
            System.arraycopy(sourceArray, 0, array, pos, length);
        }

    }
    public static class IntArrayAccessor implements ArrayAccessor<Integer> {

        private int array[];

        public IntArrayAccessor(int array[]) {
            this.array = array;
        }

        @Override
        public void set(int index, Integer value) {
            array[index] = (value == null ? NULL_INT : value);
        }

        @Override
        public Integer get(int index) {
            return array[index];
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Object getArray() {
            return array;
        }

        @Override
        public void fill(int from, int to, Integer value) {
            if (value == null) {
                Arrays.fill(array, from, to, NULL_INT);
            } else {
                Arrays.fill(array, from, to, value);
            }
        }

        @Override
        public void copyArray(Object sourceArray, int pos, int length) {
            if (sourceArray == null) {
                throw new NullPointerException();
            }
            Require.requirement(sourceArray instanceof int[], "sourceArray instanceof int[]", sourceArray.getClass(),
                    "sourceArray.getClass()");
            System.arraycopy(sourceArray, 0, array, pos, length);
        }

    }
    public static class LongArrayAccessor implements ArrayAccessor<Long> {

        private long array[];

        public LongArrayAccessor(long array[]) {
            this.array = array;
        }

        @Override
        public void set(int index, Long value) {
            array[index] = (value == null ? NULL_LONG : value);
        }

        @Override
        public Long get(int index) {
            return array[index];
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Object getArray() {
            return array;
        }

        @Override
        public void fill(int from, int to, Long value) {
            if (value == null) {
                Arrays.fill(array, from, to, NULL_LONG);
            } else {
                Arrays.fill(array, from, to, value);
            }
        }

        @Override
        public void copyArray(Object sourceArray, int pos, int length) {
            System.arraycopy(sourceArray, 0, array, pos, length);
        }


    }
    public static class ShortArrayAccessor implements ArrayAccessor<Short> {

        private short array[];

        public ShortArrayAccessor(short array[]) {
            this.array = array;
        }

        @Override
        public void set(int index, Short value) {
            array[index] = (value == null ? NULL_SHORT : value);
        }

        @Override
        public Short get(int index) {
            return array[index];
        }

        @Override
        public int length() {
            return array.length;
        }

        @Override
        public Object getArray() {
            return array;
        }

        @Override
        public void fill(int from, int to, Short value) {
            if (value == null) {
                Arrays.fill(array, from, to, NULL_SHORT);
            } else {
                Arrays.fill(array, from, to, value);
            }
        }

        @Override
        public void copyArray(Object sourceArray, int pos, int length) {
            System.arraycopy(sourceArray, 0, array, pos, length);
        }

    }

}

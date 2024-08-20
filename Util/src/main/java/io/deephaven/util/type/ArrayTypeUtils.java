//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.type;

import io.deephaven.base.verify.Require;

import java.util.Arrays;

import static io.deephaven.util.QueryConstants.*;

/**
 * Common utilities for interacting generically with arrays.
 */
public class ArrayTypeUtils {

    public static final boolean[] EMPTY_BOOLEAN_ARRAY = new boolean[0];
    public static final char[] EMPTY_CHAR_ARRAY = new char[0];
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];
    public static final short[] EMPTY_SHORT_ARRAY = new short[0];
    public static final int[] EMPTY_INT_ARRAY = new int[0];
    public static final long[] EMPTY_LONG_ARRAY = new long[0];
    public static final float[] EMPTY_FLOAT_ARRAY = new float[0];
    public static final double[] EMPTY_DOUBLE_ARRAY = new double[0];
    public static final Object[] EMPTY_OBJECT_ARRAY = new Object[0];
    public static final String[] EMPTY_STRING_ARRAY = new String[0];
    public static final String[][] EMPTY_STRING_ARRAY_ARRAY = new String[0][];
    public static final Boolean[] EMPTY_BOOLEANBOXED_ARRAY = new Boolean[0];

    public static ArrayAccessor<?> getArrayAccessor(Object array) {
        if (array instanceof Boolean[]) {
            return new BooleanArrayAccessor((Boolean[]) array);
        } else if (array instanceof byte[]) {
            return new ByteArrayAccessor((byte[]) array);
        } else if (array instanceof char[]) {
            return new CharArrayAccessor((char[]) array);
        } else if (array instanceof double[]) {
            return new DoubleArrayAccessor((double[]) array);
        } else if (array instanceof float[]) {
            return new FloatArrayAccessor((float[]) array);
        } else if (array instanceof int[]) {
            return new IntArrayAccessor((int[]) array);
        } else if (array instanceof long[]) {
            return new LongArrayAccessor((long[]) array);
        } else if (array instanceof short[]) {
            return new ShortArrayAccessor((short[]) array);
        } else {
            return new ObjectArrayAccessor<>((Object[]) array);
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

        if (value instanceof boolean[]) {
            return getBoxedArray((boolean[]) value);
        } else if (value instanceof byte[]) {
            return getBoxedArray((byte[]) value);
        } else if (value instanceof char[]) {
            return getBoxedArray((char[]) value);
        } else if (value instanceof double[]) {
            return getBoxedArray((double[]) value);
        } else if (value instanceof float[]) {
            return getBoxedArray((float[]) value);
        } else if (value instanceof int[]) {
            return getBoxedArray((int[]) value);
        } else if (value instanceof long[]) {
            return getBoxedArray((long[]) value);
        } else if (value instanceof short[]) {
            return getBoxedArray((short[]) value);
        } else {
            return (Object[]) value;
        }
    }

    public static boolean equals(Object actualValue, Object expectedValue) {
        if (actualValue instanceof byte[] && expectedValue instanceof byte[]) {
            return Arrays.equals((byte[]) actualValue, (byte[]) expectedValue);
        } else if (actualValue instanceof char[] && expectedValue instanceof char[]) {
            return Arrays.equals((char[]) actualValue, (char[]) expectedValue);
        } else if (actualValue instanceof double[] && expectedValue instanceof double[]) {
            return Arrays.equals((double[]) actualValue, (double[]) expectedValue);
        } else if (actualValue instanceof float[] && expectedValue instanceof float[]) {
            return Arrays.equals((float[]) actualValue, (float[]) expectedValue);
        } else if (actualValue instanceof int[] && expectedValue instanceof int[]) {
            return Arrays.equals((int[]) actualValue, (int[]) expectedValue);
        } else if (actualValue instanceof long[] && expectedValue instanceof long[]) {
            return Arrays.equals((long[]) actualValue, (long[]) expectedValue);
        } else if (actualValue instanceof short[] && expectedValue instanceof short[]) {
            return Arrays.equals((short[]) actualValue, (short[]) expectedValue);
        } else {
            return Arrays.equals((Object[]) actualValue, (Object[]) expectedValue);
        }
    }

    public static String toString(Object actualValue) {
        if (actualValue instanceof byte[]) {
            return Arrays.toString((byte[]) actualValue);
        } else if (actualValue instanceof char[]) {
            return Arrays.toString((char[]) actualValue);
        } else if (actualValue instanceof double[]) {
            return Arrays.toString((double[]) actualValue);
        } else if (actualValue instanceof float[]) {
            return Arrays.toString((float[]) actualValue);
        } else if (actualValue instanceof int[]) {
            return Arrays.toString((int[]) actualValue);
        } else if (actualValue instanceof long[]) {
            return Arrays.toString((long[]) actualValue);
        } else if (actualValue instanceof short[]) {
            return Arrays.toString((short[]) actualValue);
        }
        return Arrays.toString((Object[]) actualValue);
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

        private final T[] array;


        public ObjectArrayAccessor(T[] array) {
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

        private final Boolean[] array;

        public BooleanArrayAccessor(Boolean[] array) {
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

        private final byte[] array;

        public ByteArrayAccessor(byte[] array) {
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

        private final char[] array;

        public CharArrayAccessor(char[] array) {
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

        private final double[] array;

        public DoubleArrayAccessor(double[] array) {
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

        private final float[] array;

        public FloatArrayAccessor(float[] array) {
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

        private final int[] array;

        public IntArrayAccessor(int[] array) {
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

        private final long[] array;

        public LongArrayAccessor(long[] array) {
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

        private final short[] array;

        public ShortArrayAccessor(short[] array) {
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

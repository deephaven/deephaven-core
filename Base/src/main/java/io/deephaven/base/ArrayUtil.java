/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base;

import io.deephaven.base.verify.Require;

import java.util.*;

public class ArrayUtil {

    public static int[] pushArray(int e, int[] a) {
        if (a == null) {
            a = new int[] {e};
        } else {
            int[] new_a = new int[a.length + 1];
            System.arraycopy(a, 0, new_a, 0, a.length);
            new_a[a.length] = e;
            a = new_a;
        }
        return a;
    }

    public static double[] pushArray(double e, double[] a) {
        if (a == null) {
            a = new double[] {e};
        } else {
            double[] new_a = new double[a.length + 1];
            System.arraycopy(a, 0, new_a, 0, a.length);
            new_a[a.length] = e;
            a = new_a;
        }
        return a;
    }

    public static <T> T[] pushArray(T e, T[] a, Class<T> c) {
        if (a == null) {
            a = (T[]) java.lang.reflect.Array.newInstance(c, 1);
            a[0] = e;
        } else {
            T[] new_a = (T[]) java.lang.reflect.Array.newInstance(c, a.length + 1);
            System.arraycopy(a, 0, new_a, 0, a.length);
            new_a[a.length] = e;
            a = new_a;
        }
        return a;
    }

    public static <T> T[] deleteArrayPos(int i, T[] a) {
        assert a != null && i < a.length;
        T[] new_a = null;
        if (a.length > 1) {
            new_a = (T[]) java.lang.reflect.Array.newInstance(a.getClass().getComponentType(),
                a.length - 1);
            System.arraycopy(a, 0, new_a, 0, i);
            System.arraycopy(a, i + 1, new_a, i, a.length - (i + 1));
        }
        return new_a;
    }

    public static <T> T[] deleteArrayRefValueIfPresent(final T vref, final T[] a) {
        assert a != null;
        for (int i = 0; i < a.length; ++i) {
            if (a[i] == vref) {
                return deleteArrayPos(i, a);
            }
        }
        return a;
    }

    public static double[] deleteArrayPos(int i, double[] a) {
        assert a != null && i < a.length;
        double[] new_a = null;
        if (a.length > 1) {
            new_a = new double[a.length - 1];
            System.arraycopy(a, 0, new_a, 0, i);
            System.arraycopy(a, i + 1, new_a, i, a.length - (i + 1));
        }
        return new_a;
    }

    public static int[] deleteArrayPos(int i, int[] a) {
        assert a != null && i < a.length;
        int[] new_a = null;
        if (a.length > 1) {
            new_a = new int[a.length - 1];
            System.arraycopy(a, 0, new_a, 0, i);
            System.arraycopy(a, i + 1, new_a, i, a.length - (i + 1));
        }
        return new_a;
    }

    public static int[] addToArray(int e, int[] a) {
        if (a != null) {
            for (int i = 0; i < a.length; ++i) {
                if (a[i] == e) {
                    a[i] = e;
                    return a;
                }
            }
        }
        return pushArray(e, a);
    }

    public static <T> T[] addToArray(T e, T[] a, Class<T> c) {
        if (a != null) {
            for (int i = 0; i < a.length; ++i) {
                if (a[i].equals(e)) {
                    a[i] = e;
                    return a;
                }
            }
        }
        return pushArray(e, a, c);
    }

    public static <T> T[] addExactToArray(T e, T[] a, Class<T> c) {
        if (a != null) {
            for (int i = 0; i < a.length; ++i) {
                if (a[i] == e) {
                    return a;
                }
            }
        }
        return pushArray(e, a, c);
    }

    public static <T> T[] removeFromArray(T e, T[] a) {
        if (a != null) {
            for (int i = 0; i < a.length; ++i) {
                if (a[i].equals(e)) {
                    return deleteArrayPos(i, a);
                }
            }
        }
        return a;
    }

    public static <T> T[] removeExactFromArray(T e, T[] a) {
        if (a != null) {
            for (int i = 0; i < a.length; ++i) {
                if (a[i] == e) {
                    return deleteArrayPos(i, a);
                }
            }
        }
        return a;
    }

    public static int[] removeFromArray(int e, int[] a) {
        if (a != null) {
            for (int i = 0; i < a.length; ++i) {
                if (a[i] == e) {
                    return deleteArrayPos(i, a);
                }
            }
        }
        return a;
    }

    public static double[] removeFromArray(double e, double[] a) {
        if (a != null) {
            for (int i = 0; i < a.length; ++i) {
                if (a[i] == e) {
                    return deleteArrayPos(i, a);
                }
            }
        }
        return a;
    }

    public static <T> boolean replaceInArray(T e, T[] a) {
        if (a != null) {
            for (int i = 0; i < a.length; ++i) {
                if (a[i].equals(e)) {
                    a[i] = e;
                    return true;
                }
            }
        }
        return false;
    }

    public static <T> T[] addUnless(T[] a, Class<T> c, Predicate.Unary<T> pred,
        Function.Nullary<T> factory) {
        if (a != null) {
            for (int i = 0; i < a.length; ++i) {
                if (pred.call(a[i])) {
                    return a;
                }
            }
        }
        return pushArray(factory.call(), a, c);
    }

    public static <T, A> T[] addUnless(T[] a, Class<T> c, Predicate.Binary<T, A> pred,
        Function.Unary<T, A> factory, A arg) {
        if (a != null) {
            for (int i = 0; i < a.length; ++i) {
                if (pred.call(a[i], arg)) {
                    return a;
                }
            }
        }
        return pushArray(factory.call(arg), a, c);
    }

    public static <T, A> T[] replaceOrAdd(T[] a, Class<T> c, Predicate.Binary<T, A> pred,
        Function.Unary<T, A> factory, A arg) {
        if (a != null) {
            for (int i = 0; i < a.length; ++i) {
                if (pred.call(a[i], arg)) {
                    a[i] = factory.call(arg);
                    return a;
                }
            }
        }
        return pushArray(factory.call(arg), a, c);
    }

    public static <T> T[] removeIf(T[] a, Predicate.Unary<T> pred) {
        if (a != null) {
            for (int i = 0; i < a.length; ++i) {
                if (pred.call(a[i])) {
                    return deleteArrayPos(i, a);
                }
            }
        }
        return a;
    }

    public static <T, A> T[] removeIf(T[] a, Predicate.Binary<T, A> pred, A arg) {
        if (a != null) {
            for (int i = 0; i < a.length; ++i) {
                if (pred.call(a[i], arg)) {
                    return deleteArrayPos(i, a);
                }
            }
        }
        return a;
    }

    public static <T, A, B> T[] removeIf(T[] a, Predicate.Ternary<T, A, B> pred, A arg1, B arg2) {
        if (a != null) {
            for (int i = 0; i < a.length; ++i) {
                if (pred.call(a[i], arg1, arg2)) {
                    return deleteArrayPos(i, a);
                }
            }
        }
        return a;
    }

    public static <T> boolean contains(T[] a, T value) {
        if (a == null) {
            return false;
        }
        if (value == null) {
            for (T v : a)
                if (v == null)
                    return true;
        } else {
            for (T v : a)
                if (value.equals(v))
                    return true;
        }

        return false;
    }

    public static <T> boolean containsExact(T[] a, T value) {
        if (a == null) {
            return false;
        }
        if (value == null) {
            for (T v : a)
                if (v == null)
                    return true;
        } else {
            for (T v : a)
                if (v == value)
                    return true;
        }

        return false;
    }

    public static boolean contains(int[] a, int value) {
        if (a == null) {
            return false;
        }
        for (int v : a) {
            if (value == v) {
                return true;
            }
        }
        return false;
    }

    public static boolean contains(long[] a, long value) {
        if (a == null) {
            return false;
        }
        for (long v : a) {
            if (value == v) {
                return true;
            }
        }
        return false;
    }

    public static <T> T find(T[] a, Predicate.Unary<T> predicate) {
        if (a == null) {
            return null;
        }
        for (T v : a) {
            if (predicate.call(v)) {
                return v;
            }
        }
        return null;
    }

    public static int find(int[] a, Predicate.Int predicate) {
        return find(a, predicate, Integer.MIN_VALUE);
    }

    public static int find(int[] a, Predicate.Int predicate, int notFound) {
        if (a == null) {
            return notFound;
        }
        for (int v : a) {
            if (predicate.call(v)) {
                return v;
            }
        }
        return notFound;
    }

    public static boolean any(int[] a, Predicate.Int predicate) {
        if (a == null) {
            return false;
        }
        for (int v : a) {
            if (predicate.call(v)) {
                return true;
            }
        }
        return false;
    }

    public static <T> int indexOf(T[] a, T value) {
        if (value == null) {
            for (int i = 0; i < a.length; i++)
                if (a[i] == null)
                    return i;
        } else {
            for (int i = 0; i < a.length; i++)
                if (value.equals(a[i]))
                    return i;
        }

        return -1;
    }

    public static int indexOf(int[] a, int value) {
        for (int i = 0; i < a.length; i++) {
            if (value == a[i]) {
                return i;
            }
        }
        return -1;
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] extend(T[] a, int i, Class<? extends T> c) {
        T[] new_a = (T[]) java.lang.reflect.Array.newInstance(c, Math.max(a.length * 2, i + 1));
        System.arraycopy(a, 0, new_a, 0, a.length);
        return new_a;
    }

    public static int[] merge(final int[] ss1, final int[] ss2) {
        final int[] ans = new int[ss1.length + ss2.length];
        System.arraycopy(ss1, 0, ans, 0, ss1.length);
        System.arraycopy(ss2, 0, ans, ss1.length, ss2.length);
        return ans;
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] merge(final T[] ss1, final T[] ss2, Class<T> c) {
        T[] ans = (T[]) java.lang.reflect.Array.newInstance(c, ss1.length + ss2.length);
        System.arraycopy(ss1, 0, ans, 0, ss1.length);
        System.arraycopy(ss2, 0, ans, ss1.length, ss2.length);
        return ans;
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] merge(Class<T> c, final T[]... ss) {
        int tot = 0;
        for (T[] s : ss) {
            tot += s.length;
        }
        T[] ans = (T[]) java.lang.reflect.Array.newInstance(c, tot);
        int end = 0;
        for (T[] s : ss) {
            System.arraycopy(s, 0, ans, end, s.length);
            end += s.length;
        }
        return ans;
    }

    public static byte[] extend(byte[] a, int i) {
        byte[] new_a = new byte[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        return new_a;
    }

    public static byte[] extend(byte[] a, int i, byte fill) {
        byte[] new_a = new byte[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        Arrays.fill(new_a, a.length, new_a.length, fill);
        return new_a;
    }

    public static char[] extend(char[] a, int i) {
        char[] new_a = new char[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        return new_a;
    }

    public static char[] extend(char[] a, int i, char fill) {
        char[] new_a = new char[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        Arrays.fill(new_a, a.length, new_a.length, fill);
        return new_a;
    }

    public static short[] extend(short[] a, int i) {
        short[] new_a = new short[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        return new_a;
    }

    public static short[] extend(short[] a, int i, short fill) {
        short[] new_a = new short[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        Arrays.fill(new_a, a.length, new_a.length, fill);
        return new_a;
    }

    public static int[] extend(int[] a, int i) {
        int[] new_a = new int[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        return new_a;
    }

    public static int[] extend(int[] a, int i, int fill) {
        int[] new_a = new int[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        Arrays.fill(new_a, a.length, new_a.length, fill);
        return new_a;
    }

    public static long[] extend(long[] a, int i) {
        long[] new_a = new long[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        return new_a;
    }

    public static long[] extend(long[] a, int i, long fill) {
        long[] new_a = new long[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        Arrays.fill(new_a, a.length, new_a.length, fill);
        return new_a;
    }

    public static float[] extend(float[] a, int i) {
        float[] new_a = new float[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        return new_a;
    }

    public static float[] extend(float[] a, int i, float fill) {
        float[] new_a = new float[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        Arrays.fill(new_a, a.length, new_a.length, fill);
        return new_a;
    }

    public static double[] extend(double[] a, int i) {
        double[] new_a = new double[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        return new_a;
    }

    public static double[] extend(double[] a, int i, double fill) {
        double[] new_a = new double[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        Arrays.fill(new_a, a.length, new_a.length, fill);
        return new_a;
    }

    public static boolean[] extend(boolean[] a, int i) {
        boolean[] new_a = new boolean[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        return new_a;
    }

    public static boolean[] extend(boolean[] a, int i, boolean fill) {
        boolean[] new_a = new boolean[Math.max(a.length * 2, i + 1)];
        System.arraycopy(a, 0, new_a, 0, a.length);
        Arrays.fill(new_a, a.length, new_a.length, fill);
        return new_a;
    }

    public static <T> T[] ensureSize(T[] a, int len, Class<? extends T> c) {
        return a.length < len ? extend(a, len - 1, c) : a;
    }

    public static byte[] ensureSize(byte[] a, int len) {
        return a.length < len ? extend(a, len - 1) : a;
    }

    public static byte[] ensureSize(byte[] a, int len, byte fill) {
        return a.length < len ? extend(a, len - 1, fill) : a;
    }

    public static int[] ensureSize(int[] a, int len) {
        return a.length < len ? extend(a, len - 1) : a;
    }

    public static int[] ensureSize(int[] a, int len, int fill) {
        return a.length < len ? extend(a, len - 1, fill) : a;
    }

    public static long[] ensureSize(long[] a, int len) {
        return a.length < len ? extend(a, len - 1) : a;
    }

    public static long[] ensureSize(long[] a, int len, long fill) {
        return a.length < len ? extend(a, len - 1, fill) : a;
    }

    public static double[] ensureSize(double[] a, int len) {
        return a.length < len ? extend(a, len - 1) : a;
    }

    public static double[] ensureSize(double[] a, int len, double fill) {
        return a.length < len ? extend(a, len - 1, fill) : a;
    }

    public static boolean[] ensureSize(boolean[] a, int len) {
        return a.length < len ? extend(a, len - 1) : a;
    }

    public static boolean[] ensureSize(boolean[] a, int len, boolean fill) {
        return a.length < len ? extend(a, len - 1, fill) : a;
    }

    @SuppressWarnings("unchecked")
    public static <T> T[] extendNoCopy(T[] a, int i, Class<? extends T> c) {
        return (T[]) java.lang.reflect.Array.newInstance(c, Math.max(a.length * 2, i + 1));
    }

    public static char[] extendNoCopy(char[] a, int i) {
        return new char[Math.max(a.length * 2, i + 1)];
    }

    public static short[] extendNoCopy(short[] a, int i) {
        return new short[Math.max(a.length * 2, i + 1)];
    }

    public static int[] extendNoCopy(int[] a, int i) {
        return new int[Math.max(a.length * 2, i + 1)];
    }

    public static long[] extendNoCopy(long[] a, int i) {
        return new long[Math.max(a.length * 2, i + 1)];
    }

    public static float[] extendNoCopy(float[] a, int i) {
        return new float[Math.max(a.length * 2, i + 1)];
    }

    public static double[] extendNoCopy(double[] a, int i) {
        return new double[Math.max(a.length * 2, i + 1)];
    }

    public static boolean[] extendNoCopy(boolean[] a, int i) {
        return new boolean[Math.max(a.length * 2, i + 1)];
    }

    public static byte[] extendNoCopy(byte[] a, int i) {
        return new byte[Math.max(a.length * 2, i + 1)];
    }

    public static <T> T[] ensureSizeNoCopy(T[] a, int len, Class<? extends T> c) {
        return a.length < len ? extendNoCopy(a, len - 1, c) : a;
    }

    public static char[] ensureSizeNoCopy(char[] a, int len) {
        return a.length < len ? extendNoCopy(a, len - 1) : a;
    }

    public static short[] ensureSizeNoCopy(short[] a, int len) {
        return a.length < len ? extendNoCopy(a, len - 1) : a;
    }

    public static int[] ensureSizeNoCopy(int[] a, int len) {
        return a.length < len ? extendNoCopy(a, len - 1) : a;
    }

    public static long[] ensureSizeNoCopy(long[] a, int len) {
        return a.length < len ? extendNoCopy(a, len - 1) : a;
    }

    public static float[] ensureSizeNoCopy(float[] a, int len) {
        return a.length < len ? extendNoCopy(a, len - 1) : a;
    }

    public static double[] ensureSizeNoCopy(double[] a, int len) {
        return a.length < len ? extendNoCopy(a, len - 1) : a;
    }

    public static boolean[] ensureSizeNoCopy(boolean[] a, int len) {
        return a.length < len ? extendNoCopy(a, len - 1) : a;
    }

    public static byte[] ensureSizeNoCopy(byte[] a, int len) {
        return a.length < len ? extendNoCopy(a, len - 1) : a;
    }

    public static int[] insert(int[] a, int i, int v) {
        final int[] newArray = new int[a.length + 1];
        System.arraycopy(a, 0, newArray, 0, i);
        newArray[i] = v;
        System.arraycopy(a, i, newArray, i + 1, newArray.length - i - 1);
        return newArray;
    }

    public static double[] insert(double[] a, int i, double v) {
        final double[] newArray = new double[a.length + 1];
        System.arraycopy(a, 0, newArray, 0, i);
        newArray[i] = v;
        System.arraycopy(a, i, newArray, i + 1, newArray.length - i - 1);
        return newArray;
    }

    public static long[] insert(long[] a, int i, long v) {
        final long[] newArray = new long[a.length + 1];
        System.arraycopy(a, 0, newArray, 0, i);
        newArray[i] = v;
        System.arraycopy(a, i, newArray, i + 1, newArray.length - i - 1);
        return newArray;
    }

    public static <T> T[] insert(T[] a, int i, T v, Class<? extends T> c) {
        final T[] newArray = (T[]) java.lang.reflect.Array.newInstance(c, a.length + 1);
        System.arraycopy(a, 0, newArray, 0, i);
        newArray[i] = v;
        System.arraycopy(a, i, newArray, i + 1, newArray.length - i - 1);
        return newArray;
    }

    public static <T> T[] insert(T[] a, int insertionPoint, int numElements, T v,
        Class<? extends T> c) {
        if (a.length < numElements + 1) {
            T[] a2 = extendNoCopy(a, numElements, c);
            System.arraycopy(a, 0, a2, 0, insertionPoint);
            a2[insertionPoint] = v;
            System.arraycopy(a, insertionPoint, a2, insertionPoint + 1,
                numElements - insertionPoint);
            return a2;
        } else {
            System.arraycopy(a, insertionPoint, a, insertionPoint + 1,
                numElements - insertionPoint);
            a[insertionPoint] = v;
            return a;
        }
    }

    public static <T> T[] insertOrdered(Comparator<T> comp, T[] a, T v, Class<? extends T> c) {
        int i;
        for (i = 0; i < a.length && comp.compare(v, a[i]) >= 0; ++i) {
        }
        return insert(a, i, v, c);
    }

    public static <T> T[] put(T[] a, int i, T v, Class<? extends T> c) {
        if (i >= a.length) {
            a = extend(a, i, c);
        }
        a[i] = v;
        return a;
    }

    public static <T> T[] put(T[] a, int i, T[] vs, int vi, int vlen, Class<? extends T> c) {
        if (i + vlen >= a.length) {
            a = extend(a, i + vlen, c);
        }
        System.arraycopy(vs, vi, a, i, vlen);
        return a;
    }

    public static boolean[] put(boolean[] a, int i, boolean[] vs, int vi, int vlen) {
        if (i + vlen >= a.length) {
            a = extend(a, i + vlen);
        }
        System.arraycopy(vs, vi, a, i, vlen);
        return a;
    }

    public static byte[] put(byte[] a, int i, byte[] vs, int vi, int vlen) {
        if (i + vlen >= a.length) {
            a = extend(a, i + vlen);
        }
        System.arraycopy(vs, vi, a, i, vlen);
        return a;
    }

    public static char[] put(char[] a, int i, char[] vs, int vi, int vlen) {
        if (i + vlen >= a.length) {
            a = extend(a, i + vlen);
        }
        System.arraycopy(vs, vi, a, i, vlen);
        return a;
    }

    public static short[] put(short[] a, int i, short[] vs, int vi, int vlen) {
        if (i + vlen >= a.length) {
            a = extend(a, i + vlen);
        }
        System.arraycopy(vs, vi, a, i, vlen);
        return a;
    }

    public static int[] put(int[] a, int i, int[] vs, int vi, int vlen) {
        if (i + vlen >= a.length) {
            a = extend(a, i + vlen);
        }
        System.arraycopy(vs, vi, a, i, vlen);
        return a;
    }

    public static long[] put(long[] a, int i, long[] vs, int vi, int vlen) {
        if (i + vlen >= a.length) {
            a = extend(a, i + vlen);
        }
        System.arraycopy(vs, vi, a, i, vlen);
        return a;
    }

    public static float[] put(float[] a, int i, float[] vs, int vi, int vlen) {
        if (i + vlen >= a.length) {
            a = extend(a, i + vlen);
        }
        System.arraycopy(vs, vi, a, i, vlen);
        return a;
    }

    public static double[] put(double[] a, int i, double[] vs, int vi, int vlen) {
        if (i + vlen >= a.length) {
            a = extend(a, i + vlen);
        }
        System.arraycopy(vs, vi, a, i, vlen);
        return a;
    }

    public static byte[] put(byte[] a, int i, byte v) {
        if (i >= a.length) {
            a = extend(a, i);
        }
        a[i] = v;
        return a;
    }

    public static char[] put(char[] a, int i, char v) {
        if (i >= a.length) {
            a = extend(a, i);
        }
        a[i] = v;
        return a;
    }

    public static short[] put(short[] a, int i, short v) {
        if (i >= a.length) {
            a = extend(a, i);
        }
        a[i] = v;
        return a;
    }

    public static int[] put(int[] a, int i, int v) {
        if (i >= a.length) {
            a = extend(a, i);
        }
        a[i] = v;
        return a;
    }

    public static long[] put(long[] a, int i, long v) {
        if (i >= a.length) {
            a = extend(a, i);
        }
        a[i] = v;
        return a;
    }

    public static float[] put(float[] a, int i, float v) {
        if (i >= a.length) {
            a = extend(a, i);
        }
        a[i] = v;
        return a;
    }

    public static double[] put(double[] a, int i, double v) {
        if (i >= a.length) {
            a = extend(a, i);
        }
        a[i] = v;
        return a;
    }

    public static boolean[] put(boolean[] a, int i, boolean v) {
        if (i >= a.length) {
            a = extend(a, i);
        }
        a[i] = v;
        return a;
    }

    public static void swap(int[] a, int i, int j) {
        int temp = a[i];
        a[i] = a[j];
        a[j] = temp;
    }

    public static void swap(long[] a, int i, int j) {
        long temp = a[i];
        a[i] = a[j];
        a[j] = temp;
    }

    public static void swap(double[] a, int i, int j) {
        double temp = a[i];
        a[i] = a[j];
        a[j] = temp;
    }

    public static void swap(boolean[] a, int i, int j) {
        boolean temp = a[i];
        a[i] = a[j];
        a[j] = temp;
    }

    public static <T> void swap(T[] a, int i, int j) {
        T temp = a[i];
        a[i] = a[j];
        a[j] = temp;
    }

    // Equality Testing (stolen and modified from Arrays.java)

    public static boolean equals(long[] a, long[] a2, int start1, int start2, int length) {
        if (start1 + length > a.length || start2 + length > a2.length) {
            return false;
        }

        for (int i = 0; i < length; i++)
            if (a[i + start1] != a2[i + start2])
                return false;

        return true;
    }

    public static boolean equals(int[] a, int[] a2, int start1, int start2, int length) {
        if (start1 + length > a.length || start2 + length > a2.length) {
            return false;
        }

        for (int i = 0; i < length; i++)
            if (a[i + start1] != a2[i + start2])
                return false;

        return true;
    }

    public static boolean equals(short[] a, short a2[], int start1, int start2, int length) {
        if (start1 + length > a.length || start2 + length > a2.length) {
            return false;
        }

        for (int i = 0; i < length; i++)
            if (a[i + start1] != a2[i + start2])
                return false;

        return true;
    }

    public static boolean equals(char[] a, char[] a2, int start1, int start2, int length) {

        if (start1 + length > a.length || start2 + length > a2.length) {
            return false;
        }

        for (int i = 0; i < length; i++)
            if (a[i + start1] != a2[i + start2])
                return false;

        return true;
    }

    public static boolean equals(byte[] a, byte[] a2, int start1, int start2, int length) {

        if (start1 + length > a.length || start2 + length > a2.length) {
            return false;
        }

        for (int i = 0; i < length; i++)
            if (a[i + start1] != a2[i + start2])
                return false;

        return true;
    }

    public static boolean equals(boolean[] a, boolean[] a2, int start1, int start2, int length) {

        if (start1 + length > a.length || start2 + length > a2.length) {
            return false;
        }

        for (int i = 0; i < length; i++)
            if (a[i + start1] != a2[i + start2])
                return false;

        return true;
    }

    public static boolean equals(double[] a, double[] a2, int start1, int start2, int length) {
        if (start1 + length > a.length || start2 + length > a2.length) {
            return false;
        }

        for (int i = 0; i < length; i++)
            if (Double.doubleToLongBits(a[i + start1]) != Double.doubleToLongBits(a2[i + start2]))
                return false;

        return true;
    }

    public static boolean equals(float[] a, float[] a2, int start1, int start2, int length) {
        if (start1 + length > a.length || start2 + length > a2.length) {
            return false;
        }

        for (int i = 0; i < length; i++)
            if (Float.floatToIntBits(a[i + start1]) != Float.floatToIntBits(a2[i + start2]))
                return false;

        return true;
    }

    public static boolean equals(Object[] a, Object[] a2, int start1, int start2, int length) {
        if (start1 + length > a.length || start2 + length > a2.length) {
            return false;
        }

        for (int i = 0; i < length; i++) {
            Object o1 = a[i + start1];
            Object o2 = a2[i + start2];
            if (!(o1 == null ? o2 == null : o1.equals(o2)))
                return false;
        }

        return true;
    }

    public static int hashCode(long a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            long element = a[min + i];
            int elementHash = (int) (element ^ (element >>> 32));
            result = 31 * result + elementHash;
        }

        return result;
    }

    public static int hashCode(int a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            int element = a[min + i];
            result = 31 * result + element;
        }

        return result;
    }

    public static int hashCode(short a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            short element = a[min + i];
            result = 31 * result + element;
        }

        return result;
    }

    public static int hashCode(char a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            char element = a[min + i];
            result = 31 * result + element;
        }

        return result;
    }

    public static int hashCode(byte a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            byte element = a[min + i];
            result = 31 * result + element;
        }

        return result;
    }

    public static int hashCode(boolean a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            boolean element = a[min + i];
            result = 31 * result + (element ? 1231 : 1237);
        }

        return result;
    }

    public static int hashCode(float a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            float element = a[min + i];
            result = 31 * result + Float.floatToIntBits(element);
        }

        return result;
    }

    public static int hashCode(double a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            double element = a[min + i];
            long bits = Double.doubleToLongBits(element);
            result = 31 * result + (int) (bits ^ (bits >>> 32));
        }
        return result;
    }

    public static int hashCode(Object a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;

        for (int i = 0; i < length; ++i) {
            Object element = a[min + i];
            result = 31 * result + (element == null ? 0 : element.hashCode());
        }

        return result;
    }

    public static int hashCodeAnyOrder(long a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            long element = a[min + i];
            int elementHash = (int) (element ^ (element >>> 32));
            result += elementHash;
        }

        return result;
    }

    public static int hashCodeAnyOrder(int a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            int element = a[min + i];
            result += element;
        }

        return result;
    }

    public static int hashCodeAnyOrder(short a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            short element = a[min + i];
            result += element;
        }

        return result;
    }

    public static int hashCodeAnyOrder(char a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            char element = a[min + i];
            result += element;
        }

        return result;
    }

    public static int hashCodeAnyOrder(byte a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            byte element = a[min + i];
            result += element;
        }

        return result;
    }

    public static int hashCodeAnyOrder(boolean a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            boolean element = a[min + i];
            result += (element ? 1231 : 1237);
        }

        return result;
    }

    public static int hashCodeAnyOrder(float a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            float element = a[min + i];
            result += Float.floatToIntBits(element);
        }

        return result;
    }

    public static int hashCodeAnyOrder(double a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            double element = a[min + i];
            long bits = Double.doubleToLongBits(element);
            result += (int) (bits ^ (bits >>> 32));
        }
        return result;
    }

    public static int hashCodeAnyOrder(Object a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;

        for (int i = 0; i < length; ++i) {
            Object element = a[min + i];
            result += (element == null ? 0 : element.hashCode());
        }

        return result;
    }

    public static int hashCodeAnyOrderAnySign(long a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            long element = Math.abs(a[min + i]);
            int elementHash = (int) (element ^ (element >>> 32));
            result += elementHash;
        }

        return result;
    }

    public static int hashCodeAnyOrderAnySign(int a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            int element = Math.abs(a[min + i]);
            result += element;
        }

        return result;
    }

    public static int hashCodeAnyOrderAnySign(short a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            short element = (short) Math.abs(a[min + i]);
            result += element;
        }

        return result;
    }

    public static int hashCodeAnyOrderAnySign(float a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            float element = Math.abs(a[min + i]);
            result += Float.floatToIntBits(element);
        }

        return result;
    }

    public static int hashCodeAnyOrderAnySign(double a[], int min, int length) {
        if (a == null)
            return 0;

        int result = 1;
        for (int i = 0; i < length; ++i) {
            double element = Math.abs(a[min + i]);
            long bits = Double.doubleToLongBits(element);
            result += (int) (bits ^ (bits >>> 32));
        }
        return result;
    }

    public static <K, V> Map<K, V> mapFromArray(Class<K> keyType, Class<V> valueType,
        Object... data) {
        Map<K, V> map = new HashMap<K, V>(data.length);
        for (int nIndex = 0; nIndex < data.length; nIndex += 2) {
            Object key = data[nIndex];
            Object value = data[nIndex + 1];
            map.put(keyType.cast(key), valueType.cast(value));
        }
        return map;
    }

    public static String toString(double[] doubles, int nOffset, int nLength) {
        Require.neqNull(doubles, "doubles");
        Require.inRange(nOffset, "nOffset", doubles.length, "doubles.length");
        Require.inRange(nLength, "nLength", doubles.length - nOffset, "doubles.length-nOffset");
        if (0 == nLength) {
            return "[]";
        }
        int nLastIndex = nOffset + nLength;

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('[');
        for (int nIndex = nOffset;; nIndex++) {
            stringBuilder.append(doubles[nIndex]);
            if (nIndex == nLastIndex) {
                return stringBuilder.append(']').toString();
            }
            stringBuilder.append(", ");
        }
    }

    public static String toString(long[] longs, int nOffset, int nLength) {
        Require.neqNull(longs, "longs");
        Require.inRange(nOffset, "nOffset", longs.length, "longs.length");
        Require.inRange(nLength, "nLength", longs.length - nOffset, "longs.length-nOffset");
        if (0 == nLength) {
            return "[]";
        }
        int nLastIndex = nOffset + nLength;

        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append('[');
        for (int nIndex = nOffset;; nIndex++) {
            stringBuilder.append(longs[nIndex]);
            if (nIndex == nLastIndex) {
                return stringBuilder.append(']').toString();
            }
            stringBuilder.append(", ");
        }
    }

    public static StringBuilder appendIntArray(final StringBuilder sb, final int[] vs,
        final boolean compact) {
        for (int i = 0; i < vs.length; ++i) {
            if (i != 0) {
                if (compact) {
                    sb.append(',');
                } else {
                    sb.append(", ");
                }
            }
            sb.append(vs[i]);
        }
        return sb;
    }

    public static String intArrayToString(final int[] vs) {
        return intArrayToString(vs, false);
    }

    public static String intArrayToString(final int[] vs, final boolean compact) {
        final StringBuilder sb = new StringBuilder();
        appendIntArray(sb, vs, compact);
        return sb.toString();
    }

    public static String intArrayArrayToString(final int[][] vs) {
        final StringBuilder sb = new StringBuilder();
        boolean firstTime = true;
        for (int[] as : vs) {
            if (!firstTime) {
                sb.append(',');
            }
            sb.append('{');
            appendIntArray(sb, as, true);
            sb.append('}');
            firstTime = false;
        }
        return sb.toString();
    }

    public static String intArrayArrayArrayToString(final int[][][] vs) {
        final StringBuilder sb = new StringBuilder();
        boolean firstTimeA = true;
        for (int[][] as : vs) {
            if (!firstTimeA) {
                sb.append(',');
            }
            sb.append('{');
            boolean firstTimeB = true;
            for (int[] bs : as) {
                if (!firstTimeB) {
                    sb.append(',');
                }
                sb.append('{');
                appendIntArray(sb, bs, true);
                sb.append('}');
                firstTimeB = false;
            }
            sb.append('}');
            firstTimeA = false;
        }
        return sb.toString();
    }

    public static int[] copyArray(int[] a, int L) {
        final int[] r = new int[L];
        System.arraycopy(a, 0, r, 0, L);
        return r;
    }

    public static long[] copyArray(long[] a, int L) {
        final long[] r = new long[L];
        System.arraycopy(a, 0, r, 0, L);
        return r;
    }

    public static boolean isSorted(int[] integers) {
        if (integers != null && integers.length >= 2) {
            for (int i = 1; i < integers.length; i++) {
                if (integers[i] < integers[i - 1]) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isSorted(long[] longs) {
        if (longs != null && longs.length >= 2) {
            for (int i = 1; i < longs.length; i++) {
                if (longs[i] < longs[i - 1]) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean isSorted(double[] doubles) {
        if (doubles != null && doubles.length >= 2) {
            for (int i = 1; i < doubles.length; i++) {
                // d1 < d2 == negative value
                if (Double.compare(doubles[i], doubles[i - 1]) < 0) {
                    return false;
                }
            }
        }
        return true;
    }


    public static <T extends Comparable<? super T>> boolean isSorted(T[] objects) {
        if (objects != null && objects.length >= 2) {
            for (int i = 1; i < objects.length; i++) {
                // if the previous one is null it's either smaller or equal to the next 'null' and
                // therefore sorted
                if ((objects[i - 1] == null)) {
                    continue;
                }
                // if the later element is null (previous one cannot be) then its out of order,
                // otherwise compare values (o1 < o2 == negative integer)
                if (objects[i] == null || objects[i].compareTo(objects[i - 1]) < 0) {
                    return false;
                }
            }
        }
        return true;
    }

    public static int[] intRangeArray(int min, int max) {
        int[] array = new int[max - min + 1];

        for (int i = 0; i < array.length; ++i) {
            array[i] = min + i;
        }

        return array;
    }

    public static <T> void init(T[] array, Function.Nullary<T> producer) {
        for (int i = 0; i < array.length; ++i) {
            array[i] = producer.call();
        }
    }
}

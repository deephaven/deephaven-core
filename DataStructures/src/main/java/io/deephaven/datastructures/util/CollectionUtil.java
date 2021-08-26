/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.datastructures.util;

import java.io.Serializable;
import java.util.*;

import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.RequirementFailure;
import org.jetbrains.annotations.NotNull;

// --------------------------------------------------------------------
/**
 * Utility methods for creating collections.
 */
public class CollectionUtil {

    public static final byte[] ZERO_LENGTH_BYTE_ARRAY = new byte[0];
    public static final short[] ZERO_LENGTH_SHORT_ARRAY = new short[0];
    public static final int[] ZERO_LENGTH_INT_ARRAY = new int[0];
    public static final int[][] ZERO_LENGTH_INT_ARRAY_ARRAY = new int[0][];
    public static final long[] ZERO_LENGTH_LONG_ARRAY = new long[0];
    public static final float[] ZERO_LENGTH_FLOAT_ARRAY = new float[0];
    public static final double[] ZERO_LENGTH_DOUBLE_ARRAY = new double[0];
    public static final double[][] ZERO_LENGTH_DOUBLE_ARRAY_ARRAY = new double[0][];
    public static final Object[] ZERO_LENGTH_OBJECT_ARRAY = new Object[0];
    public static final String[] ZERO_LENGTH_STRING_ARRAY = new String[0];
    public static final String[][] ZERO_LENGTH_STRING_ARRAY_ARRAY = new String[0][];

    // ----------------------------------------------------------------
    public static <K, V> Map<K, V> unmodifiableMapFromArray(Class<K> typeK, Class<V> typeV,
        Object... data) {
        try {
            return Collections.unmodifiableMap(mapFromArray(typeK, typeV, data));
        } catch (RequirementFailure e) {
            throw e.adjustForDelegatingMethod();
        }
    }

    // ----------------------------------------------------------------
    public static <K, V> Map<K, V> unmodifiableInvertMap(Map<V, K> sourceMap) {
        try {
            return Collections.unmodifiableMap(invertMap(sourceMap));
        } catch (RequirementFailure e) {
            throw e.adjustForDelegatingMethod();
        }
    }

    // ----------------------------------------------------------------
    public static <E> Set<E> unmodifiableSetFromArray(E... data) {
        try {
            return Collections.unmodifiableSet(setFromArray(data));
        } catch (RequirementFailure e) {
            throw e.adjustForDelegatingMethod();
        }
    }

    // ----------------------------------------------------------------
    @SuppressWarnings({"unchecked"})
    public static <K, V> Map<K, V> mapFromArray(Class<K> typeK, Class<V> typeV, Object... data) {
        return mapFromArray(typeK, typeV, false, data);
    }

    // ----------------------------------------------------------------
    @SuppressWarnings({"unchecked"})
    public static <K, V> Map<K, V> mapFromArray(Class<K> typeK, Class<V> typeV,
        final boolean allowDuplicateKeys, Object... data) {
        Require.neqNull(data, "data");
        Require.requirement(0 == data.length % 2, "0==data.length%2");
        Map<K, V> map = newSizedLinkedHashMap(data.length / 2);
        for (int nIndex = 0; nIndex < data.length; nIndex += 2) {
            Object key = data[nIndex];
            if (null != key) {
                Require.instanceOf(key, "key", typeK);
            }
            if (!allowDuplicateKeys) {
                Require.requirement(false == map.containsKey(key),
                    "false==map.containsKey(data[nIndex])", key, "data[nIndex]");
            }
            Object value = data[nIndex + 1];
            if (null != value) {
                Require.instanceOf(value, "value", typeV);
            }
            map.put((K) key, (V) value);
        }
        return map;
    }

    // ----------------------------------------------------------------
    public static <K, V> Map<K, V> invertMap(Map<V, K> sourceMap) {
        Require.neqNull(sourceMap, "sourceMap");
        Map<K, V> targetMap = newSizedLinkedHashMap(sourceMap.size());
        for (Map.Entry<V, K> entry : sourceMap.entrySet()) {
            K key = entry.getValue();
            V value = entry.getKey();
            Require.requirement(false == targetMap.containsKey(key),
                "false==targetMap.containsKey(key)", key, "key");
            targetMap.put(key, value);
        }
        return targetMap;
    }

    // ----------------------------------------------------------------
    public static <E> Set<E> setFromArray(E... data) {
        Require.neqNull(data, "data");
        Set<E> set = newSizedLinkedHashSet(data.length);
        for (E elem : data) {
            Require.requirement(set.add(elem), "set.add(elem)");
        }
        return set;
    }

    // ----------------------------------------------------------------
    public static <TYPE> Set<TYPE> setFromArray(@NotNull final Class<TYPE> type,
        @NotNull final Object... data) {
        final Set<TYPE> set = newSizedLinkedHashSet(data.length);
        for (final Object elem : data) {
            Require.requirement(elem == null || type.isInstance(elem),
                "elem == null || type.isInstance(elem)");
            // noinspection unchecked
            Require.requirement(set.add((TYPE) elem), "set.add((TYPE)elem)");
        }
        return set;
    }

    // ----------------------------------------------------------------
    public static <E> List<E> listFromArray(E... data) {
        Require.neqNull(data, "data");
        List<E> list = new ArrayList<E>(data.length);
        for (E elem : data) {
            list.add(elem);
        }
        return list;
    }

    // ----------------------------------------------------------------
    /**
     * Returns an empty {@link HashMap} with a big enough capacity such that the given number of
     * entries can be added without resizing.
     */
    public static <K, V> Map<K, V> newSizedHashMap(int nEntries) {
        return new HashMap<K, V>((nEntries + 1) * 4 / 3);
    }

    // ----------------------------------------------------------------
    /**
     * Returns an empty {@link LinkedHashMap} with a big enough capacity such that the given number
     * of entries can be added without resizing.
     */
    public static <K, V> Map<K, V> newSizedLinkedHashMap(int nEntries) {
        return new LinkedHashMap<K, V>((nEntries + 1) * 4 / 3);
    }

    // ----------------------------------------------------------------
    /**
     * Returns an empty {@link HashSet} with a big enough capacity such that the given number of
     * entries can be added without resizing.
     */
    public static <E> Set<E> newSizedHashSet(int nEntries) {
        return new HashSet<E>((nEntries + 1) * 4 / 3);
    }

    // ----------------------------------------------------------------
    /**
     * Returns an empty {@link LinkedHashSet} with a big enough capacity such that the given number
     * of entries can be added without resizing.
     */
    public static <E> Set<E> newSizedLinkedHashSet(int nEntries) {
        return new LinkedHashSet<E>((nEntries + 1) * 4 / 3);
    }

    // ----------------------------------------------------------------
    /** Converts a List of Doubles to an array of doubles. */
    public static double[] convertDoublesToPrimitiveArray(Collection<Double> collection) {
        double[] array = new double[collection.size()];
        Iterator<Double> iter = collection.iterator();
        for (int nIndex = 0; nIndex < array.length; nIndex++) {
            array[nIndex] = iter.next();
        }
        return array;
    }

    // ----------------------------------------------------------------
    /** Converts a Collection of Integers to an array of ints. */
    public static int[] convertIntegersToPrimitiveArray(Collection<Integer> collection) {
        int[] array = new int[collection.size()];
        Iterator<Integer> iter = collection.iterator();
        for (int nIndex = 0; nIndex < array.length; nIndex++) {
            array[nIndex] = iter.next();
        }
        return array;
    }

    // ----------------------------------------------------------------
    /** Converts a Collection of Integers to an array of ints. */
    public static long[] convertLongsToPrimitiveArray(Collection<Long> collection) {
        long[] array = new long[collection.size()];
        Iterator<Long> iter = collection.iterator();
        for (int nIndex = 0; nIndex < array.length; nIndex++) {
            array[nIndex] = iter.next();
        }
        return array;
    }

    // ----------------------------------------------------------------
    /** Shuffles the elements in an array. */
    public static <E> void shuffle(E[] elems, Random random) {
        for (int nIndex = elems.length; nIndex > 1; nIndex--) {
            int nChoice = random.nextInt(nIndex);
            int nSwapWith = nIndex - 1;
            if (nChoice != nSwapWith) {
                E temp = elems[nSwapWith];
                elems[nSwapWith] = elems[nChoice];
                elems[nChoice] = temp;
            }
        }
    }

    // ----------------------------------------------------------------
    /** Determines if an array contains a null. */
    public static <E> boolean containsNull(E[] elems) {
        for (E e : elems) {
            if (e == null) {
                return true;
            }
        }

        return false;
    }


    // ----------------------------------------------------------------
    // Support for "universal" sets - really just for convenience.

    /**
     * The universal set (immutable). This set is serializable.
     */
    public static final Set UNIVERSAL_SET = new UniversalSet();

    /**
     * Returns the universal set (immutable). This set is serializable.
     */
    @SuppressWarnings("unchecked")
    public static <T> Set<T> universalSet() {
        return (Set<T>) UNIVERSAL_SET;
    }

    /**
     * A set that contains everything. Iteration is not supported - only containment checks.
     */
    private static class UniversalSet<T> implements Set<T>, Serializable {

        private static final long serialVersionUID = 1L;

        private Object readResolve() {
            return UNIVERSAL_SET;
        }

        @Override
        public int size() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isEmpty() {
            return false;
        }

        @Override
        public boolean contains(Object o) {
            return true;
        }

        @Override
        public Iterator<T> iterator() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Object[] toArray() {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean add(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean remove(Object o) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean containsAll(Collection<?> c) {
            return true;
        }

        @Override
        public boolean addAll(Collection<? extends T> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean retainAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean removeAll(Collection<?> c) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void clear() {
            throw new UnsupportedOperationException();
        }

        @Override
        public <T> T[] toArray(T[] a) {
            throw new UnsupportedOperationException();
        }
    }
}

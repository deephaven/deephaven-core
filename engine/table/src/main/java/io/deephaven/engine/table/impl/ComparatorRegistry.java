//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.engine.table.impl.comparators.CharArrayComparator;
import io.deephaven.engine.table.impl.comparators.DoubleArrayComparator;
import io.deephaven.engine.table.impl.comparators.FloatArrayComparator;
import io.deephaven.engine.table.impl.comparators.ObjectArrayComparator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.*;

/**
 * The ComparatorRegistry is a set of default comparators for sort operations.
 *
 * <p>
 * Only types that are not themselves Comparable (meaning they do not have their own natural order) may be registered.
 * </p>
 */
public class ComparatorRegistry {
    static final ComparatorRegistry INSTANCE = new ComparatorRegistry();

    private final Map<Class<?>, Comparator<?>> comparators = Collections.synchronizedMap(new HashMap<>());

    /**
     * Initializes the Comparator registry with the default set of comparators.
     */
    private ComparatorRegistry() {
        reset();
    }

    /**
     * Resets the Comparator registry to the default set of Comparators.
     *
     * <p>
     * Any user-specified comparators are discarded. Existing sort operations are not affected.
     * </p>
     *
     * <p>
     * The default set of Comparators includes lexicographical comparators for primitive, String, BigInteger, and
     * BigDecimal arrays.
     * </p>
     */
    public void reset() {
        synchronized (comparators) {
            clear();
            registerComparator(char[].class, new CharArrayComparator());
            registerComparator(float[].class, new FloatArrayComparator());
            registerComparator(double[].class, new DoubleArrayComparator());
            registerComparator(byte[].class, Arrays::compare);
            registerComparator(short[].class, Arrays::compare);
            registerComparator(int[].class, Arrays::compare);
            registerComparator(long[].class, Arrays::compare);
            registerComparator(String[].class, new ObjectArrayComparator<>());
            registerComparator(BigInteger[].class, new ObjectArrayComparator<>());
            registerComparator(BigDecimal[].class, new ObjectArrayComparator<>());
            registerComparator(Comparable[].class, new ObjectArrayComparator<>());
        }
    }

    /**
     * Removes all registered comparators from the registry.
     *
     * <p>
     * Any default or user-specified comparators are discarded. Existing sort operations are not affected.
     * </p>
     */
    public void clear() {
        comparators.clear();
    }

    /**
     * Adds a new Comparator for the given type to the registry.
     *
     * <p>
     * Existing sort operations are not affected.
     * </p>
     *
     * @param type the type to associate with this comparator, the type may not already be Comparable. To sort a type
     *        using an order other than the natural order, you must use a {@link ComparatorSortColumn}.
     * @param comparator the comparator to register for the given type
     */
    public <T> void registerComparator(final Class<T> type, final Comparator<T> comparator) {
        if (Comparable.class.isAssignableFrom(type) || type.isPrimitive()) {
            throw new IllegalArgumentException(
                    "Cannot register comparator for " + type + ", already provides a natural order.");
        }
        comparators.put(type, comparator);
    }

    /**
     * Retrieves the comparator that {@link io.deephaven.engine.table.Table#sort} uses for the given type.
     *
     * @param type the type to retrieve the comparator for
     * @return the comparator for the provided type, or null if one is not registered
     * @param <T> type's type
     */
    public <T> Comparator<T> getComparator(final Class<T> type) {
        // noinspection unchecked
        return (Comparator<T>) comparators.get(type);
    }
}

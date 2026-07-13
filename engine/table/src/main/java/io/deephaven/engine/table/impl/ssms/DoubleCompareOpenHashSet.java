//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit FloatCompareOpenHashSet and run "./gradlew replicateSegmentedSortedMultiset" to regenerate
//
// @formatter:off
package io.deephaven.engine.table.impl.ssms;

import io.deephaven.util.compare.DoubleComparisons;
import it.unimi.dsi.fastutil.doubles.DoubleCollection;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;

/**
 * A subset of {@link DoubleOpenHashSet} that compares values consistently with {@link DoubleComparisons} equality rather
 * than fastutil's default raw-bit equality. Concretely, {@code -0.0d} and {@code +0.0d} are treated as the same value,
 * and all NaN bit patterns are treated as a single value.
 *
 * <p>
 * Values are canonicalized on every entry point ({@link #add(double)}, {@link #remove(double)}, {@link #contains(double)},
 * the array/collection constructors, and the bulk-add helpers): negative zero is collapsed to positive zero, and any
 * NaN is replaced with {@link Double#NaN} (the canonical {@code 0x7ff8000000000000L} bit pattern). The canonicalized value is
 * what is stored and returned by iteration.
 */
class DoubleCompareOpenHashSet {

    final DoubleOpenHashSet wrapped;

    DoubleCompareOpenHashSet() {
        wrapped = new DoubleOpenHashSet();
    }

    DoubleCompareOpenHashSet(final int expected) {
        wrapped = new DoubleOpenHashSet(expected);
    }

    /**
     * Canonicalize a double for storage in the set: collapse {@code -0.0d} to {@code +0.0d}, and any NaN to
     * {@link Double#NaN}.
     */
    private static double canonicalize(final double v) {
        if (v == 0.0d) {
            return 0.0d;
        }
        if (Double.isNaN(v)) {
            return Double.NaN;
        }
        return v;
    }

    public boolean add(final double k) {
        return wrapped.add(canonicalize(k));
    }

    public boolean remove(final double k) {
        return wrapped.remove(canonicalize(k));
    }

    public boolean contains(final double k) {
        return wrapped.contains(canonicalize(k));
    }

    public int size() {
        return wrapped.size();
    }

    public double[] toDoubleArray() {
        return wrapped.toDoubleArray();
    }

    public boolean addAll(final DoubleCollection c) {
        boolean changed = false;
        final DoubleIterator it = c.iterator();
        while (it.hasNext()) {
            changed |= add(it.nextDouble());
        }
        return changed;
    }
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssms;

import io.deephaven.util.compare.DoubleComparisons;
import it.unimi.dsi.fastutil.doubles.DoubleCollection;
import it.unimi.dsi.fastutil.doubles.DoubleIterator;
import it.unimi.dsi.fastutil.doubles.DoubleOpenHashSet;

/**
 * A {@link DoubleOpenHashSet} that compares values using {@link DoubleComparisons} equality rather than fastutil's
 * default raw-bit equality. Concretely, {@code -0.0d} and {@code +0.0d} are treated as the same value, and all NaN bit
 * patterns are treated as a single value.
 *
 * <p>
 * Values are canonicalized on every entry point ({@link #add(double)}, {@link #remove(double)},
 * {@link #contains(double)}, the array/collection constructors, and the bulk-add helpers): negative zero is collapsed
 * to positive zero, and any NaN is replaced with {@link Double#NaN} (the canonical {@code 0x7ff8000000000000L} bit
 * pattern). The canonicalized value is what is stored and returned by iteration.
 */
public class DoubleCompareOpenHashSet extends DoubleOpenHashSet {

    public DoubleCompareOpenHashSet() {
        super();
    }

    public DoubleCompareOpenHashSet(final int expected) {
        super(expected);
    }

    public DoubleCompareOpenHashSet(final int expected, final float f) {
        super(expected, f);
    }

    public DoubleCompareOpenHashSet(final double[] a) {
        super();
        for (final double v : a) {
            add(v);
        }
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

    @Override
    public boolean add(final double k) {
        return super.add(canonicalize(k));
    }

    @Override
    public boolean remove(final double k) {
        return super.remove(canonicalize(k));
    }

    @Override
    public boolean contains(final double k) {
        return super.contains(canonicalize(k));
    }

    @Override
    public boolean addAll(final DoubleCollection c) {
        boolean changed = false;
        final DoubleIterator it = c.iterator();
        while (it.hasNext()) {
            changed |= add(it.nextDouble());
        }
        return changed;
    }
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssms;

import io.deephaven.util.compare.FloatComparisons;
import it.unimi.dsi.fastutil.floats.FloatCollection;
import it.unimi.dsi.fastutil.floats.FloatIterator;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;

/**
 * A {@link FloatOpenHashSet} that compares values consistently with {@link FloatComparisons} equality rather than
 * fastutil's default raw-bit equality. Concretely, {@code -0.0f} and {@code +0.0f} are treated as the same value, and
 * all NaN bit patterns are treated as a single value.
 *
 * <p>
 * Values are canonicalized on every entry point ({@link #add(float)}, {@link #remove(float)}, {@link #contains(float)},
 * the array/collection constructors, and the bulk-add helpers): negative zero is collapsed to positive zero, and any
 * NaN is replaced with {@link Float#NaN} (the canonical {@code 0x7fc00000} bit pattern). The canonicalized value is
 * what is stored and returned by iteration.
 */
public class FloatCompareOpenHashSet extends FloatOpenHashSet {

    public FloatCompareOpenHashSet() {
        super();
    }

    public FloatCompareOpenHashSet(final int expected) {
        super(expected);
    }

    public FloatCompareOpenHashSet(final int expected, final float f) {
        super(expected, f);
    }

    public FloatCompareOpenHashSet(final float[] a) {
        super();
        for (final float v : a) {
            add(v);
        }
    }

    /**
     * Canonicalize a float for storage in the set: collapse {@code -0.0f} to {@code +0.0f}, and any NaN to
     * {@link Float#NaN}.
     */
    private static float canonicalize(final float v) {
        if (v == 0.0f) {
            return 0.0f;
        }
        if (Float.isNaN(v)) {
            return Float.NaN;
        }
        return v;
    }

    @Override
    public boolean add(final float k) {
        return super.add(canonicalize(k));
    }

    @Override
    public boolean remove(final float k) {
        return super.remove(canonicalize(k));
    }

    @Override
    public boolean contains(final float k) {
        return super.contains(canonicalize(k));
    }

    @Override
    public boolean addAll(final FloatCollection c) {
        boolean changed = false;
        final FloatIterator it = c.iterator();
        while (it.hasNext()) {
            changed |= add(it.nextFloat());
        }
        return changed;
    }
}

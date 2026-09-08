//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.ssms;

import io.deephaven.util.compare.FloatComparisons;
import it.unimi.dsi.fastutil.floats.FloatCollection;
import it.unimi.dsi.fastutil.floats.FloatIterator;
import it.unimi.dsi.fastutil.floats.FloatOpenHashSet;

/**
 * A subset of {@link FloatOpenHashSet} that compares values consistently with {@link FloatComparisons} equality rather
 * than fastutil's default raw-bit equality. Concretely, {@code -0.0f} and {@code +0.0f} are treated as the same value,
 * and all NaN bit patterns are treated as a single value.
 *
 * <p>
 * Values are canonicalized on every entry point ({@link #add(float)}, {@link #remove(float)}, {@link #contains(float)},
 * the array/collection constructors, and the bulk-add helpers): negative zero is collapsed to positive zero, and any
 * NaN is replaced with {@link Float#NaN} (the canonical {@code 0x7fc00000} bit pattern). The canonicalized value is
 * what is stored and returned by iteration.
 */
class FloatCompareOpenHashSet {

    final FloatOpenHashSet wrapped;

    FloatCompareOpenHashSet() {
        wrapped = new FloatOpenHashSet();
    }

    FloatCompareOpenHashSet(final int expected) {
        wrapped = new FloatOpenHashSet(expected);
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

    public boolean add(final float k) {
        return wrapped.add(canonicalize(k));
    }

    public boolean remove(final float k) {
        return wrapped.remove(canonicalize(k));
    }

    public boolean contains(final float k) {
        return wrapped.contains(canonicalize(k));
    }

    public int size() {
        return wrapped.size();
    }

    public float[] toFloatArray() {
        return wrapped.toFloatArray();
    }

    public boolean addAll(final FloatCollection c) {
        boolean changed = false;
        final FloatIterator it = c.iterator();
        while (it.hasNext()) {
            changed |= add(it.nextFloat());
        }
        return changed;
    }
}

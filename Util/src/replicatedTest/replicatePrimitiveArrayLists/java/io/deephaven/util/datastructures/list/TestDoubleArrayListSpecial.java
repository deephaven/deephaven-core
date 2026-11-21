//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit TestFloatArrayListSpecial and run "./gradlew replicatePrimitiveArrayLists" to regenerate
//
// @formatter:off
package io.deephaven.util.datastructures.list;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestDoubleArrayListSpecial {

    @Test
    public void testAddAndGetNaN() {
        final DoubleArrayList list = new DoubleArrayList();
        list.add(Double.NaN);
        assertEquals(1, list.size());
        assertTrue(Double.isNaN(list.getDouble(0)));
    }

    @Test
    public void testEqualsTreatsNaNAsUnequal() {
        // equals(...) uses primitive == per element, so NaN != NaN; two distinct
        // lists holding only NaN are not equal even though they contain the
        // "same" value.
        final DoubleArrayList a = new DoubleArrayList();
        final DoubleArrayList b = new DoubleArrayList();
        a.add(Double.NaN);
        b.add(Double.NaN);
        assertFalse(a.equals(b));

        // Reflexivity short-circuit still holds: a list equals itself.
        assertEquals(a, a);
    }

    @Test
    public void testHashCodeStableForNaN() {
        // DoubleComparisons.hashCode collapses every NaN bit pattern to a single
        // canonical hash, so two independent lists containing NaN share a hash
        // (consistent with the equals/hashCode contract: equal hash for unequal
        // values is permitted).
        final DoubleArrayList a = new DoubleArrayList();
        final DoubleArrayList b = new DoubleArrayList();
        a.add(Double.NaN);
        b.add(Double.NaN);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a.hashCode(), a.hashCode());
    }

    @Test
    public void testEqualsAndHashCodeForSignedZero() {
        // 0.0f == -0.0f, so the lists are equal under our equals(...). The
        // contract requires equal hashCodes; DoubleComparisons.hashCode delivers
        // this via its ZERO_HASHCODE special case.
        final DoubleArrayList positive = new DoubleArrayList();
        final DoubleArrayList negative = new DoubleArrayList();
        positive.add(0.0f);
        negative.add(-0.0f);
        assertEquals(positive, negative);
        assertEquals(positive.hashCode(), negative.hashCode());
    }
}

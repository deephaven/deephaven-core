//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.datastructures.list;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestFloatArrayListSpecial {

    @Test
    public void testAddAndGetNaN() {
        final FloatArrayList list = new FloatArrayList();
        list.add(Float.NaN);
        assertEquals(1, list.size());
        assertTrue(Float.isNaN(list.getFloat(0)));
    }

    @Test
    public void testEqualsTreatsNaNAsUnequal() {
        // equals(...) uses primitive == per element, so NaN != NaN; two distinct
        // lists holding only NaN are not equal even though they contain the
        // "same" value.
        final FloatArrayList a = new FloatArrayList();
        final FloatArrayList b = new FloatArrayList();
        a.add(Float.NaN);
        b.add(Float.NaN);
        assertFalse(a.equals(b));

        // Reflexivity short-circuit still holds: a list equals itself.
        assertEquals(a, a);
    }

    @Test
    public void testHashCodeStableForNaN() {
        // FloatComparisons.hashCode collapses every NaN bit pattern to a single
        // canonical hash, so two independent lists containing NaN share a hash
        // (consistent with the equals/hashCode contract: equal hash for unequal
        // values is permitted).
        final FloatArrayList a = new FloatArrayList();
        final FloatArrayList b = new FloatArrayList();
        a.add(Float.NaN);
        b.add(Float.NaN);
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(a.hashCode(), a.hashCode());
    }

    @Test
    public void testEqualsAndHashCodeForSignedZero() {
        // 0.0f == -0.0f, so the lists are equal under our equals(...). The
        // contract requires equal hashCodes; FloatComparisons.hashCode delivers
        // this via its ZERO_HASHCODE special case.
        final FloatArrayList positive = new FloatArrayList();
        final FloatArrayList negative = new FloatArrayList();
        positive.add(0.0f);
        negative.add(-0.0f);
        assertEquals(positive, negative);
        assertEquals(positive.hashCode(), negative.hashCode());
    }
}

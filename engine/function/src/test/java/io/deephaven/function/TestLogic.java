//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.function;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.vector.ObjectVectorDirect;

import static io.deephaven.function.Logic.*;

/**
 * Test Logic.
 */
public class TestLogic extends BaseArrayTestCase {

    public void testAnd() {
        assertTrue(and(new Boolean[] {true, true, true}));
        assertFalse(and(new Boolean[] {false, true, true}));
        assertFalse(and(new Boolean[] {false, false, true}));
        assertFalse(and(new Boolean[] {true, false, false}));
        assertFalse(and(new Boolean[] {false, false, true}));
        assertFalse(and(new Boolean[] {false, false, false}));

        assertTrue(and(new Boolean[] {true, true, null}, true));
        assertFalse(and(new Boolean[] {false, true, null}, true));
        assertFalse(and(new Boolean[] {false, false, null}, true));
        assertFalse(and(new Boolean[] {true, false, null}, false));
        assertFalse(and(new Boolean[] {false, false, null}, true));
        assertFalse(and(new Boolean[] {false, false, null}, false));

        assertTrue(and(new boolean[] {true, true, true}));
        assertFalse(and(new boolean[] {false, true, true}));
        assertFalse(and(new boolean[] {false, false, true}));
        assertFalse(and(new boolean[] {true, false, false}));
        assertFalse(and(new boolean[] {false, false, true}));
        assertFalse(and(new boolean[] {false, false, false}));

        assertTrue(and(new ObjectVectorDirect<>(true, true, true)));
        assertFalse(and(new ObjectVectorDirect<>(false, true, true)));
        assertFalse(and(new ObjectVectorDirect<>(false, false, true)));
        assertFalse(and(new ObjectVectorDirect<>(true, false, false)));
        assertFalse(and(new ObjectVectorDirect<>(false, false, true)));
        assertFalse(and(new ObjectVectorDirect<>(false, false, false)));

        assertTrue(and(new ObjectVectorDirect<>(true, true, null), true));
        assertFalse(and(new ObjectVectorDirect<>(false, true, null), true));
        assertFalse(and(new ObjectVectorDirect<>(false, false, null), true));
        assertFalse(and(new ObjectVectorDirect<>(true, false, null), false));
        assertFalse(and(new ObjectVectorDirect<>(false, false, null), true));
        assertFalse(and(new ObjectVectorDirect<>(false, false, null), false));
    }

    public void testOr() {
        assertTrue(or(new Boolean[] {true, true, true}));
        assertTrue(or(new Boolean[] {false, true, true}));
        assertTrue(or(new Boolean[] {false, false, true}));
        assertTrue(or(new Boolean[] {true, false, false}));
        assertTrue(or(new Boolean[] {false, false, true}));
        assertFalse(or(new Boolean[] {false, false, false}));

        assertTrue(or(new Boolean[] {true, true, null}, false));
        assertTrue(or(new Boolean[] {false, true, null}, true));
        assertTrue(or(new Boolean[] {false, false, null}, true));
        assertTrue(or(new Boolean[] {true, false, null}, false));
        assertTrue(or(new Boolean[] {false, false, null}, true));
        assertFalse(or(new Boolean[] {false, false, null}, false));

        assertTrue(or(new boolean[] {true, true, true}));
        assertTrue(or(new boolean[] {false, true, true}));
        assertTrue(or(new boolean[] {false, false, true}));
        assertTrue(or(new boolean[] {true, false, false}));
        assertTrue(or(new boolean[] {false, false, true}));
        assertFalse(or(new boolean[] {false, false, false}));
    }

    public void testNot() {
        // noinspection ConstantValue
        assertNull(not((boolean[]) null));
        // noinspection ConstantValue
        assertNull(not((Boolean[]) null));

        assertEquals(new Boolean[] {false, false, false}, not(new Boolean[] {true, true, true}));
        assertEquals(new Boolean[] {true, false, false}, not(new Boolean[] {false, true, true}));
        assertEquals(new Boolean[] {true, true, false}, not(new Boolean[] {false, false, true}));
        assertEquals(new Boolean[] {false, true, true}, not(new Boolean[] {true, false, false}));
        assertEquals(new Boolean[] {true, true, false}, not(new Boolean[] {false, false, true}));
        assertEquals(new Boolean[] {true, true, true}, not(new Boolean[] {false, false, false}));
        assertEquals(new Boolean[] {true, null, true}, not(false, null, false));

        assertEquals(new Boolean[] {false, false, false}, not(new boolean[] {true, true, true}));
        assertEquals(new Boolean[] {true, false, false}, not(new boolean[] {false, true, true}));
        assertEquals(new Boolean[] {true, true, false}, not(new boolean[] {false, false, true}));
        assertEquals(new Boolean[] {false, true, true}, not(new boolean[] {true, false, false}));
        assertEquals(new Boolean[] {true, true, false}, not(new boolean[] {false, false, true}));
        assertEquals(new Boolean[] {true, true, true}, not(new boolean[] {false, false, false}));
    }
}

/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.generator.primitivetemplate;

import junit.framework.TestCase;

public class TestPrimitiveType extends TestCase {

    public void testPrimitiveType() {

        final PrimitiveType pt = new PrimitiveType(
                "primitive", "boxed",
                "VectorDEBUG", "DirectDEBUG",
                "NULLDEBUG", "POSINFDEBUG", "NEGINFDEBUG",
                ValueType.CHARACTER);

        assertEquals("primitive", pt.getPrimitive());
        assertEquals("boxed", pt.getBoxed());
        assertEquals("VectorDEBUG", pt.getVector());
        assertEquals("DirectDEBUG", pt.getVectorDirect());
        assertEquals("NULLDEBUG", pt.getNull());
        assertEquals("POSINFDEBUG", pt.getMaxValue());
        assertEquals("NEGINFDEBUG", pt.getMinValue());
        assertEquals(ValueType.CHARACTER, pt.getValueType());
    }
}

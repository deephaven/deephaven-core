//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl.sources.regioned;

import io.deephaven.chunk.attributes.Values;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ColumnRegionObject}.
 */
public class TstColumnRegionObject {

    public static class TestConstant extends TstColumnRegionPrimative.Constant<ColumnRegionObject<String, Values>> {

        @Before
        public void setUp() throws Exception {
            SUT = new ColumnRegionObject.Constant<>(Long.MAX_VALUE, "value");
        }

        @Override
        @Test
        public void testGet() {
            assertEquals("value", SUT.getObject(0));
            assertEquals("value", SUT.getObject(Long.MAX_VALUE));
        }
    }
}

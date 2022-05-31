package io.deephaven.plot.util;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;

public class TestRange extends BaseArrayTestCase {

    public void testRangeBadOrder() {
        final double min = 1.0;
        final double max = -1.0;

        try {
            final Range r = new Range(min, max);
            fail("Should have raised an error");
        } catch (RequirementFailure e) {
            // pass
        }
    }

    public void testRangeDefault() {
        final double min = -1.0;
        final double max = 1.0;
        final Range r = new Range(min, max);

        assertFalse(r.inRange(-1.1));
        assertTrue(r.inRange(-1.0));
        assertTrue(r.inRange(-0.9));
        assertTrue(r.inRange(+0.5));
        assertTrue(r.inRange(+0.9));
        assertFalse(r.inRange(+1.0));
        assertFalse(r.inRange(+1.1));
    }

    public void testRangeOpen() {
        final double min = -1.0;
        final double max = 1.0;
        final boolean minOpen = true;
        final boolean maxOpen = true;
        final Range r = new Range(min, max, minOpen, maxOpen);

        assertFalse(r.inRange(-1.1));
        assertFalse(r.inRange(-1.0));
        assertTrue(r.inRange(-0.9));
        assertTrue(r.inRange(+0.5));
        assertTrue(r.inRange(+0.9));
        assertFalse(r.inRange(+1.0));
        assertFalse(r.inRange(+1.1));
    }

    public void testRangeClosed() {
        final double min = -1.0;
        final double max = 1.0;
        final boolean minOpen = false;
        final boolean maxOpen = false;
        final Range r = new Range(min, max, minOpen, maxOpen);

        assertFalse(r.inRange(-1.1));
        assertTrue(r.inRange(-1.0));
        assertTrue(r.inRange(-0.9));
        assertTrue(r.inRange(+0.5));
        assertTrue(r.inRange(+0.9));
        assertTrue(r.inRange(+1.0));
        assertFalse(r.inRange(+1.1));
    }
}

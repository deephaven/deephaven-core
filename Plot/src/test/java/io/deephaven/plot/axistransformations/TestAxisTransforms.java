/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.axistransformations;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.time.calendar.BusinessCalendar;
import io.deephaven.time.calendar.Calendars;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TestAxisTransforms extends BaseArrayTestCase {

    final double d1 = 3.5;
    final double d2 = 4.2;
    final double d3 = 5.0;
    final double d4 = 6.2;
    final double d5 = 7.0;
    final double d6 = -d1;
    final double d7 = -d2;
    final double d8 = -d3;
    final double d9 = -d4;
    private final double delta = 0.00001;

    public void testLog() {
        final AxisTransform transform = AxisTransforms.LOG;

        assertEquals(0.0, transform.transform(1.0), delta);
        assertEquals(Math.E, transform.inverseTransform(1.0), delta);
        assertTrue(transform.isVisible(1.1));
        assertFalse(transform.isVisible(0.0));
        assertEquals(d1, transform.inverseTransform(transform.transform(d1)), delta);
        assertEquals(d2, transform.inverseTransform(transform.transform(d2)), delta);
        assertEquals(d3, transform.inverseTransform(transform.transform(d3)), delta);
        assertEquals(d4, transform.inverseTransform(transform.transform(d4)), delta);
        assertEquals(d5, transform.inverseTransform(transform.transform(d5)), delta);
        assertEquals(Double.NaN, transform.inverseTransform(transform.transform(d6)));
        assertEquals(Double.NaN, transform.inverseTransform(transform.transform(d7)));
        assertEquals(Double.NaN, transform.inverseTransform(transform.transform(d8)));
        assertEquals(Double.NaN, transform.inverseTransform(transform.transform(d9)));
    }

    public void testSQRT() {
        final AxisTransform transform = AxisTransforms.SQRT;

        assertEquals(1.0, transform.transform(1.0), delta);
        assertEquals(4.0, transform.inverseTransform(2.0), delta);
        assertTrue(transform.isVisible(1.1));
        assertFalse(transform.isVisible(-0.2));
        assertEquals(d1, transform.inverseTransform(transform.transform(d1)), delta);
        assertEquals(d2, transform.inverseTransform(transform.transform(d2)), delta);
        assertEquals(d3, transform.inverseTransform(transform.transform(d3)), delta);
        assertEquals(d4, transform.inverseTransform(transform.transform(d4)), delta);
        assertEquals(d5, transform.inverseTransform(transform.transform(d5)), delta);
        assertEquals(Double.NaN, transform.inverseTransform(transform.transform(d6)));
        assertEquals(Double.NaN, transform.inverseTransform(transform.transform(d7)));
        assertEquals(Double.NaN, transform.inverseTransform(transform.transform(d8)));
        assertEquals(Double.NaN, transform.inverseTransform(transform.transform(d9)));
    }

    public void testAxisTransformNames() {
        final String[] names = AxisTransforms.axisTransformNames();
        final Set<String> nameSet = new HashSet<>(Arrays.asList(names));
        assertTrue(names.length > 2);
        assertTrue(nameSet.contains("LOG"));
        assertTrue(nameSet.contains("SQRT"));
        assertTrue(nameSet.contains("USNYSE"));
    }

    public void testAxisTransform() {
        assertEquals(AxisTransforms.LOG, AxisTransforms.axisTransform("log"));
        assertEquals(AxisTransforms.LOG, AxisTransforms.axisTransform("LOG"));
        assertEquals(AxisTransforms.SQRT, AxisTransforms.axisTransform("sqrt"));
        assertEquals(AxisTransforms.SQRT, AxisTransforms.axisTransform("SQRT"));

        final BusinessCalendar cal = Calendars.calendar("USNYSE");
        final AxisTransformBusinessCalendar at1 =
                (AxisTransformBusinessCalendar) AxisTransforms.axisTransform("USNYSE");
        assertEquals(cal, at1.getBusinessCalendar());
    }
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.colors;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.gui.color.Color;
import io.deephaven.gui.color.Paint;
import io.deephaven.plot.util.Range;
import groovy.lang.Closure;
import junit.framework.TestCase;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;

public class TestColorMap extends BaseArrayTestCase {

    public void testHeatMap() {
        Function<Double, Color> map = ColorMaps.heatMap(0, 100, new Color(254, 0, 0), new Color(0, 0, 254));
        Color c = map.apply(0.0);
        assertEquals(new Color(254, 0, 0), c);
        c = map.apply(50.0);
        assertEquals(new Color(127, 0, 127), c);
        c = map.apply(100.0);
        assertEquals(new Color(0, 0, 254), c);
        assertNull(map.apply(null));

        // c1 = blue, 0,0,255 c2 = = red 255,0,0 c3 = yellow = 255,255,0
        map = ColorMaps.heatMap(0, 100);
        c = map.apply(0.0);
        assertEquals(new Color(0, 0, 255), c);
        c = map.apply(50.0);
        assertEquals(new Color(127, 0, 127), c);
        c = map.apply(100.0);
        assertEquals(new Color(255, 0, 0), c);
        assertEquals(new Color(255, 255, 0), map.apply(null));

    }

    public void testRangeColor() {
        Range r1 = new Range(0, 50, true, false);
        Range r2 = new Range(50, 100);
        Range r3 = new Range(100, 150);
        Range r4 = new Range(150, 200);
        Color c1 = new Color(255, 0, 0);
        Color c2 = new Color(0, 255, 0);
        Color c3 = new Color(0, 0, 255);
        Color c4 = new Color(255, 255, 255);
        assertTrue(r1.inRange(5.0));
        assertFalse(r1.inRange(0.0));
        assertTrue(r1.inRange(50.0));

        final Map<Range, Color> m = new LinkedHashMap<>();
        m.put(r1, c1);
        m.put(r2, c2);
        m.put(r3, c3);
        m.put(r4, c4);

        Function<Double, Paint> map = ColorMaps.rangeMap(m);
        Paint p = map.apply(0.0);
        assertNull(p);
        p = map.apply(50.0);
        assertEquals(p, new Color(255, 0, 0));
        p = map.apply(100.0);
        assertEquals(p, new Color(0, 0, 255));
        assertNull(map.apply(null));


        try {
            ColorMaps.rangeMap(null);
            TestCase.fail("Expected an exception");
        } catch (RequirementFailure e) {
            assertTrue(e.getMessage().contains("map == null"));
        }
    }

    public void testPredicateColor() {
        Color c1 = new Color(255, 0, 0);
        Color c2 = new Color(0, 255, 0);
        Color c3 = new Color(0, 0, 255);
        Color c4 = new Color(255, 255, 255);

        final Map<ColorMaps.SerializablePredicate<Double>, Color> m = new LinkedHashMap<>();
        m.put(x -> x == 50, c1);
        m.put(x -> false, c2);
        m.put(x -> x == 100.0, c3);
        m.put(x -> false, c4);

        Function<Double, Paint> map = ColorMaps.predicateMap(m);
        Paint p = map.apply(0.0);
        assertNull(p);
        p = map.apply(50.0);
        assertEquals(p, new Color(255, 0, 0));
        p = map.apply(100.0);
        assertEquals(p, new Color(0, 0, 255));
        assertNull(map.apply(null));


        try {
            ColorMaps.rangeMap(null);
            TestCase.fail("Expected an exception");
        } catch (RequirementFailure e) {
            assertTrue(e.getMessage().contains("map == null"));
        }
    }

    public void testClosureColor() {
        Closure<Boolean> r1 = new Closure<Boolean>(null) {
            @Override
            public Boolean call(Object... args) {
                return ((Double) args[0]) == 50.0;
            }
        };
        Closure<Boolean> r2 = new Closure<Boolean>(null) {
            @Override
            public Boolean call(Object... args) {
                return false;
            }
        };
        Closure<Boolean> r3 = new Closure<Boolean>(null) {
            @Override
            public Boolean call(Object... args) {
                return ((Double) args[0]) == 100.0;
            }
        };
        Closure<Boolean> r4 = new Closure<Boolean>(null) {
            @Override
            public Boolean call(Object... args) {
                return false;
            }
        };
        Color c1 = new Color(255, 0, 0);
        Color c2 = new Color(0, 255, 0);
        Color c3 = new Color(0, 0, 255);
        Color c4 = new Color(255, 255, 255);

        final Map<Closure<Boolean>, Color> m = new LinkedHashMap<>();
        m.put(r1, c1);
        m.put(r2, c2);
        m.put(r3, c3);
        m.put(r4, c4);

        Function<Double, Paint> map = ColorMaps.closureMap(m);
        Paint p = map.apply(0.0);
        assertNull(p);
        p = map.apply(50.0);
        assertEquals(p, new Color(255, 0, 0));
        p = map.apply(100.0);
        assertEquals(p, new Color(0, 0, 255));
        assertNull(map.apply(null));


        try {
            ColorMaps.rangeMap(null);
            TestCase.fail("Expected an exception");
        } catch (RequirementFailure e) {
            assertTrue(e.getMessage().contains("map == null"));
        }
    }

}

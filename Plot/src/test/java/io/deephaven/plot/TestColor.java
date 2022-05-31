/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.gui.color.Color;
import junit.framework.TestCase;

public class TestColor extends BaseArrayTestCase {

    public void testColorMisc() {
        assertTrue(Color.colorNames().length > 0);

        final Color c = new Color(0, 0, 0);
        assertEquals(c.javaColor().hashCode(), c.hashCode());
    }

    public void testColorDefinitions() {
        // string
        assertEquals(new Color("BLUE"), new Color(0, 0, 255));
        assertEquals(new Color("BLUE"), new Color("blUe"));
        assertEquals(new Color("BLUE"), new Color("#0000FF"));

        try {
            new Color("Blu e");
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid color name"));
        }

        try {
            new Color(null);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("null"));
        }

        assertEquals(new Color("#FFFFFF"), new Color(255, 255, 255));
        assertEquals(new Color("#FFFFFFF"), new Color(255, 255, 255));



        // rgba
        assertEquals(new Color(1, 2, 3).javaColor(), new java.awt.Color(1, 2, 3));
        assertEquals(new Color(1, 2, 3, 4).javaColor(), new java.awt.Color(1, 2, 3, 4));
        assertEquals(new Color(1, 2, 3, 4).javaColor(), new java.awt.Color(1, 2, 3, 4));
        assertEquals(new Color(1, 2, 3, 4).javaColor(), new java.awt.Color(1, 2, 3, 4));
        assertEquals(new Color(0x11223344).javaColor(), new java.awt.Color(0x11223344));
        assertEquals(new Color(0x11223344, true).javaColor(), new java.awt.Color(0x11223344, true));
        assertEquals(new Color(0.1f, 0.2f, 0.3f).javaColor(), new java.awt.Color(0.1f, 0.2f, 0.3f));
        assertEquals(new Color(0.1f, 0.2f, 0.3f, 0.4f).javaColor(), new java.awt.Color(0.1f, 0.2f, 0.3f, 0.4f));
    }

    public void testStaticHelpers() {
        // string
        assertEquals(Color.color("BLUE"), new Color(0, 0, 255));
        assertEquals(Color.color("BLUE"), Color.color("blUe"));
        assertEquals(Color.color("BLUE"), Color.color("#0000FF"));

        try {
            Color.color("Blu e");
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Invalid color name"));
        }

        try {
            Color.color(null);
            TestCase.fail("Expected an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("null"));
        }

        assertEquals(Color.color("#FFFFFF"), new Color(255, 255, 255));
        assertEquals(Color.color("#FFFFFFF"), new Color(255, 255, 255));



        // rgba
        assertEquals(Color.colorRGB(1, 2, 3).javaColor(), new java.awt.Color(1, 2, 3));
        assertEquals(Color.colorRGB(1, 2, 3, 4).javaColor(), new java.awt.Color(1, 2, 3, 4));
        assertEquals(Color.colorRGB(1, 2, 3, 4).javaColor(), new java.awt.Color(1, 2, 3, 4));
        assertEquals(Color.colorRGB(1, 2, 3, 4).javaColor(), new java.awt.Color(1, 2, 3, 4));
        assertEquals(Color.colorRGB(0x11223344).javaColor(), new java.awt.Color(0x11223344));
        assertEquals(Color.colorRGB(0x11223344, true).javaColor(), new java.awt.Color(0x11223344, true));
        assertEquals(Color.colorRGB(0.1f, 0.2f, 0.3f).javaColor(), new java.awt.Color(0.1f, 0.2f, 0.3f));
        assertEquals(Color.colorRGB(0.1f, 0.2f, 0.3f, 0.4f).javaColor(), new java.awt.Color(0.1f, 0.2f, 0.3f, 0.4f));
        assertEquals(Color.colorHSL(36f, 20, 20).javaColor(), new java.awt.Color(61, 53, 41));
        assertEquals(Color.colorHSL(36f, 20, 20, 0.5f).javaColor(), new java.awt.Color(61, 53, 41, 128));


        try {
            Color.colorHSL(1f, -13, 99, 0.5f);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("out"));
        }

        try {
            Color.colorHSL(1f, 13, 106, 0.5f);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("out"));
        }

        try {
            Color.colorHSL(1f, 13, 99, 1.5f);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("out"));
        }
    }

    public void testEquals() {
        final Color color1 = new Color(0, 0, 0, 0);
        final Color color2 = new Color(0, 0, 0, 0);
        final Color color3 = new Color(1, 0, 0, 0);
        final Color color4 = new Color(0, 1, 0, 0);
        final Color color5 = new Color(0, 0, 1, 0);
        final Color color6 = new Color(0, 0, 0, 1);

        assertEquals(color1, color2);
        assertFalse(color1.equals(color3));
        assertFalse(color1.equals(color4));
        assertFalse(color1.equals(color5));
        assertFalse(color1.equals(color6));
    }
}

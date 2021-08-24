/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot;

import io.deephaven.base.testing.BaseArrayTestCase;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class TestFont extends BaseArrayTestCase {

    public void testFontMisc() {
        assertTrue(Font.fontFamilyNames().length > 2);
        assertTrue(Font.fontStyleNames().length > 2);
    }

    public void testStyle() {
        try {
            // noinspection ConstantConditions
            Font.fontStyle(null);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("null"));
        }

        assertEquals(Font.FontStyle.PLAIN.mask(), java.awt.Font.PLAIN);
        assertEquals(Font.FontStyle.BOLD.mask(), java.awt.Font.BOLD);
        assertEquals(Font.FontStyle.ITALIC.mask(), java.awt.Font.ITALIC);
        assertEquals(Font.FontStyle.BOLD_ITALIC.mask(), java.awt.Font.BOLD | java.awt.Font.ITALIC);



        assertEquals(Font.FontStyle.PLAIN, Font.fontStyle("plain"));
        assertEquals(Font.FontStyle.PLAIN, Font.fontStyle("PLAIN"));
        assertEquals(Font.FontStyle.PLAIN, Font.fontStyle("p"));
        assertEquals(Font.FontStyle.BOLD, Font.fontStyle("bold"));
        assertEquals(Font.FontStyle.BOLD, Font.fontStyle("BOLD"));
        assertEquals(Font.FontStyle.BOLD, Font.fontStyle("b"));
        assertEquals(Font.FontStyle.ITALIC, Font.fontStyle("italic"));
        assertEquals(Font.FontStyle.ITALIC, Font.fontStyle("ITALIC"));
        assertEquals(Font.FontStyle.ITALIC, Font.fontStyle("i"));
        assertEquals(Font.FontStyle.BOLD_ITALIC, Font.fontStyle("bold_italic"));
        assertEquals(Font.FontStyle.BOLD_ITALIC, Font.fontStyle("BOLD_ITALIC"));
        assertEquals(Font.FontStyle.BOLD_ITALIC, Font.fontStyle("bi"));
        assertEquals(Font.FontStyle.BOLD_ITALIC, Font.fontStyle("ib"));

        final Set<String> target = new HashSet<>();
        target.addAll(
            Arrays.asList("PLAIN", "BOLD", "ITALIC", "BOLD_ITALIC", "P", "B", "I", "BI", "IB"));

        assertEquals(target, new HashSet<String>(Arrays.asList(Font.fontStyleNames())));
    }

    public void testConstructors() {
        assertEquals(new Font("Ariel", Font.FontStyle.PLAIN, 10).javaFont(),
            new java.awt.Font("Ariel", java.awt.Font.PLAIN, 10));
        assertEquals(new Font("Ariel", "PLAIN", 10).javaFont(),
            new java.awt.Font("Ariel", java.awt.Font.PLAIN, 10));
        assertEquals(new Font("Ariel", "P", 10).javaFont(),
            new java.awt.Font("Ariel", java.awt.Font.PLAIN, 10));


        assertEquals(Font.font("Ariel", Font.FontStyle.PLAIN, 10).javaFont(),
            new java.awt.Font("Ariel", java.awt.Font.PLAIN, 10));
        assertEquals(Font.font("Ariel", "PLAIN", 10).javaFont(),
            new java.awt.Font("Ariel", java.awt.Font.PLAIN, 10));
        assertEquals(Font.font("Ariel", "P", 10).javaFont(),
            new java.awt.Font("Ariel", java.awt.Font.PLAIN, 10));
    }

    public void testTransforms() {
        Font f = new Font("Ariel", Font.FontStyle.PLAIN, 10);

        f = f.resize(12);
        assertEquals(f.javaFont(), new java.awt.Font("Ariel", java.awt.Font.PLAIN, 12));

        f = f.restyle("bold");
        assertEquals(f.javaFont(), new java.awt.Font("Ariel", java.awt.Font.BOLD, 12));

        f = f.restyle("plain");
        assertEquals(f.javaFont(), new java.awt.Font("Ariel", java.awt.Font.PLAIN, 12));

        f = f.refamily("SanSerif");
        assertEquals(f.javaFont(), new java.awt.Font("SanSerif", java.awt.Font.PLAIN, 12));
    }

    public void testEquals() {
        final Font font1 = new Font("Arial", Font.FontStyle.BOLD, 12);
        final Font font2 = new Font("Arial", Font.FontStyle.BOLD, 12);
        final Font font3 = new Font("SanSerif", Font.FontStyle.BOLD, 12);
        final Font font4 = new Font("Arial", Font.FontStyle.PLAIN, 12);
        final Font font5 = new Font("Arial", Font.FontStyle.BOLD, 11);

        assertEquals(font1, font2);
        assertFalse(font1.equals(font3));
        assertFalse(font1.equals(font4));
        assertFalse(font1.equals(font5));
    }
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.themes;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.gui.color.Color;
import io.deephaven.db.plot.Font;
import io.deephaven.gui.color.Paint;
import io.deephaven.db.plot.Theme;
import junit.framework.TestCase;

public class TestTheme extends BaseArrayTestCase {
    private final Theme theme = Themes.theme();

    public void testTheme() {
        ThemeImpl theme2 = (ThemeImpl) theme.copy();
        theme2.figureTitleFont(null);
        Font f = theme2.getFigureTitleFont();
        assertEquals(new Font("SanSerif", Font.FontStyle.PLAIN, 14).toString(), f.toString());

        theme2.figureTextFont(null);
        try {
            theme2.getFigureTitleFont();
            TestCase.fail("Expected an Exception");
        } catch(UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("found"));
        }


        theme2.figureTitleColor((Paint) null);
        Paint p = theme2.getFigureTitleColor();
        assertEquals(new java.awt.Color(65, 65, 65), p.javaColor());

        theme2.figureTitleColor("RED");
        p = theme2.getFigureTitleColor();
        assertEquals(Color.color("RED").javaColor(), p.javaColor());


        theme2.chartBackgroundColor((Paint) null);
        try {
            theme2.getChartBackgroundColor();
            TestCase.fail("Expected an Exception");
        } catch(UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("found"));
        }

    }

    public void testEquals() {
        final Theme light1 = Themes.theme("LIGHT");
        final Theme light2 = Themes.theme("LIGHT");
        final Theme dark = Themes.theme("DARK");

        assertEquals(light1, light2);
        assertFalse(light1.equals(dark));
        assertFalse(light1.equals((Theme) null));
    }
}

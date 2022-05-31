/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.gui.color.Color;

public class TestPaint extends BaseArrayTestCase {

    public void testComponenet() {
        Color c = new Color(0, 0, 0);
        Color c2 = new Color(1, 1, 1);

        assertEquals(c.javaColor(), new java.awt.Color(0, 0, 0));
        assertEquals(c2.javaColor(), new java.awt.Color(1, 1, 1));
    }
}

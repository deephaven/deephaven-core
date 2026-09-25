//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot;

import io.deephaven.gui.color.Color;
import org.junit.Test;

import static org.junit.Assert.*;

public class TestPaint {

    @Test
    public void testComponenet() {
        Color c = new Color(0, 0, 0);
        Color c2 = new Color(1, 1, 1);

        assertEquals(c.javaColor(), new java.awt.Color(0, 0, 0));
        assertEquals(c2.javaColor(), new java.awt.Color(1, 1, 1));
    }
}

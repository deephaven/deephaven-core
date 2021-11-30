/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.plot.axisformatters;

import io.deephaven.base.testing.BaseArrayTestCase;

import java.text.NumberFormat;

public class TestDecimalAxisFormat extends BaseArrayTestCase {

    public void testFormat() {
        final DecimalAxisFormat format = new DecimalAxisFormat();
        final NumberFormat f = format.getNumberFormatter();
        assertEquals(f.format(11123), "11,123");
        assertEquals(f.format(11123.45), "11,123.45");
    }

    public void testFormatPattern() {
        final DecimalAxisFormat format = new DecimalAxisFormat();
        format.setPattern("#,#00.00 $MM");
        final NumberFormat f = format.getNumberFormatter();
        assertEquals(f.format(3), "03.00 $MM");
        assertEquals(f.format(3.45), "03.45 $MM");
        assertEquals(f.format(11123), "11,123.00 $MM");
        assertEquals(f.format(11123.45), "11,123.45 $MM");
    }
}

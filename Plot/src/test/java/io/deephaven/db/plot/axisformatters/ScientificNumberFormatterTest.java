/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.axisformatters;

import io.deephaven.base.testing.BaseArrayTestCase;

import java.text.FieldPosition;

public class ScientificNumberFormatterTest extends BaseArrayTestCase {

    public void testFormat() {
        final ScientificNumberFormatter formatter = new ScientificNumberFormatter(3, 1E-5, 1E5);

        // less than the max number of decimals
        assertEquals(formatter.format(3.5, new StringBuffer(), new FieldPosition(0)).toString(),
            "3.5");

        // more than the max number of decimals, rounded down
        assertEquals(formatter.format(3.5234, new StringBuffer(), new FieldPosition(0)).toString(),
            "3.523");

        // more than the max number of decimals, rounded up
        assertEquals(formatter.format(3.5238, new StringBuffer(), new FieldPosition(0)).toString(),
            "3.524");

        // smaller than lower limit
        assertEquals(formatter.format(3E-6, new StringBuffer(), new FieldPosition(0)).toString(),
            "3E-6");
        assertEquals(
            formatter.format(0.000003, new StringBuffer(), new FieldPosition(0)).toString(),
            "3E-6");
        assertEquals(
            formatter.format(-0.000003, new StringBuffer(), new FieldPosition(0)).toString(),
            "-3E-6");

        // smaller than lower limit, more decimal points than max number of decimals
        assertEquals(
            formatter.format(0.00000334534, new StringBuffer(), new FieldPosition(0)).toString(),
            "3.345E-6");

        // larger than upper limit
        assertEquals(formatter.format(3E6, new StringBuffer(), new FieldPosition(0)).toString(),
            "3E6");
        assertEquals(formatter.format(3000000, new StringBuffer(), new FieldPosition(0)).toString(),
            "3E6");

        // larger than upper limit, more decimal points than max number of decimals
        assertEquals(
            formatter.format(334584234, new StringBuffer(), new FieldPosition(0)).toString(),
            "3.346E8");

        // long larger than upper limit, more decimal points than max number of decimals
        assertEquals(
            formatter.format(33458423423423L, new StringBuffer(), new FieldPosition(0)).toString(),
            "3.346E13");
        assertEquals(
            formatter.format(-33458423423423L, new StringBuffer(), new FieldPosition(0)).toString(),
            "-3.346E13");
    }
}

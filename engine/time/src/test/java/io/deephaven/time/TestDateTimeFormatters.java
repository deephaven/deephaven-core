//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time;

import io.deephaven.base.testing.BaseArrayTestCase;
import junit.framework.TestCase;

public class TestDateTimeFormatters extends BaseArrayTestCase {

    public void testAll() {
        final boolean isISO = true;
        final boolean hasDate = true;
        final boolean hasTime = true;
        final int subsecondDigits = 9;
        final boolean hasTZ = true;
        final DateTimeFormatter dtf1 = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);
        final DateTimeFormatters dtf2 = DateTimeFormatters.ISO9TZ;

        TestCase.assertEquals(dtf1.getPattern(), dtf2.getFormatter().getPattern());
        TestCase.assertEquals(dtf1.toString(), dtf2.toString());
    }
}

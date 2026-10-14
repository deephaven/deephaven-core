//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time;

import org.junit.Test;

import static org.junit.Assert.*;

public class TestDateTimeFormatters {

    @Test
    public void testAll() {
        final boolean isISO = true;
        final boolean hasDate = true;
        final boolean hasTime = true;
        final int subsecondDigits = 9;
        final boolean hasTZ = true;
        final DateTimeFormatter dtf1 = new DateTimeFormatter(isISO, hasDate, hasTime, subsecondDigits, hasTZ);
        final DateTimeFormatters dtf2 = DateTimeFormatters.ISO9TZ;

        assertEquals(dtf1.getPattern(), dtf2.getFormatter().getPattern());
        assertEquals(dtf1.toString(), dtf2.toString());
    }
}

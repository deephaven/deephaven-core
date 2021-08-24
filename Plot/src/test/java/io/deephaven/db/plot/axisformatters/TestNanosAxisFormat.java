/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.db.plot.axisformatters;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.db.tables.utils.DBDateTime;
import io.deephaven.db.tables.utils.DBTimeUtils;
import io.deephaven.db.tables.utils.DBTimeZone;
import junit.framework.TestCase;

import java.text.NumberFormat;

public class TestNanosAxisFormat extends BaseArrayTestCase {

    public void testFormat() {
        final NanosAxisFormat nyFormat = new NanosAxisFormat();
        final NanosAxisFormat tokyoFormat = new NanosAxisFormat(DBTimeZone.TZ_JP);
        final NumberFormat nyNumberFormat = nyFormat.getNumberFormatter();
        final NumberFormat tokyoNumberFormat = tokyoFormat.getNumberFormatter();

        final DBDateTime time = new DBDateTime(DBTimeUtils.YEAR);
        final long lNanos = time.getNanos();
        final double dNanos = lNanos;
        assertEquals(nyNumberFormat.format(lNanos), time.toDateString(DBTimeZone.TZ_DEFAULT));
        assertEquals(nyNumberFormat.format(dNanos), time.toDateString(DBTimeZone.TZ_DEFAULT));
        assertEquals(tokyoNumberFormat.format(lNanos), time.toDateString(DBTimeZone.TZ_JP));
        assertEquals(tokyoNumberFormat.format(dNanos), time.toDateString(DBTimeZone.TZ_JP));

        try {
            nyNumberFormat.parse("TEST", null);
            TestCase.fail("Expected an exception");
        } catch (UnsupportedOperationException e) {
            assertTrue(e.getMessage().contains("Not implemented"));
        }
    }

    public void testFormatString() {
        final DBDateTime time = DBTimeUtils.convertDateTime("2017-03-24T14:32:12.345678 MN");

        final NanosAxisFormat formatMN = new NanosAxisFormat(DBTimeZone.TZ_MN);
        final NanosAxisFormat formatNY = new NanosAxisFormat(DBTimeZone.TZ_NY);

        assertEquals("2017-03-24", formatMN.getNumberFormatter().format(time.getNanos()));
        assertEquals("2017-03-24", formatNY.getNumberFormatter().format(time.getNanos()));

        formatMN.setPattern("yyyy-MM-dd'T'HH:mm");
        formatNY.setPattern("yyyy-MM-dd'T'HH:mm");

        assertEquals("2017-03-24T14:32", formatMN.getNumberFormatter().format(time.getNanos()));
        assertEquals("2017-03-24T15:32", formatNY.getNumberFormatter().format(time.getNanos()));

        formatMN.setPattern("HH:mm:ss.SSSS");
        formatNY.setPattern("HH:mm:ss.SSSS");

        assertEquals("14:32:12.3456", formatMN.getNumberFormatter().format(time.getNanos()));
        assertEquals("15:32:12.3456", formatNY.getNumberFormatter().format(time.getNanos()));

        try {
            formatMN.setPattern("junkpattern");
        } catch (IllegalArgumentException e) {
            // pass
        }

        formatNY.setPattern(null);
        formatMN.setPattern(null);

        assertEquals("2017-03-24", formatMN.getNumberFormatter().format(time.getNanos()));
        assertEquals("2017-03-24", formatNY.getNumberFormatter().format(time.getNanos()));
    }
}

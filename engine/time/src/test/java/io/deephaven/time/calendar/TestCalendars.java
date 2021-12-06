/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.time.calendar;

import io.deephaven.base.testing.BaseArrayTestCase;
import io.deephaven.configuration.Configuration;
import io.deephaven.time.DateTime;
import io.deephaven.time.DateTimeUtils;

import java.util.HashMap;
import java.util.Map;

public class TestCalendars extends BaseArrayTestCase {

    public void testIsBusinessDay() {
        BusinessCalendar usnyse = Calendars.calendar("USNYSE");
        BusinessCalendar usny = Calendars.calendar("USNY");

        // USNYSE
        DateTime businessDay = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 NY");
        DateTime halfDay = DateTimeUtils.convertDateTime("2014-07-03T01:00:00.000000000 NY");
        DateTime holiday = DateTimeUtils.convertDateTime("2002-01-01T01:00:00.000000000 NY");
        DateTime holiday2 = DateTimeUtils.convertDateTime("2002-01-21T01:00:00.000000000 NY");

        assertTrue(usnyse.isBusinessDay(businessDay));
        assertTrue(usnyse.isBusinessDay(halfDay));
        assertFalse(usnyse.isBusinessDay(holiday));
        assertFalse(usnyse.isBusinessDay(holiday2));

        // USNY
        businessDay = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 NY");
        holiday = DateTimeUtils.convertDateTime("2005-11-24T01:00:00.000000000 NY");
        holiday2 = DateTimeUtils.convertDateTime("2002-01-21T01:00:00.000000000 NY");

        assertTrue(usny.isBusinessDay(businessDay));
        assertFalse(usny.isBusinessDay(holiday));
        assertTrue(usny.isBusinessDay(holiday2));
    }

    public void testGetDefault() {
        Configuration.getInstance().setProperty("businessCalendar.default", "USNYSE");
        BusinessCalendar calendars = Calendars.calendar();
        // USNYSE
        DateTime businessDay = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 NY");
        DateTime halfDay = DateTimeUtils.convertDateTime("2014-07-03T01:00:00.000000000 NY");
        DateTime holiday = DateTimeUtils.convertDateTime("2002-01-01T01:00:00.000000000 NY");
        DateTime holiday2 = DateTimeUtils.convertDateTime("2002-01-21T01:00:00.000000000 NY");

        assertTrue(calendars.isBusinessDay(businessDay));
        assertTrue(calendars.isBusinessDay(halfDay));
        assertFalse(calendars.isBusinessDay(holiday));
        assertFalse(calendars.isBusinessDay(holiday2));
    }

    public void testGetInstance() {
        BusinessCalendar usnyse = Calendars.calendar("USNYSE");
        DateTime businessDay = DateTimeUtils.convertDateTime("2016-08-31T01:00:00.000000000 NY");
        DateTime halfDay = DateTimeUtils.convertDateTime("2014-07-03T01:00:00.000000000 NY");
        DateTime holiday = DateTimeUtils.convertDateTime("2002-01-01T01:00:00.000000000 NY");
        DateTime holiday2 = DateTimeUtils.convertDateTime("2002-01-21T01:00:00.000000000 NY");

        assertTrue(usnyse.isBusinessDay(businessDay));
        assertTrue(usnyse.isBusinessDay(halfDay));
        assertFalse(usnyse.isBusinessDay(holiday));
        assertFalse(usnyse.isBusinessDay(holiday2));
    }

    public void testMapMethods() {
        Calendars calendars = Calendars.getInstance();

        final Map<String, BusinessCalendar> values = new HashMap<>(calendars);

        assertTrue(calendars.size() > 0);
        assertEquals(calendars.keySet().size(), calendars.size());
        assertTrue(calendars.containsKey("USNYSE"));
        assertTrue(calendars.containsKey("usnyse"));
        assertFalse(calendars.containsKey("usnys"));
        calendars.remove("USNYSE");
        assertFalse(calendars.containsKey("USNYSE"));
        calendars.clear();
        assertTrue(calendars.isEmpty());

        // replacing values so that other unit tests don't fail
        calendars.putAll(values);
    }

    public void testEquals() {
        final BusinessCalendar usnyse1 = Calendars.calendar("USNYSE");
        final BusinessCalendar usnyse2 = Calendars.calendar("USNYSE");
        final BusinessCalendar jpose = Calendars.calendar("JPOSE");

        assertEquals(usnyse1, usnyse2);
        assertFalse(usnyse1.equals(jpose));
        assertFalse(usnyse1.equals((Calendar) null));
    }
}

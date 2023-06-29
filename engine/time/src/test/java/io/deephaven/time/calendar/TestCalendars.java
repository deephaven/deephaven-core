/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

import io.deephaven.time.DateTimeUtils;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestCalendars {

    /*
     * TODO (https://github.com/deephaven/deephaven-core/issues/3958): Determine how best to restore these tests once
     * calendars have been updated. Maybe we can install some test-only calendars that don't map to real calendars, and
     * an appropriate default calendar from amongst the test calendars.
     */

    @Ignore
    @Test
    public void testIsBusinessDay() {
        BusinessCalendar usnyse = Calendars.calendar("USNYSE");
        BusinessCalendar usny = Calendars.calendar("USNY");

        // USNYSE
        Instant businessDay = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 NY");
        Instant halfDay = DateTimeUtils.parseInstant("2014-07-03T01:00:00.000000000 NY");
        Instant holiday = DateTimeUtils.parseInstant("2002-01-01T01:00:00.000000000 NY");
        Instant holiday2 = DateTimeUtils.parseInstant("2002-01-21T01:00:00.000000000 NY");

        assertTrue(usnyse.isBusinessDay(businessDay));
        assertTrue(usnyse.isBusinessDay(halfDay));
        assertFalse(usnyse.isBusinessDay(holiday));
        assertFalse(usnyse.isBusinessDay(holiday2));

        // USNY
        businessDay = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 NY");
        holiday = DateTimeUtils.parseInstant("2005-11-24T01:00:00.000000000 NY");
        holiday2 = DateTimeUtils.parseInstant("2002-01-21T01:00:00.000000000 NY");

        assertTrue(usny.isBusinessDay(businessDay));
        assertFalse(usny.isBusinessDay(holiday));
        assertTrue(usny.isBusinessDay(holiday2));
    }

    @Ignore
    @Test
    public void testGetDefault() {
        // Default is configured via a configuration property and cannot be changed once Calendars is statically
        // initialized
        assertEquals(Calendars.getDefaultName(), "USNYSE");
        BusinessCalendar calendars = Calendars.calendar();
        // USNYSE
        Instant businessDay = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 NY");
        Instant halfDay = DateTimeUtils.parseInstant("2014-07-03T01:00:00.000000000 NY");
        Instant holiday = DateTimeUtils.parseInstant("2002-01-01T01:00:00.000000000 NY");
        Instant holiday2 = DateTimeUtils.parseInstant("2002-01-21T01:00:00.000000000 NY");

        assertTrue(calendars.isBusinessDay(businessDay));
        assertTrue(calendars.isBusinessDay(halfDay));
        assertFalse(calendars.isBusinessDay(holiday));
        assertFalse(calendars.isBusinessDay(holiday2));
    }

    @Ignore
    @Test
    public void testGetInstance() {
        BusinessCalendar usnyse = Calendars.calendar("USNYSE");
        Instant businessDay = DateTimeUtils.parseInstant("2016-08-31T01:00:00.000000000 NY");
        Instant halfDay = DateTimeUtils.parseInstant("2014-07-03T01:00:00.000000000 NY");
        Instant holiday = DateTimeUtils.parseInstant("2002-01-01T01:00:00.000000000 NY");
        Instant holiday2 = DateTimeUtils.parseInstant("2002-01-21T01:00:00.000000000 NY");

        assertTrue(usnyse.isBusinessDay(businessDay));
        assertTrue(usnyse.isBusinessDay(halfDay));
        assertFalse(usnyse.isBusinessDay(holiday));
        assertFalse(usnyse.isBusinessDay(holiday2));
    }

    @Ignore
    @Test
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

    @Ignore
    @Test
    public void testEquals() {
        final BusinessCalendar usnyse1 = Calendars.calendar("USNYSE");
        final BusinessCalendar usnyse2 = Calendars.calendar("USNYSE");
        final BusinessCalendar jpose = Calendars.calendar("JPOSE");

        assertEquals(usnyse1, usnyse2);
        assertFalse(usnyse1.equals(jpose));
        assertFalse(usnyse1.equals(null));
    }
}

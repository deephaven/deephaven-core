/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

final class CalendarInit {

    static {
        for (BusinessCalendar calendar : Calendars.calendarsFromConfiguration()) {
            Calendars.addCalendar(calendar);
        }
    }

    static void noop() {

    }
}

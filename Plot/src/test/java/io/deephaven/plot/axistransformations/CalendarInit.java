/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.plot.axistransformations;

import io.deephaven.time.calendar.BusinessCalendar;
import io.deephaven.time.calendar.Calendars;

final class CalendarInit {
    static {
        for (BusinessCalendar calendar : Calendars.calendarsFromConfiguration()) {
            Calendars.addCalendar(calendar);
        }
    }

    static void noop() {

    }
}

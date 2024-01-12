/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.integrations.python;

import io.deephaven.time.calendar.BusinessCalendar;
import io.deephaven.time.calendar.Calendars;

public final class CalendarsHelper {
    public static void addCalendarsFromConfiguration() {
        for (BusinessCalendar calendar : Calendars.calendarsFromConfiguration()) {
            Calendars.addCalendar(calendar);
        }
    }
}

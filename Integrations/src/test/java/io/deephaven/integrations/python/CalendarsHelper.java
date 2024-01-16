/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.integrations.python;

import io.deephaven.time.calendar.BusinessCalendar;
import io.deephaven.time.calendar.Calendars;
import io.deephaven.util.annotations.ScriptApi;

public final class CalendarsHelper {

    /**
     * This invokes {@link Calendars#addCalendar(BusinessCalendar)} for all the
     * {@link Calendars#calendarsFromConfiguration()}.
     */
    @ScriptApi
    public static void addCalendarsFromConfiguration() {
        for (BusinessCalendar calendar : Calendars.calendarsFromConfiguration()) {
            Calendars.addCalendar(calendar);
        }
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.time.calendar;

public final class CalendarInit {

    private static volatile boolean initialized = false;

    /**
     * This is a guarded initialization of {@link Calendars#addCalendar(BusinessCalendar)} for all the
     * {@link Calendars#calendarsFromConfiguration()}.
     */
    public static void init() {
        if (!initialized) {
            synchronized (CalendarInit.class) {
                if (!initialized) {
                    for (BusinessCalendar calendar : Calendars.calendarsFromConfiguration()) {
                        Calendars.addCalendar(calendar);
                    }
                    initialized = true;
                }
            }
        }
    }
}

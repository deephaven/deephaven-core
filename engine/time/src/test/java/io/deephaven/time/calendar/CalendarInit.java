/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.time.calendar;

final class CalendarInit {

    private static volatile boolean initialized = false;

    /**
     * This is a guarded initialization of {@link Calendars#addCalendar(BusinessCalendar)} for all the
     * {@link Calendars#calendarsFromConfiguration()}.
     */
    static void init() {
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

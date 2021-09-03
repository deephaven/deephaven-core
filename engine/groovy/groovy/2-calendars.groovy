///////////////////// Calendars /////////////////////
import static io.deephaven.util.calendar.Calendars.calendar
import static io.deephaven.util.calendar.Calendars.calendarNames

//todo prefix name with CALENDAR?

publishVariable( "CALENDAR_DEFAULT", calendar())

for( String n : calendarNames() ) {
    publishVariable("CALENDAR_" + n, calendar(n))
}
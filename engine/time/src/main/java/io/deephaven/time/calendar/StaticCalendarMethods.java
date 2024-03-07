//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Run StaticCalendarMethodsGenerator or "./gradlew :Generators:generateStaticCalendarMethods" to regenerate
//
// @formatter:off
package io.deephaven.time.calendar;

import io.deephaven.time.calendar.BusinessCalendar;
import io.deephaven.time.calendar.Calendar;
import io.deephaven.time.calendar.CalendarDay;
import java.lang.String;
import java.time.DayOfWeek;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Map;
import java.util.Set;

/**
 * Static versions of business calendar methods that use the default business calendar.
 *
 * @see io.deephaven.time.calendar
 * @see io.deephaven.time.calendar.Calendar
 * @see io.deephaven.time.calendar.BusinessCalendar
 * @see io.deephaven.time.calendar.Calendars
 */
public class StaticCalendarMethods {
    /** @see io.deephaven.time.calendar.BusinessCalendar#businessDates(java.lang.String,java.lang.String) */
    public static  java.lang.String[] businessDates( java.lang.String start, java.lang.String end ) {return Calendars.calendar().businessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#businessDates(java.time.Instant,java.time.Instant) */
    public static  java.time.LocalDate[] businessDates( java.time.Instant start, java.time.Instant end ) {return Calendars.calendar().businessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#businessDates(java.time.LocalDate,java.time.LocalDate) */
    public static  java.time.LocalDate[] businessDates( java.time.LocalDate start, java.time.LocalDate end ) {return Calendars.calendar().businessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#businessDates(java.time.ZonedDateTime,java.time.ZonedDateTime) */
    public static  java.time.LocalDate[] businessDates( java.time.ZonedDateTime start, java.time.ZonedDateTime end ) {return Calendars.calendar().businessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#businessDates(java.lang.String,java.lang.String,boolean,boolean) */
    public static  java.lang.String[] businessDates( java.lang.String start, java.lang.String end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().businessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#businessDates(java.time.Instant,java.time.Instant,boolean,boolean) */
    public static  java.time.LocalDate[] businessDates( java.time.Instant start, java.time.Instant end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().businessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#businessDates(java.time.LocalDate,java.time.LocalDate,boolean,boolean) */
    public static  java.time.LocalDate[] businessDates( java.time.LocalDate start, java.time.LocalDate end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().businessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#businessDates(java.time.ZonedDateTime,java.time.ZonedDateTime,boolean,boolean) */
    public static  java.time.LocalDate[] businessDates( java.time.ZonedDateTime start, java.time.ZonedDateTime end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().businessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.Calendar#calendarDate() */
    public static  java.time.LocalDate calendarDate( ) {return Calendars.calendar().calendarDate( );}

    /** @see io.deephaven.time.calendar.Calendar#calendarDates(java.lang.String,java.lang.String) */
    public static  java.lang.String[] calendarDates( java.lang.String start, java.lang.String end ) {return Calendars.calendar().calendarDates( start, end );}

    /** @see io.deephaven.time.calendar.Calendar#calendarDates(java.time.Instant,java.time.Instant) */
    public static  java.time.LocalDate[] calendarDates( java.time.Instant start, java.time.Instant end ) {return Calendars.calendar().calendarDates( start, end );}

    /** @see io.deephaven.time.calendar.Calendar#calendarDates(java.time.LocalDate,java.time.LocalDate) */
    public static  java.time.LocalDate[] calendarDates( java.time.LocalDate start, java.time.LocalDate end ) {return Calendars.calendar().calendarDates( start, end );}

    /** @see io.deephaven.time.calendar.Calendar#calendarDates(java.time.ZonedDateTime,java.time.ZonedDateTime) */
    public static  java.time.LocalDate[] calendarDates( java.time.ZonedDateTime start, java.time.ZonedDateTime end ) {return Calendars.calendar().calendarDates( start, end );}

    /** @see io.deephaven.time.calendar.Calendar#calendarDates(java.lang.String,java.lang.String,boolean,boolean) */
    public static  java.lang.String[] calendarDates( java.lang.String start, java.lang.String end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().calendarDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.Calendar#calendarDates(java.time.Instant,java.time.Instant,boolean,boolean) */
    public static  java.time.LocalDate[] calendarDates( java.time.Instant start, java.time.Instant end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().calendarDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.Calendar#calendarDates(java.time.LocalDate,java.time.LocalDate,boolean,boolean) */
    public static  java.time.LocalDate[] calendarDates( java.time.LocalDate start, java.time.LocalDate end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().calendarDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.Calendar#calendarDates(java.time.ZonedDateTime,java.time.ZonedDateTime,boolean,boolean) */
    public static  java.time.LocalDate[] calendarDates( java.time.ZonedDateTime start, java.time.ZonedDateTime end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().calendarDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#calendarDay() */
    public static  io.deephaven.time.calendar.CalendarDay<java.time.Instant> calendarDay( ) {return Calendars.calendar().calendarDay( );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#calendarDay(java.lang.String) */
    public static  io.deephaven.time.calendar.CalendarDay<java.time.Instant> calendarDay( java.lang.String date ) {return Calendars.calendar().calendarDay( date );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#calendarDay(java.time.Instant) */
    public static  io.deephaven.time.calendar.CalendarDay<java.time.Instant> calendarDay( java.time.Instant time ) {return Calendars.calendar().calendarDay( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#calendarDay(java.time.LocalDate) */
    public static  io.deephaven.time.calendar.CalendarDay<java.time.Instant> calendarDay( java.time.LocalDate date ) {return Calendars.calendar().calendarDay( date );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#calendarDay(java.time.ZonedDateTime) */
    public static  io.deephaven.time.calendar.CalendarDay<java.time.Instant> calendarDay( java.time.ZonedDateTime time ) {return Calendars.calendar().calendarDay( time );}

    /** @see io.deephaven.time.calendar.Calendar#dayOfWeek() */
    public static  java.time.DayOfWeek calendarDayOfWeek( ) {return Calendars.calendar().dayOfWeek( );}

    /** @see io.deephaven.time.calendar.Calendar#dayOfWeek(java.lang.String) */
    public static  java.time.DayOfWeek calendarDayOfWeek( java.lang.String date ) {return Calendars.calendar().dayOfWeek( date );}

    /** @see io.deephaven.time.calendar.Calendar#dayOfWeek(java.time.Instant) */
    public static  java.time.DayOfWeek calendarDayOfWeek( java.time.Instant time ) {return Calendars.calendar().dayOfWeek( time );}

    /** @see io.deephaven.time.calendar.Calendar#dayOfWeek(java.time.LocalDate) */
    public static  java.time.DayOfWeek calendarDayOfWeek( java.time.LocalDate date ) {return Calendars.calendar().dayOfWeek( date );}

    /** @see io.deephaven.time.calendar.Calendar#dayOfWeek(java.time.ZonedDateTime) */
    public static  java.time.DayOfWeek calendarDayOfWeek( java.time.ZonedDateTime time ) {return Calendars.calendar().dayOfWeek( time );}

    /** @see io.deephaven.time.calendar.Calendar#dayOfWeekValue() */
    public static  int calendarDayOfWeekValue( ) {return Calendars.calendar().dayOfWeekValue( );}

    /** @see io.deephaven.time.calendar.Calendar#dayOfWeekValue(java.lang.String) */
    public static  int calendarDayOfWeekValue( java.lang.String date ) {return Calendars.calendar().dayOfWeekValue( date );}

    /** @see io.deephaven.time.calendar.Calendar#dayOfWeekValue(java.time.Instant) */
    public static  int calendarDayOfWeekValue( java.time.Instant time ) {return Calendars.calendar().dayOfWeekValue( time );}

    /** @see io.deephaven.time.calendar.Calendar#dayOfWeekValue(java.time.LocalDate) */
    public static  int calendarDayOfWeekValue( java.time.LocalDate date ) {return Calendars.calendar().dayOfWeekValue( date );}

    /** @see io.deephaven.time.calendar.Calendar#dayOfWeekValue(java.time.ZonedDateTime) */
    public static  int calendarDayOfWeekValue( java.time.ZonedDateTime time ) {return Calendars.calendar().dayOfWeekValue( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#diffBusinessDays(java.time.Instant,java.time.Instant) */
    public static  double diffBusinessDays( java.time.Instant start, java.time.Instant end ) {return Calendars.calendar().diffBusinessDays( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#diffBusinessDays(java.time.ZonedDateTime,java.time.ZonedDateTime) */
    public static  double diffBusinessDays( java.time.ZonedDateTime start, java.time.ZonedDateTime end ) {return Calendars.calendar().diffBusinessDays( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#diffBusinessDuration(java.time.Instant,java.time.Instant) */
    public static  java.time.Duration diffBusinessDuration( java.time.Instant start, java.time.Instant end ) {return Calendars.calendar().diffBusinessDuration( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#diffBusinessDuration(java.time.ZonedDateTime,java.time.ZonedDateTime) */
    public static  java.time.Duration diffBusinessDuration( java.time.ZonedDateTime start, java.time.ZonedDateTime end ) {return Calendars.calendar().diffBusinessDuration( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#diffBusinessNanos(java.time.Instant,java.time.Instant) */
    public static  long diffBusinessNanos( java.time.Instant start, java.time.Instant end ) {return Calendars.calendar().diffBusinessNanos( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#diffBusinessNanos(java.time.ZonedDateTime,java.time.ZonedDateTime) */
    public static  long diffBusinessNanos( java.time.ZonedDateTime start, java.time.ZonedDateTime end ) {return Calendars.calendar().diffBusinessNanos( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#diffBusinessYears(java.time.Instant,java.time.Instant) */
    public static  double diffBusinessYears( java.time.Instant start, java.time.Instant end ) {return Calendars.calendar().diffBusinessYears( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#diffBusinessYears(java.time.ZonedDateTime,java.time.ZonedDateTime) */
    public static  double diffBusinessYears( java.time.ZonedDateTime start, java.time.ZonedDateTime end ) {return Calendars.calendar().diffBusinessYears( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#diffNonBusinessDuration(java.time.Instant,java.time.Instant) */
    public static  java.time.Duration diffNonBusinessDuration( java.time.Instant start, java.time.Instant end ) {return Calendars.calendar().diffNonBusinessDuration( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#diffNonBusinessDuration(java.time.ZonedDateTime,java.time.ZonedDateTime) */
    public static  java.time.Duration diffNonBusinessDuration( java.time.ZonedDateTime start, java.time.ZonedDateTime end ) {return Calendars.calendar().diffNonBusinessDuration( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#diffNonBusinessNanos(java.time.Instant,java.time.Instant) */
    public static  long diffNonBusinessNanos( java.time.Instant start, java.time.Instant end ) {return Calendars.calendar().diffNonBusinessNanos( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#diffNonBusinessNanos(java.time.ZonedDateTime,java.time.ZonedDateTime) */
    public static  long diffNonBusinessNanos( java.time.ZonedDateTime start, java.time.ZonedDateTime end ) {return Calendars.calendar().diffNonBusinessNanos( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#fractionBusinessDayComplete() */
    public static  double fractionBusinessDayComplete( ) {return Calendars.calendar().fractionBusinessDayComplete( );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#fractionBusinessDayComplete(java.time.Instant) */
    public static  double fractionBusinessDayComplete( java.time.Instant time ) {return Calendars.calendar().fractionBusinessDayComplete( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#fractionBusinessDayComplete(java.time.ZonedDateTime) */
    public static  double fractionBusinessDayComplete( java.time.ZonedDateTime time ) {return Calendars.calendar().fractionBusinessDayComplete( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#fractionBusinessDayRemaining() */
    public static  double fractionBusinessDayRemaining( ) {return Calendars.calendar().fractionBusinessDayRemaining( );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#fractionBusinessDayRemaining(java.time.Instant) */
    public static  double fractionBusinessDayRemaining( java.time.Instant time ) {return Calendars.calendar().fractionBusinessDayRemaining( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#fractionBusinessDayRemaining(java.time.ZonedDateTime) */
    public static  double fractionBusinessDayRemaining( java.time.ZonedDateTime time ) {return Calendars.calendar().fractionBusinessDayRemaining( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#fractionStandardBusinessDay() */
    public static  double fractionStandardBusinessDay( ) {return Calendars.calendar().fractionStandardBusinessDay( );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#fractionStandardBusinessDay(java.lang.String) */
    public static  double fractionStandardBusinessDay( java.lang.String date ) {return Calendars.calendar().fractionStandardBusinessDay( date );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#fractionStandardBusinessDay(java.time.Instant) */
    public static  double fractionStandardBusinessDay( java.time.Instant time ) {return Calendars.calendar().fractionStandardBusinessDay( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#fractionStandardBusinessDay(java.time.LocalDate) */
    public static  double fractionStandardBusinessDay( java.time.LocalDate date ) {return Calendars.calendar().fractionStandardBusinessDay( date );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#fractionStandardBusinessDay(java.time.ZonedDateTime) */
    public static  double fractionStandardBusinessDay( java.time.ZonedDateTime time ) {return Calendars.calendar().fractionStandardBusinessDay( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#futureBusinessDate(int) */
    public static  java.time.LocalDate futureBusinessDate( int days ) {return Calendars.calendar().futureBusinessDate( days );}

    /** @see io.deephaven.time.calendar.Calendar#futureDate(int) */
    public static  java.time.LocalDate futureDate( int days ) {return Calendars.calendar().futureDate( days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#futureNonBusinessDate(int) */
    public static  java.time.LocalDate futureNonBusinessDate( int days ) {return Calendars.calendar().futureNonBusinessDate( days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#holidays() */
    public static  java.util.Map<java.time.LocalDate, io.deephaven.time.calendar.CalendarDay<java.time.Instant>> holidays( ) {return Calendars.calendar().holidays( );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isBusinessDay() */
    public static  boolean isBusinessDay( ) {return Calendars.calendar().isBusinessDay( );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isBusinessDay(java.lang.String) */
    public static  boolean isBusinessDay( java.lang.String date ) {return Calendars.calendar().isBusinessDay( date );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isBusinessDay(java.time.DayOfWeek) */
    public static  boolean isBusinessDay( java.time.DayOfWeek day ) {return Calendars.calendar().isBusinessDay( day );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isBusinessDay(java.time.Instant) */
    public static  boolean isBusinessDay( java.time.Instant time ) {return Calendars.calendar().isBusinessDay( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isBusinessDay(java.time.LocalDate) */
    public static  boolean isBusinessDay( java.time.LocalDate date ) {return Calendars.calendar().isBusinessDay( date );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isBusinessDay(java.time.ZonedDateTime) */
    public static  boolean isBusinessDay( java.time.ZonedDateTime time ) {return Calendars.calendar().isBusinessDay( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isBusinessTime() */
    public static  boolean isBusinessTime( ) {return Calendars.calendar().isBusinessTime( );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isBusinessTime(java.time.Instant) */
    public static  boolean isBusinessTime( java.time.Instant time ) {return Calendars.calendar().isBusinessTime( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isBusinessTime(java.time.ZonedDateTime) */
    public static  boolean isBusinessTime( java.time.ZonedDateTime time ) {return Calendars.calendar().isBusinessTime( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfMonth() */
    public static  boolean isLastBusinessDayOfMonth( ) {return Calendars.calendar().isLastBusinessDayOfMonth( );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfMonth(java.lang.String) */
    public static  boolean isLastBusinessDayOfMonth( java.lang.String date ) {return Calendars.calendar().isLastBusinessDayOfMonth( date );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfMonth(java.time.Instant) */
    public static  boolean isLastBusinessDayOfMonth( java.time.Instant time ) {return Calendars.calendar().isLastBusinessDayOfMonth( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfMonth(java.time.ZonedDateTime) */
    public static  boolean isLastBusinessDayOfMonth( java.time.ZonedDateTime time ) {return Calendars.calendar().isLastBusinessDayOfMonth( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfWeek() */
    public static  boolean isLastBusinessDayOfWeek( ) {return Calendars.calendar().isLastBusinessDayOfWeek( );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfWeek(java.lang.String) */
    public static  boolean isLastBusinessDayOfWeek( java.lang.String date ) {return Calendars.calendar().isLastBusinessDayOfWeek( date );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfWeek(java.time.Instant) */
    public static  boolean isLastBusinessDayOfWeek( java.time.Instant time ) {return Calendars.calendar().isLastBusinessDayOfWeek( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfWeek(java.time.LocalDate) */
    public static  boolean isLastBusinessDayOfWeek( java.time.LocalDate date ) {return Calendars.calendar().isLastBusinessDayOfWeek( date );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfWeek(java.time.ZonedDateTime) */
    public static  boolean isLastBusinessDayOfWeek( java.time.ZonedDateTime time ) {return Calendars.calendar().isLastBusinessDayOfWeek( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfYear() */
    public static  boolean isLastBusinessDayOfYear( ) {return Calendars.calendar().isLastBusinessDayOfYear( );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfYear(java.time.Instant) */
    public static  boolean isLastBusinessDayOfYear( java.time.Instant time ) {return Calendars.calendar().isLastBusinessDayOfYear( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#isLastBusinessDayOfYear(java.time.ZonedDateTime) */
    public static  boolean isLastBusinessDayOfYear( java.time.ZonedDateTime time ) {return Calendars.calendar().isLastBusinessDayOfYear( time );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#minusBusinessDays(java.lang.String,int) */
    public static  java.lang.String minusBusinessDays( java.lang.String date, int days ) {return Calendars.calendar().minusBusinessDays( date, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#minusBusinessDays(java.time.Instant,int) */
    public static  java.time.Instant minusBusinessDays( java.time.Instant time, int days ) {return Calendars.calendar().minusBusinessDays( time, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#minusBusinessDays(java.time.LocalDate,int) */
    public static  java.time.LocalDate minusBusinessDays( java.time.LocalDate date, int days ) {return Calendars.calendar().minusBusinessDays( date, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#minusBusinessDays(java.time.ZonedDateTime,int) */
    public static  java.time.ZonedDateTime minusBusinessDays( java.time.ZonedDateTime time, int days ) {return Calendars.calendar().minusBusinessDays( time, days );}

    /** @see io.deephaven.time.calendar.Calendar#minusDays(java.lang.String,int) */
    public static  java.lang.String minusDays( java.lang.String date, int days ) {return Calendars.calendar().minusDays( date, days );}

    /** @see io.deephaven.time.calendar.Calendar#minusDays(java.time.Instant,int) */
    public static  java.time.Instant minusDays( java.time.Instant time, int days ) {return Calendars.calendar().minusDays( time, days );}

    /** @see io.deephaven.time.calendar.Calendar#minusDays(java.time.LocalDate,int) */
    public static  java.time.LocalDate minusDays( java.time.LocalDate date, int days ) {return Calendars.calendar().minusDays( date, days );}

    /** @see io.deephaven.time.calendar.Calendar#minusDays(java.time.ZonedDateTime,int) */
    public static  java.time.ZonedDateTime minusDays( java.time.ZonedDateTime time, int days ) {return Calendars.calendar().minusDays( time, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#minusNonBusinessDays(java.lang.String,int) */
    public static  java.lang.String minusNonBusinessDays( java.lang.String date, int days ) {return Calendars.calendar().minusNonBusinessDays( date, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#minusNonBusinessDays(java.time.Instant,int) */
    public static  java.time.Instant minusNonBusinessDays( java.time.Instant time, int days ) {return Calendars.calendar().minusNonBusinessDays( time, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#minusNonBusinessDays(java.time.LocalDate,int) */
    public static  java.time.LocalDate minusNonBusinessDays( java.time.LocalDate date, int days ) {return Calendars.calendar().minusNonBusinessDays( date, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#minusNonBusinessDays(java.time.ZonedDateTime,int) */
    public static  java.time.ZonedDateTime minusNonBusinessDays( java.time.ZonedDateTime time, int days ) {return Calendars.calendar().minusNonBusinessDays( time, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#nonBusinessDates(java.lang.String,java.lang.String) */
    public static  java.lang.String[] nonBusinessDates( java.lang.String start, java.lang.String end ) {return Calendars.calendar().nonBusinessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#nonBusinessDates(java.time.Instant,java.time.Instant) */
    public static  java.time.LocalDate[] nonBusinessDates( java.time.Instant start, java.time.Instant end ) {return Calendars.calendar().nonBusinessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#nonBusinessDates(java.time.LocalDate,java.time.LocalDate) */
    public static  java.time.LocalDate[] nonBusinessDates( java.time.LocalDate start, java.time.LocalDate end ) {return Calendars.calendar().nonBusinessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#nonBusinessDates(java.time.ZonedDateTime,java.time.ZonedDateTime) */
    public static  java.time.LocalDate[] nonBusinessDates( java.time.ZonedDateTime start, java.time.ZonedDateTime end ) {return Calendars.calendar().nonBusinessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#nonBusinessDates(java.lang.String,java.lang.String,boolean,boolean) */
    public static  java.lang.String[] nonBusinessDates( java.lang.String start, java.lang.String end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().nonBusinessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#nonBusinessDates(java.time.Instant,java.time.Instant,boolean,boolean) */
    public static  java.time.LocalDate[] nonBusinessDates( java.time.Instant start, java.time.Instant end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().nonBusinessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#nonBusinessDates(java.time.LocalDate,java.time.LocalDate,boolean,boolean) */
    public static  java.time.LocalDate[] nonBusinessDates( java.time.LocalDate start, java.time.LocalDate end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().nonBusinessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#nonBusinessDates(java.time.ZonedDateTime,java.time.ZonedDateTime,boolean,boolean) */
    public static  java.time.LocalDate[] nonBusinessDates( java.time.ZonedDateTime start, java.time.ZonedDateTime end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().nonBusinessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberBusinessDates(java.lang.String,java.lang.String) */
    public static  int numberBusinessDates( java.lang.String start, java.lang.String end ) {return Calendars.calendar().numberBusinessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberBusinessDates(java.time.Instant,java.time.Instant) */
    public static  int numberBusinessDates( java.time.Instant start, java.time.Instant end ) {return Calendars.calendar().numberBusinessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberBusinessDates(java.time.LocalDate,java.time.LocalDate) */
    public static  int numberBusinessDates( java.time.LocalDate start, java.time.LocalDate end ) {return Calendars.calendar().numberBusinessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberBusinessDates(java.time.ZonedDateTime,java.time.ZonedDateTime) */
    public static  int numberBusinessDates( java.time.ZonedDateTime start, java.time.ZonedDateTime end ) {return Calendars.calendar().numberBusinessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberBusinessDates(java.lang.String,java.lang.String,boolean,boolean) */
    public static  int numberBusinessDates( java.lang.String start, java.lang.String end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().numberBusinessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberBusinessDates(java.time.Instant,java.time.Instant,boolean,boolean) */
    public static  int numberBusinessDates( java.time.Instant start, java.time.Instant end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().numberBusinessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberBusinessDates(java.time.LocalDate,java.time.LocalDate,boolean,boolean) */
    public static  int numberBusinessDates( java.time.LocalDate start, java.time.LocalDate end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().numberBusinessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberBusinessDates(java.time.ZonedDateTime,java.time.ZonedDateTime,boolean,boolean) */
    public static  int numberBusinessDates( java.time.ZonedDateTime start, java.time.ZonedDateTime end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().numberBusinessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.Calendar#numberCalendarDates(java.lang.String,java.lang.String) */
    public static  int numberCalendarDates( java.lang.String start, java.lang.String end ) {return Calendars.calendar().numberCalendarDates( start, end );}

    /** @see io.deephaven.time.calendar.Calendar#numberCalendarDates(java.time.Instant,java.time.Instant) */
    public static  int numberCalendarDates( java.time.Instant start, java.time.Instant end ) {return Calendars.calendar().numberCalendarDates( start, end );}

    /** @see io.deephaven.time.calendar.Calendar#numberCalendarDates(java.time.LocalDate,java.time.LocalDate) */
    public static  int numberCalendarDates( java.time.LocalDate start, java.time.LocalDate end ) {return Calendars.calendar().numberCalendarDates( start, end );}

    /** @see io.deephaven.time.calendar.Calendar#numberCalendarDates(java.time.ZonedDateTime,java.time.ZonedDateTime) */
    public static  int numberCalendarDates( java.time.ZonedDateTime start, java.time.ZonedDateTime end ) {return Calendars.calendar().numberCalendarDates( start, end );}

    /** @see io.deephaven.time.calendar.Calendar#numberCalendarDates(java.lang.String,java.lang.String,boolean,boolean) */
    public static  int numberCalendarDates( java.lang.String start, java.lang.String end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().numberCalendarDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.Calendar#numberCalendarDates(java.time.Instant,java.time.Instant,boolean,boolean) */
    public static  int numberCalendarDates( java.time.Instant start, java.time.Instant end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().numberCalendarDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.Calendar#numberCalendarDates(java.time.LocalDate,java.time.LocalDate,boolean,boolean) */
    public static  int numberCalendarDates( java.time.LocalDate start, java.time.LocalDate end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().numberCalendarDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.Calendar#numberCalendarDates(java.time.ZonedDateTime,java.time.ZonedDateTime,boolean,boolean) */
    public static  int numberCalendarDates( java.time.ZonedDateTime start, java.time.ZonedDateTime end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().numberCalendarDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberNonBusinessDates(java.lang.String,java.lang.String) */
    public static  int numberNonBusinessDates( java.lang.String start, java.lang.String end ) {return Calendars.calendar().numberNonBusinessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberNonBusinessDates(java.time.Instant,java.time.Instant) */
    public static  int numberNonBusinessDates( java.time.Instant start, java.time.Instant end ) {return Calendars.calendar().numberNonBusinessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberNonBusinessDates(java.time.LocalDate,java.time.LocalDate) */
    public static  int numberNonBusinessDates( java.time.LocalDate start, java.time.LocalDate end ) {return Calendars.calendar().numberNonBusinessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberNonBusinessDates(java.time.ZonedDateTime,java.time.ZonedDateTime) */
    public static  int numberNonBusinessDates( java.time.ZonedDateTime start, java.time.ZonedDateTime end ) {return Calendars.calendar().numberNonBusinessDates( start, end );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberNonBusinessDates(java.lang.String,java.lang.String,boolean,boolean) */
    public static  int numberNonBusinessDates( java.lang.String start, java.lang.String end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().numberNonBusinessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberNonBusinessDates(java.time.Instant,java.time.Instant,boolean,boolean) */
    public static  int numberNonBusinessDates( java.time.Instant start, java.time.Instant end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().numberNonBusinessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberNonBusinessDates(java.time.LocalDate,java.time.LocalDate,boolean,boolean) */
    public static  int numberNonBusinessDates( java.time.LocalDate start, java.time.LocalDate end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().numberNonBusinessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#numberNonBusinessDates(java.time.ZonedDateTime,java.time.ZonedDateTime,boolean,boolean) */
    public static  int numberNonBusinessDates( java.time.ZonedDateTime start, java.time.ZonedDateTime end, boolean startInclusive, boolean endInclusive ) {return Calendars.calendar().numberNonBusinessDates( start, end, startInclusive, endInclusive );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#pastBusinessDate(int) */
    public static  java.time.LocalDate pastBusinessDate( int days ) {return Calendars.calendar().pastBusinessDate( days );}

    /** @see io.deephaven.time.calendar.Calendar#pastDate(int) */
    public static  java.time.LocalDate pastDate( int days ) {return Calendars.calendar().pastDate( days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#pastNonBusinessDate(int) */
    public static  java.time.LocalDate pastNonBusinessDate( int days ) {return Calendars.calendar().pastNonBusinessDate( days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#plusBusinessDays(java.lang.String,int) */
    public static  java.lang.String plusBusinessDays( java.lang.String date, int days ) {return Calendars.calendar().plusBusinessDays( date, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#plusBusinessDays(java.time.Instant,int) */
    public static  java.time.Instant plusBusinessDays( java.time.Instant time, int days ) {return Calendars.calendar().plusBusinessDays( time, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#plusBusinessDays(java.time.LocalDate,int) */
    public static  java.time.LocalDate plusBusinessDays( java.time.LocalDate date, int days ) {return Calendars.calendar().plusBusinessDays( date, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#plusBusinessDays(java.time.ZonedDateTime,int) */
    public static  java.time.ZonedDateTime plusBusinessDays( java.time.ZonedDateTime time, int days ) {return Calendars.calendar().plusBusinessDays( time, days );}

    /** @see io.deephaven.time.calendar.Calendar#plusDays(java.lang.String,int) */
    public static  java.lang.String plusDays( java.lang.String date, int days ) {return Calendars.calendar().plusDays( date, days );}

    /** @see io.deephaven.time.calendar.Calendar#plusDays(java.time.Instant,int) */
    public static  java.time.Instant plusDays( java.time.Instant time, int days ) {return Calendars.calendar().plusDays( time, days );}

    /** @see io.deephaven.time.calendar.Calendar#plusDays(java.time.LocalDate,int) */
    public static  java.time.LocalDate plusDays( java.time.LocalDate date, int days ) {return Calendars.calendar().plusDays( date, days );}

    /** @see io.deephaven.time.calendar.Calendar#plusDays(java.time.ZonedDateTime,int) */
    public static  java.time.ZonedDateTime plusDays( java.time.ZonedDateTime time, int days ) {return Calendars.calendar().plusDays( time, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#plusNonBusinessDays(java.lang.String,int) */
    public static  java.lang.String plusNonBusinessDays( java.lang.String date, int days ) {return Calendars.calendar().plusNonBusinessDays( date, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#plusNonBusinessDays(java.time.Instant,int) */
    public static  java.time.Instant plusNonBusinessDays( java.time.Instant time, int days ) {return Calendars.calendar().plusNonBusinessDays( time, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#plusNonBusinessDays(java.time.LocalDate,int) */
    public static  java.time.LocalDate plusNonBusinessDays( java.time.LocalDate date, int days ) {return Calendars.calendar().plusNonBusinessDays( date, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#plusNonBusinessDays(java.time.ZonedDateTime,int) */
    public static  java.time.ZonedDateTime plusNonBusinessDays( java.time.ZonedDateTime time, int days ) {return Calendars.calendar().plusNonBusinessDays( time, days );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#standardBusinessDay() */
    public static  io.deephaven.time.calendar.CalendarDay<java.time.LocalTime> standardBusinessDay( ) {return Calendars.calendar().standardBusinessDay( );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#standardBusinessDuration() */
    public static  java.time.Duration standardBusinessDuration( ) {return Calendars.calendar().standardBusinessDuration( );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#standardBusinessNanos() */
    public static  long standardBusinessNanos( ) {return Calendars.calendar().standardBusinessNanos( );}

    /** @see io.deephaven.time.calendar.Calendar#timeZone() */
    public static  java.time.ZoneId calendarTimeZone( ) {return Calendars.calendar().timeZone( );}

    /** @see io.deephaven.time.calendar.BusinessCalendar#weekendDays() */
    public static  java.util.Set<java.time.DayOfWeek> weekendDays( ) {return Calendars.calendar().weekendDays( );}

}


/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util;

import io.deephaven.base.verify.Require;
import io.deephaven.base.verify.RequirementFailure;
import io.deephaven.configuration.PropertyFile;

import java.io.File;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

// --------------------------------------------------------------------
/**
 * Useful methods for working with dates. Not for use in the critical path.
 */
public class DateUtil {

    public static final boolean DAYMASK_STRICT = true;
    public static final boolean DAYMASK_NOT_STRICT = false;

    public static final int DAY_VALID = '1';
    public static final int DAY_INVALID = '0';
    public static final int DAY_OPTIONAL = '2';

    public static final String DAYMASK_NORMAL_BUSINESS_WEEK = "0111110";

    public static final long NANOS_PER_MICRO = 1000;
    public static final long NANOS_PER_MILLI = NANOS_PER_MICRO * 1000;
    public static final long NANOS_PER_SECOND = NANOS_PER_MILLI * 1000;

    public static final long MICROS_PER_MILLI = 1000;
    public static final long MICROS_PER_SECOND = MICROS_PER_MILLI * 1000;

    public static final long MILLIS_PER_SECOND = 1000;
    public static final long MILLIS_PER_MINUTE = MILLIS_PER_SECOND * 60;
    public static final long MILLIS_PER_HOUR = MILLIS_PER_MINUTE * 60;
    public static final long MILLIS_PER_DAY = MILLIS_PER_HOUR * 24;

    public static final int SECONDS_PER_MINUTE = 60;
    public static final int SECONDS_PER_HOUR = SECONDS_PER_MINUTE * 60;
    public static final int SECONDS_PER_DAY = SECONDS_PER_HOUR * 24;

    public static final int DAYS_PER_WEEK = 7;

    public static final long[] THOUSANDS = {1, 1000, 1000000, 1000000000};

    /** Number of days in each month. (Jan==1, Feb is non-leap-year) */
    public static final int[] DAYS_PER_MONTH = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

    /** Three letter abbreviations of month names. (Jan==1, title case) */
    public static final String[] MONTH_ABBREVIATIONS_3T =
        {"Xxx", "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};

    /** Three letter abbreviations of month names. (Jan==1, upper case) */
    public static final String[] MONTH_ABBREVIATIONS_3U =
        {"XXX", "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"};

    /** Three letter abbreviations of month names. (Jan==1, lower case) */
    public static final String[] MONTH_ABBREVIATIONS_3L =
        {"xxx", "jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct", "nov", "dec"};

    // some useful formatting objects
    /** Formats a year in YYYY format. */
    private static final DateFormat ms_dateFormatYear =
        new ThreadSafeDateFormat(new SimpleDateFormat("yyyy"));
    /** Formats a month in MM format. */
    private static final DateFormat ms_dateFormatMonth =
        new ThreadSafeDateFormat(new SimpleDateFormat("MM"));
    /** Formats a day in DD format. */
    private static final DateFormat ms_dateFormatDay =
        new ThreadSafeDateFormat(new SimpleDateFormat("dd"));

    private static final DateFormat ms_dateFormatHour =
        new ThreadSafeDateFormat(new SimpleDateFormat("HH"));
    private static final DateFormat ms_dateFormatMinute =
        new ThreadSafeDateFormat(new SimpleDateFormat("mm"));
    private static final DateFormat ms_dateFormatSecond =
        new ThreadSafeDateFormat(new SimpleDateFormat("ss"));


    /**
     * An easy way to get the OS-specific directory name component separator.
     */
    private static final String DIR_SEP = File.separator;

    /**
     * The "local" time zone. We make it explicit for testing purposes.
     */
    private static TimeZone ms_localTimeZone = TimeZone.getDefault();

    // ----------------------------------------------------------------
    /** Gets the "local" time zone. */
    public static TimeZone getLocalTimeZone() {
        return ms_localTimeZone;
    }

    // ----------------------------------------------------------------
    /** Gets the "local" time zone. */
    public static void setLocalTimeZone(TimeZone localTimeZone) {
        Require.neqNull(localTimeZone, "localTimeZone");
        ms_localTimeZone = localTimeZone;
        ms_dateFormatDay.setTimeZone(localTimeZone);
        ms_dateFormatMonth.setTimeZone(localTimeZone);
        ms_dateFormatYear.setTimeZone(localTimeZone);
    }

    // ----------------------------------------------------------------
    public static boolean isLeapYear(int nYear) {
        return 0 == nYear % 4 && (0 != nYear % 100 || 0 == nYear % 400) && 0 != nYear;
    }

    // ----------------------------------------------------------------
    public static int getDaysInMonth(int nMonth, int nYear) {
        Require.geq(nMonth, "nMonth", 1);
        Require.leq(nMonth, "nMonth", 12);
        return DAYS_PER_MONTH[nMonth] + (2 == nMonth && isLeapYear(nYear) ? 1 : 0);
    }

    // ----------------------------------------------------------------
    /**
     * Converts the given date (local timezone) to a string in MMDD format.
     */
    public static String getDateAsMMDD(Date date) {
        Require.neqNull(date, "date");
        return ms_dateFormatMonth.format(date) + ms_dateFormatDay.format(date);
    }

    // ----------------------------------------------------------------
    /**
     * Converts the given date (local timezone) to a string in MM format.
     */
    public static String getDateAsMM(Date date) {
        Require.neqNull(date, "date");
        return ms_dateFormatMonth.format(date);
    }

    // ----------------------------------------------------------------
    /**
     * Converts the given date (local timezone) to a string in DD format.
     */
    public static String getDateAsDD(Date date) {
        Require.neqNull(date, "date");
        return ms_dateFormatDay.format(date);
    }

    // ----------------------------------------------------------------
    /**
     * Converts the given date (local timezone) to a string in YYYYMM format.
     */
    public static String getDateAsYYYYMM(Date date) {
        Require.neqNull(date, "date");
        return ms_dateFormatYear.format(date) + ms_dateFormatMonth.format(date);
    }

    // ----------------------------------------------------------------
    /**
     * Converts the given date (local timezone) to a string in YYYYMMDD format.
     */
    public static String getDateAsYYYYMMDD(Date date) {
        Require.neqNull(date, "date");
        return ms_dateFormatYear.format(date) + ms_dateFormatMonth.format(date)
            + ms_dateFormatDay.format(date);
    }

    // ----------------------------------------------------------------
    /**
     * Converts the given date (local timezone) to a string in YYYYMMDD format.
     */
    public static String getDateAsYYYYMMDD(long timeInMillis) {
        Date date = new Date(timeInMillis);
        return getDateAsYYYYMMDD(date);
    }

    // ----------------------------------------------------------------
    /**
     * Converts the given date (local timezone) to a string in YYYYMMDDTHH:MM:SS format.
     */
    public static String getDateAsYYYYdMMdDDTHHcMMcSS(Date date) {
        return ms_dateFormatYear.format(date) + "-" + ms_dateFormatMonth.format(date) + "-"
            + ms_dateFormatDay.format(date) + "T" + ms_dateFormatHour.format(date) + ":"
            + ms_dateFormatMinute.format(date) + ":" + ms_dateFormatSecond.format(date);
    }

    // ----------------------------------------------------------------
    /**
     * Converts the given date (local timezone) to a string in YYYYMMDDTHH:MM:SS format.
     */
    public static String getDateAsYYYYdMMdDDTHHcMMcSS(long timeInMillis) {
        Date date = new Date(timeInMillis);
        return getDateAsYYYYdMMdDDTHHcMMcSS(date);
    }


    // ----------------------------------------------------------------
    /**
     * Converts the given date (local timezone) to a string in MMDDYYYY format.
     */
    public static String getDateAsMMDDYYYY(Date date) {
        Require.neqNull(date, "date");
        return ms_dateFormatMonth.format(date) + ms_dateFormatDay.format(date)
            + ms_dateFormatYear.format(date);
    }

    // ----------------------------------------------------------------
    /**
     * Converts the given date (local timezone) to a string in YYYY/YYYYMM/YYYYMMDD format.
     */
    public static String getDateAsPath(Date date) {
        Require.neqNull(date, "date");
        String sYear = ms_dateFormatYear.format(date);
        String sMonth = ms_dateFormatMonth.format(date);
        String sDay = ms_dateFormatDay.format(date);
        return sYear + DIR_SEP + sYear + sMonth + DIR_SEP + sYear + sMonth + sDay;
    }

    // ----------------------------------------------------------------
    /**
     * Converts the given integer in YYYYMMDD format to a string in YYYY/YYYYMM/YYYYMMDD format.
     */
    public static String getYyyymmddIntAsPath(int nDateYyyymmdd) {
        String sYyyymmdd = "00000000" + Integer.toString(nDateYyyymmdd);
        sYyyymmdd = sYyyymmdd.substring(sYyyymmdd.length() - 8);
        String sYear = sYyyymmdd.substring(0, 4);
        String sMonth = sYyyymmdd.substring(4, 6);
        String sDay = sYyyymmdd.substring(6, 8);
        return sYear + DIR_SEP + sYear + sMonth + DIR_SEP + sYear + sMonth + sDay;
    }

    // ----------------------------------------------------------------
    /**
     * Gets the download path, in [DownloadBaseDir]/sDataSubdir/YYYY/YYYYMM/YYYYMMDD format given a
     * date (local timezone).
     */
    public static String getDateDownloadPath(PropertyFile configuration, String sDataSubdir,
        Date date) {
        Require.nonempty(sDataSubdir, "sDataSubdir");
        Require.neqNull(date, "date");
        return configuration.getProperty("DownloadBaseDir") + DIR_SEP + sDataSubdir + DIR_SEP
            + getDateAsPath(date);
    }

    // ----------------------------------------------------------------
    /**
     * Gets the download path, in [DownloadBaseDir]/sDataSubdir/YYYY/YYYYMM/YYYYMMDD format given an
     * integer in YYYYMMDD format.
     */
    public static String getYyyymmddIntDownloadPath(PropertyFile configuration, String sDataSubdir,
        int nDateYyyymmdd) {
        Require.nonempty(sDataSubdir, "sDataSubdir");
        return configuration.getProperty("DownloadBaseDir") + DIR_SEP + sDataSubdir + DIR_SEP
            + getYyyymmddIntAsPath(nDateYyyymmdd);
    }

    // ----------------------------------------------------------------
    /** Gets a date object representing the time 24 hours ago. */
    public static Date getDateYesterday() {
        long timeNow = System.currentTimeMillis();
        long timeYesterday = timeNow - MILLIS_PER_DAY;
        return new Date(timeYesterday);
    }

    // ----------------------------------------------------------------
    /**
     * Gets a date object representing the next day at the same hour (which may not be exactly 24
     * hours in the future).
     */
    public static Date getNextDaySameTime(Date baseline, TimeZone zone) {
        Require.neqNull(baseline, "baseline");
        Require.neqNull(zone, "zone");
        Calendar calendar = Calendar.getInstance(zone);
        calendar.setTime(baseline);
        calendar.add(Calendar.DATE, 1);
        return calendar.getTime();
    }

    // ----------------------------------------------------------------
    /**
     * Subtracts zero or more 24hr periods from the given date until the day of week for the
     * resulting date (local timezone) is a valid day according to the mask. If the strict flag is
     * true, optional days are not considered valid.
     * <P>
     * See {@link #validateDayOfWeekMask}.
     */
    public static Date getMostRecentValidDate(Date date, String sValidDaysMask, boolean bStrict) {
        Require.neqNull(date, "date");
        validateDayOfWeekMask(sValidDaysMask, bStrict);

        Calendar calendar = Calendar.getInstance(ms_localTimeZone);
        while (true) {
            calendar.setTime(date);
            char chDayType =
                sValidDaysMask.charAt(calendar.get(Calendar.DAY_OF_WEEK) - Calendar.SUNDAY);
            if (DAY_VALID == chDayType || (!bStrict && DAY_OPTIONAL == chDayType)) {
                break;
            }
            date = new Date(date.getTime() - MILLIS_PER_DAY);
        }
        return date;
    }

    // ----------------------------------------------------------------
    /**
     * Adds one or more 24hr periods from the given date until the day of week for the resulting
     * date (local timezone) is a valid day according to the mask. If the strict flag is true,
     * optional days are not considered valid.
     * <P>
     * See {@link #validateDayOfWeekMask}.
     */
    public static Date getNextValidDate(Date date, String sValidDaysMask, boolean bStrict) {
        Require.neqNull(date, "date");
        validateDayOfWeekMask(sValidDaysMask, bStrict);

        Calendar calendar = Calendar.getInstance(ms_localTimeZone);
        while (true) {
            date = new Date(date.getTime() + MILLIS_PER_DAY);
            calendar.setTime(date);
            char chDayType =
                sValidDaysMask.charAt(calendar.get(Calendar.DAY_OF_WEEK) - Calendar.SUNDAY);
            if (DAY_VALID == chDayType || (!bStrict && DAY_OPTIONAL == chDayType)) {
                break;
            }
        }
        return date;
    }

    // ----------------------------------------------------------------
    /**
     * Returns the validity flag from the mask for the given date (local timezone).
     * <P>
     * See {@link #validateDayOfWeekMask}.
     */
    public static int getDayValidity(Date date, String sValidDaysMask) {
        Require.neqNull(date, "date");
        validateDayOfWeekMask(sValidDaysMask, DAYMASK_NOT_STRICT);
        return sValidDaysMask.charAt(getDayOfWeek(date));
    }

    // ----------------------------------------------------------------
    /**
     * Throws a requirement exception if the given day of week mask is not valid. There must be at
     * least one valid day in the mask. If the strict flag is set, optional days are not considered
     * valid.
     * <P>
     * See {@link #DAY_VALID}, {@link #DAY_INVALID}, {@link #DAY_OPTIONAL}, {@link #DAYMASK_STRICT},
     * {@link #DAYMASK_NOT_STRICT}
     */
    public static void validateDayOfWeekMask(String sValidDaysMask, boolean bStrict) {
        Require.neqNull(sValidDaysMask, "sValidDaysMask", 1);
        Require.eq(sValidDaysMask.length(), "sValidDaysMask.length()", DAYS_PER_WEEK, 1);
        int nValidDaysFound = 0;
        for (int nIndex = 0; nIndex < DAYS_PER_WEEK; nIndex++) {
            char chDayType = sValidDaysMask.charAt(nIndex);
            Require.requirement(
                DAY_INVALID == chDayType || DAY_VALID == chDayType || DAY_OPTIONAL == chDayType,
                "DAY_INVALID==chDayType || DAY_VALID==chDayType || DAY_OPTIONAL==chDayType", 1);
            if (DAY_VALID == chDayType || (!bStrict && DAY_OPTIONAL == chDayType)) {
                nValidDaysFound++;
            }
        }
        Require.gtZero(nValidDaysFound, "nValidDaysFound", 1);
    }

    // ----------------------------------------------------------------
    /**
     * Gets the day of the week (Su == 0) for the given date (local timezone).
     */
    public static int getDayOfWeek(Date date) {
        Require.neqNull(date, "date");
        Calendar calendar = Calendar.getInstance(ms_localTimeZone);
        calendar.setTime(date);
        return calendar.get(Calendar.DAY_OF_WEEK) - Calendar.SUNDAY;
    }

    // ----------------------------------------------------------------
    /**
     * Gets the current date (local timezone) as an integer, in YYYYMMDD format.
     */
    public static int getDateTodayAsYyyymmddInt() {
        return getDateAsYyyymmddInt(new Date());
    }

    // ----------------------------------------------------------------
    /**
     * Gets the given date (local timezone) as an integer, in YYYYMMDD format.
     */
    public static int getDateAsYyyymmddInt(Date date) {
        Require.neqNull(date, "date");
        Calendar calendar = Calendar.getInstance(ms_localTimeZone);
        calendar.setTime(date);
        return calendar.get(Calendar.YEAR) * 10000 + (calendar.get(Calendar.MONTH) + 1) * 100
            + calendar.get(Calendar.DAY_OF_MONTH);
    }

    // ----------------------------------------------------------------
    /** Converts an integer in YYYYMMDD format into "YYYY-MM-DD". */
    public static String formatYyyymmddIntAsIso(int nDateYyyymmdd) {
        return formatYyyymmddStringAsIso(Integer.toString(nDateYyyymmdd));
    }

    // ----------------------------------------------------------------
    /** Converts an integer in YYYYMMDD format into "MM/DD/YYYY". */
    public static String formatYyyymmddIntAsUs(int nDateYyyymmdd) {
        return formatYyyymmddStringAsUs(Integer.toString(nDateYyyymmdd));
    }

    // ----------------------------------------------------------------
    /** Converts a String in YYYYMMDD format into "YYYY-MM-DD". */
    public static String formatYyyymmddStringAsIso(String sDateYyyymmdd) {
        Require.neqNull(sDateYyyymmdd, "sDateYyyymmdd");
        Require.eq(sDateYyyymmdd.length(), "sDateYyyymmdd.length()", 8);
        return sDateYyyymmdd.substring(0, 4) + "-" + sDateYyyymmdd.substring(4, 6) + "-"
            + sDateYyyymmdd.substring(6, 8);
    }

    // ----------------------------------------------------------------
    /** Converts a String in YYYYMMDD format into "MM/DD/YYYY". */
    public static String formatYyyymmddStringAsUs(String sDateYyyymmdd) {
        Require.neqNull(sDateYyyymmdd, "sDateYyyymmdd");
        Require.eq(sDateYyyymmdd.length(), "sDateYyyymmdd.length()", 8);
        return sDateYyyymmdd.substring(4, 6) + "/" + sDateYyyymmdd.substring(6, 8) + "/"
            + sDateYyyymmdd.substring(0, 4);
    }

    // ----------------------------------------------------------------
    /** Converts a String in (M|MM)/(D|DD)/(YY|YYYY) format into "YYYY-MM-DD". */
    public static String formatMmddyyyyStringAsIso(String sDateMmddyyyy) {
        Require.neqNull(sDateMmddyyyy, "sDateMmddyyyy");
        String[] date = sDateMmddyyyy.split("/");
        String res;

        res = ((date[2].length() == 2) ? "20" + date[2] : date[2]);
        res += "-" + ((date[0].length() == 1) ? "0" + date[0] : date[0]);
        res += "-" + ((date[1].length() == 1) ? "0" + date[1] : date[1]);

        Require.eq(res.length(), "sDateMmddyyyy.length()", 10);
        return res;
    }

    // ----------------------------------------------------------------
    /** Converts a String in (M|MM)/(D|DD)/YYYY format into "YYYY-MM-DD". */
    public static String formatMmddyyyyStringAsIsoAllowNull(String sDateMmddyyyy) {
        if (null == sDateMmddyyyy || sDateMmddyyyy.length() == 0) {
            return "";
        }
        Require.neqNull(sDateMmddyyyy, "sDateMmddyyyy");
        String[] date = sDateMmddyyyy.split("/");
        String res;

        res = date[2];
        res += "-" + ((date[0].length() == 1) ? "0" + date[0] : date[0]);
        res += "-" + ((date[1].length() == 1) ? "0" + date[1] : date[1]);

        Require.eq(res.length(), "sDateMmddyyyy.length()", 10);
        return res;
    }


    // ----------------------------------------------------------------
    /** Converts a String in DDM3UYYYY format into "YYYY-MM-DD". */
    public static String formatddM3UyyyyStringAsIso(String sDateddM3Uyyyy) {
        Require.neqNull(sDateddM3Uyyyy, "sDateddM3Uyyyy");
        String res;

        res = sDateddM3Uyyyy.substring(5);
        int monthValue =
            Arrays.asList(MONTH_ABBREVIATIONS_3U).indexOf(sDateddM3Uyyyy.substring(2, 5));
        res += "-" + ((monthValue < 10) ? "0" + monthValue : monthValue);
        res += "-" + (sDateddM3Uyyyy.substring(0, 2));

        Require.eq(res.length(), "sDateddM3Uyyyy.length()", 10);
        return res;
    }

    // ----------------------------------------------------------------
    /** Converts a String in DD-MMM-YYYY format into "YYYY-MM-DD". */
    public static String formatddMMMyyyyStringAsIso(String sDateddMMMyyyy) {
        Require.neqNull(sDateddMMMyyyy, "sDateddMMMyyyy");
        String[] date = sDateddMMMyyyy.split("-");
        String res;

        res = date[2];
        int monthValue = Arrays.asList(MONTH_ABBREVIATIONS_3U).indexOf(date[1]);
        res += "-" + ((monthValue < 10) ? "0" + monthValue : monthValue);
        res += "-" + ((date[0].length() == 1) ? "0" + date[0] : date[0]);

        Require.eq(res.length(), "sDateddmmmyyyy.length()", 10);
        return res;
    }

    // ----------------------------------------------------------------
    /** Converts a String in DD-MMM-YY format into "YYYY-MM-DD". */
    public static String formatddMMMyyStringAsIso(String sDateddMMMyy) {
        Require.neqNull(sDateddMMMyy, "sDateddMMMyy");
        String[] date = sDateddMMMyy.split("-");
        String res;

        res = date[2];
        int monthValue = Arrays.asList(MONTH_ABBREVIATIONS_3U).indexOf(date[1].toUpperCase());
        res += "-" + ((monthValue < 10) ? "0" + monthValue : monthValue);
        res += "-" + ((date[0].length() == 1) ? "0" + date[0] : date[0]);

        Require.eq(res.length(), "sDateddmmmyyyy.length()", 10);
        return res;
    }


    // ------------------------------------------------------------------
    /** Converts a String in "Mmm dd, YYYY" format int "YYYY-MM-DD". */
    public static String formatMmmddcYYYYStringAsIso(String sDateMmmddcYYYY) {
        Require.neqNull(sDateMmmddcYYYY, "sDateMmmddcYYYY");
        String[] date = sDateMmmddcYYYY.split("[ ,]");
        String res;

        res = date[3];
        int monthValue = Arrays.asList(MONTH_ABBREVIATIONS_3T).indexOf(date[0]);
        res += "-" + ((monthValue < 10) ? "0" + monthValue : monthValue);
        res += "-" + date[1];

        Require.eq(res.length(), "sDateMmmddcYYYY.length()", 10);
        return res;
    }

    // ------------------------------------------------------------------
    /** Converts a String in "YYYY-MM-DD" format into "MM/DD/YYYY" format. */
    public static String formatIsoAsMMsDDsYYYYString(String sDateYYYYdMMdDD) {
        Require.neqNull(sDateYYYYdMMdDD, "sDateYYYYdMMdDD");
        String[] date = sDateYYYYdMMdDD.split("-");
        String res = date[1] + "/" + date[2] + "/" + date[0];
        Require.eq(res.length(), "sDateYYYYdMMdDD.length()", 10);
        return res;
    }

    /**
     * Converts a date string into a date.
     *
     * @param date
     * @param sourceFormat
     * @param resultFormat
     * @return date
     * @throws ParseException
     */
    public static String formatDateFromStringToString(String date, String sourceFormat,
        String resultFormat) {
        final DateFormat sourceDateFormat = new SimpleDateFormat(sourceFormat);
        final DateFormat resultDateFormat = new SimpleDateFormat(resultFormat);
        return formatDateFromFormatToFormat(date, sourceDateFormat, resultDateFormat);
    }

    /**
     * Converts a date string into a date.
     *
     * @param date
     * @param sourceDateFormat
     * @param resultDateFormat
     * @return date
     * @throws ParseException
     */
    public static String formatDateFromFormatToFormat(String date, DateFormat sourceDateFormat,
        DateFormat resultDateFormat) {
        try {
            return resultDateFormat.format(sourceDateFormat.parse(date));
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    // ################################################################

    // ----------------------------------------------------------------
    /**
     * Returns the absolute timestamp of the most recent occurrence (before or exactly on the
     * <code>referenceTimestamp</code>) of a daily event. The time of day is taken from
     * <code><I>sPropertyNameRoot</I>.time</code> in "h:mm a" format. The time zone for calculations
     * is taken from <code><I>sPropertyNameRoot</I>.timeZone</code>.
     */
    public static Date getTimestampOfMostRecentDailyEvent(PropertyFile configuration,
        String sPropertyNameRoot, Date referenceTimestamp) {
        Require.nonempty(sPropertyNameRoot, "sPropertyNameRoot");
        Require.neqNull(referenceTimestamp, "referenceTimestamp");

        // get the time zone of the event from the system properties
        TimeZone timeZone = getTimeZoneOfEvent(configuration, sPropertyNameRoot);

        // get the time of day of the event from the system properties
        Calendar eventTimestampCalendar =
            buildEventTimestampCalendar(timeZone, sPropertyNameRoot, configuration);

        // determine the exact timestamp of when the event happens today
        Calendar referenceTimestampCalendar = Calendar.getInstance(timeZone);
        referenceTimestampCalendar.setTime(referenceTimestamp);
        eventTimestampCalendar.set(
            referenceTimestampCalendar.get(Calendar.YEAR),
            referenceTimestampCalendar.get(Calendar.MONTH),
            referenceTimestampCalendar.get(Calendar.DAY_OF_MONTH));

        // if the event happens in the future, then the most recent occurrence was the one that
        // happened one day ago
        if (eventTimestampCalendar.getTimeInMillis() > referenceTimestampCalendar
            .getTimeInMillis()) {
            eventTimestampCalendar.add(Calendar.DAY_OF_MONTH, -1);
        }

        return eventTimestampCalendar.getTime();
    }

    // ----------------------------------------------------------------
    /**
     * Returns the absolute timestamp of the occurrence of a daily event that happens in the same
     * "day" as right now. The time of day of the event is taken from
     * <code><I>sPropertyNameRoot</I>.time</code> in "h:mm a" format. The time zone for calculations
     * (and for determining the boundaries of "today") is taken from
     * <code><I>sPropertyNameRoot</I>.timeZone</code>.
     */
    public static Date getTimestampOfEventToday(PropertyFile configuration,
        String sPropertyNameRoot) {
        Require.nonempty(sPropertyNameRoot, "sPropertyNameRoot");

        // get the time zone of the event from the system properties
        TimeZone timeZone = getTimeZoneOfEvent(configuration, sPropertyNameRoot);

        // get the time of day of the event from the system properties
        Calendar eventTimestampCalendar =
            buildEventTimestampCalendar(timeZone, sPropertyNameRoot, configuration);

        // determine the exact timestamp of when the event happens today
        Calendar referenceTimestampCalendar = Calendar.getInstance(timeZone);
        eventTimestampCalendar.set(
            referenceTimestampCalendar.get(Calendar.YEAR),
            referenceTimestampCalendar.get(Calendar.MONTH),
            referenceTimestampCalendar.get(Calendar.DAY_OF_MONTH));

        return eventTimestampCalendar.getTime();
    }

    // ----------------------------------------------------------------
    /**
     * Returns the absolute timestamp of the occurrence of a daily event that happens in the same
     * "day" as right now. The time of day of the event is taken from
     * <code><I>sPropertyNameRoot</I>.time</code> in "h:mm a" format. The time zone for calculations
     * (and for determining the boundaries of "today") is taken from
     * <code><I>sPropertyNameRoot</I>.timeZone</code>.
     */
    public static Date getTimestampOfEventToday(PropertyFile configuration,
        String sPropertyNameRoot, long nNowMillis) {
        Require.nonempty(sPropertyNameRoot, "sPropertyNameRoot");

        // get the time zone of the event from the system properties
        TimeZone timeZone = getTimeZoneOfEvent(configuration, sPropertyNameRoot);

        // get the time of day of the event from the system properties
        Calendar eventTimestampCalendar =
            buildEventTimestampCalendar(timeZone, sPropertyNameRoot, configuration);

        // determine the exact timestamp of when the event happens today
        Calendar referenceTimestampCalendar = Calendar.getInstance(timeZone);
        referenceTimestampCalendar.setTimeInMillis(nNowMillis);
        eventTimestampCalendar.set(
            referenceTimestampCalendar.get(Calendar.YEAR),
            referenceTimestampCalendar.get(Calendar.MONTH),
            referenceTimestampCalendar.get(Calendar.DAY_OF_MONTH));

        return eventTimestampCalendar.getTime();
    }

    // ----------------------------------------------------------------
    private static Calendar buildEventTimestampCalendar(TimeZone timeZone, String sPropertyNameRoot,
        PropertyFile configuration) {
        String sTimeProperty = sPropertyNameRoot + ".time";
        String sTime = configuration.getProperty(sTimeProperty);
        Calendar eventTimestampCalendar = Calendar.getInstance(timeZone);
        SimpleDateFormat timeFormat = new SimpleDateFormat("h:mm:ss a");
        timeFormat.setCalendar(eventTimestampCalendar);
        try {
            timeFormat.parse(sTime);
        } catch (ParseException e) {
            timeFormat = new SimpleDateFormat("h:mm a");
            timeFormat.setCalendar(eventTimestampCalendar);
            try {
                timeFormat.parse(sTime);
            } catch (ParseException e2) {
                throw Require.exceptionNeverCaught("Value of property " + sTimeProperty + " (\""
                    + sTime + "\") not in proper format (\"" + timeFormat.toPattern() + "\").", e2);
            }
        }
        return eventTimestampCalendar;
    }

    // ----------------------------------------------------------------
    /**
     * Gets the timestamp of an event based upon a daily event and a date (retrieved from
     * properties)
     */
    public static Date getTimestampOfEvent(PropertyFile configuration, String sEventPropertyRoot,
        String sDateProperty) {
        Require.nonempty(sEventPropertyRoot, "sEventPropertyRoot");
        Require.nonempty(sDateProperty, "sDateProperty");

        // get the time zone of the event from the system properties
        TimeZone timeZone = getTimeZoneOfEvent(configuration, sEventPropertyRoot);

        // get the time of day of the event from the system properties
        Calendar eventTimestampCalendar =
            buildEventTimestampCalendar(timeZone, sEventPropertyRoot, configuration);

        // parse the date string and set the year, month, and day of the timestamp we are building
        // note: time zone is irrelevant for the next step because we just want the numbers - we
        // could use a regexp.
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
        String sDate = configuration.getProperty(sDateProperty);
        try {
            dateFormat.parse(sDate);
        } catch (ParseException e) {
            throw Require.exceptionNeverCaught(sDateProperty + " (\"" + sDate + "\") not in \""
                + dateFormat.toPattern() + "\" format.", e);
        }
        Calendar dateCalendar = dateFormat.getCalendar();

        // set the year, month, and day
        eventTimestampCalendar.set(dateCalendar.get(Calendar.YEAR),
            dateCalendar.get(Calendar.MONTH), dateCalendar.get(Calendar.DAY_OF_MONTH));

        return eventTimestampCalendar.getTime();
    }

    // ----------------------------------------------------------------
    /**
     * Gets the timestamp of an event based upon a daily event and a date specified by year, month
     * (jan=1), day
     */
    public static Date getTimestampOfEvent(PropertyFile configuration, String sEventPropertyRoot,
        int nYear, int nMonth, int nDay) {
        Require.nonempty(sEventPropertyRoot, "sEventPropertyRoot");

        // get the time zone of the event from the system properties
        TimeZone timeZone = getTimeZoneOfEvent(configuration, sEventPropertyRoot);

        // get the time of day of the event from the system properties
        Calendar eventTimestampCalendar =
            buildEventTimestampCalendar(timeZone, sEventPropertyRoot, configuration);

        // set the year, month, and day
        eventTimestampCalendar.set(nYear, nMonth - 1, nDay);

        return eventTimestampCalendar.getTime();
    }

    // ----------------------------------------------------------------
    /**
     * Gets the timestamp of an event based upon a daily event and a date in YYYYMMDD format
     */
    public static Date getTimestampOfEvent(PropertyFile configuration, String sEventPropertyRoot,
        int nYYYYMMDD) {
        Require.nonempty(sEventPropertyRoot, "sEventPropertyRoot");
        return getTimestampOfEvent(configuration, sEventPropertyRoot, nYYYYMMDD / 10000,
            (nYYYYMMDD / 100) % 100, nYYYYMMDD % 100);
    }

    // ----------------------------------------------------------------
    /** Gets the time zone associated with a particular daily event. */
    public static TimeZone getTimeZoneOfEvent(PropertyFile configuration,
        String sPropertyNameRoot) {
        Require.nonempty(sPropertyNameRoot, "sPropertyNameRoot");
        return TimeZone.getTimeZone(configuration.getProperty(sPropertyNameRoot + ".timeZone"));
    }

    // ----------------------------------------------------------------
    /**
     * Returns a date (noon in the local time zone) which is the date of the most recent occurrence
     * (before or exactly on the <code>referenceTimestamp</code>) of the specified event, in the
     * event's timezone.
     */
    public static Date getDateOfMostRecentDailyEvent(PropertyFile configuration,
        String sPropertyNameRoot, Date referenceTimestamp) {
        Require.nonempty(sPropertyNameRoot, "sPropertyNameRoot");
        Require.neqNull(referenceTimestamp, "referenceTimestamp");
        Date eventTimestamp = getTimestampOfMostRecentDailyEvent(configuration, sPropertyNameRoot,
            referenceTimestamp);
        Calendar sourceCalendar =
            Calendar.getInstance(getTimeZoneOfEvent(configuration, sPropertyNameRoot));
        sourceCalendar.setTime(eventTimestamp);
        Calendar targetCalendar = Calendar.getInstance(ms_localTimeZone);
        targetCalendar.clear();
        targetCalendar.set(sourceCalendar.get(Calendar.YEAR), sourceCalendar.get(Calendar.MONTH),
            sourceCalendar.get(Calendar.DAY_OF_MONTH), 12, 0, 0);
        return targetCalendar.getTime();
    }

    // ----------------------------------------------------------------
    /**
     * Returns a date (noon in the local time zone) which is the date of the most recent occurrence
     * (before or exactly on the <code>referenceTimestamp</code>) of the specified event, in the
     * event's timezone. If the (strict) valid days mask indicates that the date is not valid, days
     * will be subtracted until the date is valid.
     * <P>
     * See {@link #validateDayOfWeekMask}.
     */
    public static Date getDateOfMostRecentDailyEvent(PropertyFile configuration,
        String sPropertyNameRoot, Date referenceTimestamp, String sValidDaysMask) {
        Require.nonempty(sPropertyNameRoot, "sPropertyNameRoot");
        Require.neqNull(referenceTimestamp, "referenceTimestamp");
        validateDayOfWeekMask(sValidDaysMask, DAYMASK_STRICT);
        Calendar calendar = Calendar.getInstance(ms_localTimeZone);
        calendar.setTime(
            getDateOfMostRecentDailyEvent(configuration, sPropertyNameRoot, referenceTimestamp));
        while (true) {
            char chDayType =
                sValidDaysMask.charAt(calendar.get(Calendar.DAY_OF_WEEK) - Calendar.SUNDAY);
            if (DAY_VALID == chDayType) {
                break;
            }
            calendar.add(Calendar.DATE, -1);
        }
        return calendar.getTime();
    }

    // ----------------------------------------------------------------
    /**
     * Wraps a "daily event" as an object. The time of day of the event is taken from
     * <code><I>sPropertyNameRoot</I>.time</code> in "h:mm a" format. The time zone for calculations
     * (and for determining the boundaries of "today") is taken from
     * <code><I>sPropertyNameRoot</I>.timeZone</code>.
     */
    public static class DailyEvent {

        private final PropertyFile m_configuration;
        private final String m_sPropertyNameRoot;

        // ------------------------------------------------------------
        public DailyEvent(PropertyFile configuration, String sPropertyNameRoot) {
            Require.neqNull(configuration, "configuration");
            Require.nonempty(sPropertyNameRoot, "sPropertyNameRoot");
            try {
                buildEventTimestampCalendar(getTimeZoneOfEvent(configuration, sPropertyNameRoot),
                    sPropertyNameRoot, configuration);
            } catch (RequirementFailure e) {
                throw e.adjustForDelegatingMethod();
            }
            m_configuration = configuration;
            m_sPropertyNameRoot = sPropertyNameRoot;
        }

        // ------------------------------------------------------------
        public long getTimestampOfEventToday(long nNow) {
            return DateUtil.getTimestampOfEventToday(m_configuration, m_sPropertyNameRoot, nNow)
                .getTime();
        }

        // ------------------------------------------------------------
        @Override
        public String toString() {
            return m_configuration.getProperty(m_sPropertyNameRoot + ".time") + ", "
                + m_configuration.getProperty(m_sPropertyNameRoot + ".timeZone");
        }
    }

    // ################################################################

    // ----------------------------------------------------------------
    /** Parse the given string into a date with the given format. */
    public static long parse(String sTime, String sFormat) throws ParseException {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(sFormat);
        simpleDateFormat.setTimeZone(ms_localTimeZone);
        return simpleDateFormat.parse(sTime).getTime();
    }

    // ----------------------------------------------------------------
    /**
     * Determines if two dates are on the same calendar day.
     * 
     * @param d1 first date.
     * @param d2 second date.
     * @param tz timezone for the calendar.
     * @return true if the dates are on the same calendar day, and false otherwise.
     */
    public static boolean isSameDay(Date d1, Date d2, TimeZone tz) {
        Calendar calendar1 = new GregorianCalendar(tz);
        calendar1.setTime(d1);
        Calendar calendar2 = new GregorianCalendar(tz);
        calendar2.setTime(d2);

        if (calendar1.get(Calendar.YEAR) != calendar2.get(Calendar.YEAR)) {
            return false;
        } else if (calendar1.get(Calendar.DAY_OF_YEAR) != calendar2.get(Calendar.DAY_OF_YEAR)) {
            return false;
        } else {
            return true;
        }
    }

    // ################################################################

    // ----------------------------------------------------------------
    /**
     * Returns a string in "0d 0h 0m 0.000'000'000s" format from a time interval in nanoseconds.
     */
    public static String formatIntervalNanos(long tsInterval) {
        return internalFormatInterval(tsInterval, 3);
    }

    // ----------------------------------------------------------------
    /**
     * Returns a string in "0d 0h 0m 0.000'000s" format from a time interval in microseconds.
     */
    public static String formatIntervalMicros(long tsInterval) {
        return internalFormatInterval(tsInterval, 2);
    }

    // ----------------------------------------------------------------
    /**
     * Returns a string in "0d 0h 0m 0.000s" format from a time interval in milliseconds.
     */
    public static String formatIntervalMillis(long tsInterval) {
        return internalFormatInterval(tsInterval, 1);
    }

    // ----------------------------------------------------------------
    private static String internalFormatInterval(long tsInterval, int nThousands) {

        StringBuilder stringBuilder = new StringBuilder();
        if (tsInterval < 0) {
            stringBuilder.append("-");
            tsInterval = -tsInterval;
        }

        long tsSeconds = tsInterval / THOUSANDS[nThousands];

        boolean bNeedUnit = false;
        if (tsSeconds > SECONDS_PER_DAY) {
            long nDays = tsSeconds / SECONDS_PER_DAY;
            tsSeconds %= SECONDS_PER_DAY;
            stringBuilder.append(nDays).append("d ");
            bNeedUnit = true;
        }
        if (tsSeconds > SECONDS_PER_HOUR || bNeedUnit) {
            long nHours = tsSeconds / SECONDS_PER_HOUR;
            tsSeconds %= SECONDS_PER_HOUR;
            stringBuilder.append(nHours).append("h ");
            bNeedUnit = true;
        }
        if (tsSeconds > SECONDS_PER_MINUTE || bNeedUnit) {
            long nMinutes = tsSeconds / SECONDS_PER_MINUTE;
            tsSeconds %= SECONDS_PER_MINUTE;
            stringBuilder.append(nMinutes).append("m ");
        }
        stringBuilder.append(tsSeconds).append('.');

        long tsFractions = tsInterval % THOUSANDS[nThousands];

        for (int nIndex = nThousands; nIndex > 0; nIndex--) {
            // if (nIndex!=nThousands) { stringBuilder.append('\''); }
            long tsThousand = tsFractions / THOUSANDS[nIndex - 1];
            tsFractions %= THOUSANDS[nIndex - 1];

            String sLeadingZeros;
            if (tsThousand >= 100) {
                sLeadingZeros = "";
            } else if (tsThousand >= 10) {
                sLeadingZeros = "0";
            } else {
                sLeadingZeros = "00";
            }
            stringBuilder.append(sLeadingZeros).append(tsThousand);
        }
        return stringBuilder.append("s").toString();
    }

    // ----------------------------------------------------------------
    /**
     * Formats the given microsecond timestamp with the given date formatter and then appends the
     * last three microsend digits.
     */
    public static String formatWithTrailingMicros(DateFormat dateFormat, long nTimestampMicros) {
        return dateFormat.format(nTimestampMicros / DateUtil.MICROS_PER_MILLI)
            + DateUtil.formatTrailingMicros(nTimestampMicros);
    }

    // ----------------------------------------------------------------
    /**
     * Returns the last three digits of the given microsecond timestamp as a string, suitable for
     * appending to a timestamp formatted to millisecond precision.
     */
    public static String formatTrailingMicros(long nTimestampMicros) {
        nTimestampMicros = nTimestampMicros % 1000;
        String sLeadingZeros;
        if (nTimestampMicros >= 100) {
            sLeadingZeros = "";
        } else if (nTimestampMicros >= 10) {
            sLeadingZeros = "0";
        } else {
            sLeadingZeros = "00";
        }
        return sLeadingZeros + nTimestampMicros;
    }
}

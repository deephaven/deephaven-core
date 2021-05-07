package io.deephaven.web.client.api.csv;

import io.deephaven.web.client.api.i18n.JsDateTimeFormat;
import io.deephaven.web.client.api.i18n.JsTimeZone;
import io.deephaven.web.client.api.subscription.QueryConstants;
import io.deephaven.web.shared.data.ColumnHolder;
import io.deephaven.web.shared.data.LocalTime;
import io.deephaven.web.shared.data.columns.*;
import io.deephaven.web.shared.fu.JsArrays;

/**
 * Helper class for parsing CSV data into Columns of the correct type.
 */
public class CsvTypeParser {
    // CSV Column types
    public static final String INTEGER = "int";
    public static final String LONG = "long";
    public static final String DOUBLE = "double";
    public static final String BOOLEAN = "bool";
    public static final String DATE_TIME = "datetime";
    public static final String LOCAL_TIME = "localtime";

    // DbDateTime and LocalTime are not visible to this code
    private static final String DATE_TIME_TYPE = "io.deephaven.db.tables.utils.DBDateTime";
    private static final String LOCAL_TIME_TYPE = "java.time.LocalTime";

    private static final String DATE_TIME_PATTERN = "yyyy-MM-dd'T'HH:mm:ss";
    private static final int SEPARATOR_INDEX = 10;

    public static ColumnHolder createColumnHolder(String name, String type, String[] data, String userTimeZone) {
        try {
            switch (type) {
                case INTEGER:
                    final int[] ints = new int[data.length];
                    for (int i = 0; i < data.length; i++) {
                        if (data[i] == null || data[i].trim().isEmpty()) {
                            ints[i] = QueryConstants.NULL_INT;
                        } else {
                            ints[i] = Integer.parseInt(data[i].trim().replaceAll(",", ""));
                        }
                    }
                    return new ColumnHolder(name, INTEGER, new IntArrayColumnData(ints), false);
                case LONG:
                    final long[] longs = new long[data.length];
                    for (int i = 0; i < data.length; i++) {
                        if (data[i] == null || data[i].trim().isEmpty()) {
                            longs[i] = QueryConstants.NULL_LONG;
                        } else {
                            longs[i] = Long.parseLong(data[i].trim().replaceAll(",", ""));
                        }
                    }
                    return new ColumnHolder(name, LONG, new LongArrayColumnData(longs), false);
                case DOUBLE:
                    final double[] doubles = new double[data.length];
                    for (int i = 0; i < data.length; i++) {
                        if (data[i] == null || data[i].trim().isEmpty()) {
                            doubles[i] = QueryConstants.NULL_DOUBLE;
                        } else {
                            doubles[i] = Double.parseDouble(data[i].trim().replaceAll(",", ""));
                        }
                    }
                    return new ColumnHolder(name, DOUBLE, new DoubleArrayColumnData(doubles), false);
                case BOOLEAN:
                    final byte[] bytes = new byte[data.length];
                    for (int i = 0; i < data.length; i++) {
                        if (data[i] == null || data[i].trim().isEmpty()) {
                            bytes[i] = QueryConstants.NULL_BOOLEAN_AS_BYTE;
                        } else {
                            bytes[i] = Boolean.parseBoolean(data[i].trim()) ? QueryConstants.TRUE_BOOLEAN_AS_BYTE : QueryConstants.FALSE_BOOLEAN_AS_BYTE;
                        }
                    }
                    return new ColumnHolder(name, Boolean.class.getCanonicalName(), new ByteArrayColumnData(bytes), false);
                case DATE_TIME:
                    final long[] datetimes = new long[data.length];
                    for (int i = 0; i < data.length; i++) {
                        if (data[i] == null || data[i].trim().isEmpty()) {
                            datetimes[i] = QueryConstants.NULL_LONG;
                        } else {
                            datetimes[i] = parseDateTime(data[i], userTimeZone);
                        }
                    }
                    return new ColumnHolder(name, DATE_TIME_TYPE, new LongArrayColumnData(datetimes), false);
                case LOCAL_TIME:
                    final LocalTime[] localtimes = new LocalTime[data.length];
                    for (int i = 0; i < data.length; i++) {
                        if (data[i] == null || data[i].trim().isEmpty()) {
                            localtimes[i] = null;
                        } else {
                            localtimes[i] = parseLocalTime(data[i]);
                        }
                    }
                    return new ColumnHolder(name, LOCAL_TIME_TYPE, new LocalTimeArrayColumnData(localtimes), false);
                default:
                    final StringArrayColumnData columnData = new StringArrayColumnData();
                    JsArrays.setArray(data, columnData::setData);
                    return new ColumnHolder(name, String.class.getCanonicalName(), columnData, false);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("Error parsing data for type " + type + "\n" + e.getMessage());
        }
    }

    private static long parseDateTime(String str, String userTimeZone) {
        final String s = ensureSeparator(str);
        final int spaceIndex = s.indexOf(' ');
        final String dateTimeString;
        final String timeZoneString;
        if (spaceIndex == - 1) {
            // Zulu is an exception to the space rule
            if (s.endsWith("Z")) {
                dateTimeString = s.substring(0, s.length() - 1);
                timeZoneString = "Z";
            } else {
                dateTimeString = s;
                timeZoneString = null;
            }
        } else {
            dateTimeString =  s.substring(0, spaceIndex);
            timeZoneString = s.substring(spaceIndex + 1);
        }
        final String pattern = getSubsecondPattern(dateTimeString);
        final String tzString = timeZoneString == null ? userTimeZone : timeZoneString;
        final com.google.gwt.i18n.client.TimeZone timeZone = JsTimeZone.getTimeZone(tzString).unwrap();
        return JsDateTimeFormat.getFormat(pattern).parseWithTimezoneAsLong(dateTimeString, timeZone, JsTimeZone.needsDstAdjustment(timeZoneString));
    }

    // Updates the pattern for the correct number of subsecond digits 'S'
    private static String getSubsecondPattern(String s) {
        final int decimalIndex = s.indexOf('.');
        if (decimalIndex == -1) {
            // No subsecond digits
            return DATE_TIME_PATTERN;
        }
        final int numDigits = s.length() - decimalIndex - 1;
        final StringBuilder stringBuilder = new StringBuilder(numDigits);
        for (int i = 0; i < numDigits; i++) {
            stringBuilder.append('S');
        }
        return DATE_TIME_PATTERN + "." + stringBuilder.toString();
    }

    // Ensures that the 'T' separator character is in the the date time
    private static String ensureSeparator(String s) {
        if (s.charAt(SEPARATOR_INDEX) == ' ') {
            return s.replaceFirst(" ", "T");
        }
        return s;
    }

    private static LocalTime parseLocalTime(String s) {
        long dayNanos = 0;
        long subsecondNanos = 0;

        final int tIndex = s.indexOf('T');
        if (tIndex != -1) {
            dayNanos = 86400000000000L * Integer.parseInt(s.substring(0, tIndex));
            s = s.substring(tIndex + 1);
        }

        final int decimalIndex = s.indexOf('.');
        if (decimalIndex != -1) {
            subsecondNanos = parseNanos(s.substring(decimalIndex+1));
            s = s.substring(0, decimalIndex);
        }

        final String[] tokens = s.split(":");
        if (tokens.length == 2) {   //hh:mm
            return new LocalTime(Byte.parseByte(tokens[0]), Byte.parseByte(tokens[1]), (byte) 0, (int) (dayNanos + subsecondNanos));
        } else if (tokens.length == 3) {   //hh:mm:ss
            return new LocalTime(Byte.parseByte(tokens[0]), Byte.parseByte(tokens[1]), Byte.parseByte(tokens[2]), (int) (dayNanos + subsecondNanos));
        }

        return null;
    }

    private static long parseNanos(final String input) {
        long result = 0;
        for (int i=0; i<9; i++) {
            result *= 10;
            final int digit;
            if (i >= input.length()) {
                digit = 0;
            } else {
                digit = Character.digit(input.charAt(i), 10);
                if (digit < 0) {
                    throw new NumberFormatException("Invalid character for nanoseconds conversion: " + input.charAt(i));
                }
            }
            result += digit;
        }
        return result;
    }
}

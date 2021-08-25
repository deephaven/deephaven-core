package io.deephaven.web.client.api.i18n;

import com.google.gwt.i18n.shared.DateTimeFormat;
import com.google.gwt.i18n.shared.TimeZone;
import elemental2.core.JsDate;
import io.deephaven.web.client.api.DateWrapper;
import io.deephaven.web.client.api.LongWrapper;
import io.deephaven.web.client.fu.JsLog;
import jsinterop.annotations.JsConstructor;
import jsinterop.annotations.JsIgnore;
import jsinterop.annotations.JsOptional;
import jsinterop.annotations.JsType;

import javax.annotation.Nonnull;
import java.util.*;

/**
 * Largely an exported wrapper for the GWT DateFormat, but also includes support for formatting nanoseconds as an
 * additional 6 decimal places after the rest of the number.
 *
 * Other concerns that this handles includes accepting a js Date and ignoring the lack of nanos, accepting a js Number
 * and assuming it to be a lossy nano value, and parsing into a js Date.
 */
@JsType(namespace = "dh.i18n", name = "DateTimeFormat")
public class JsDateTimeFormat {
    public static final int NANOS_PER_MILLI = 1_000_000;
    private static final int NANOS_PER_SECOND = 1_000_000_000;
    private static final int NUM_MILLISECONDS_IN_MINUTE = 60000;

    private static final Map<String, JsDateTimeFormat> cache = new HashMap<>();

    public static JsDateTimeFormat getFormat(String pattern) {
        return cache.computeIfAbsent(pattern, JsDateTimeFormat::new);
    }

    public static String format(String pattern, Object date, @JsOptional JsTimeZone timeZone) {
        return getFormat(pattern).format(date, timeZone);
    }

    public static JsDate parseAsDate(String pattern, String text) {
        return getFormat(pattern).parseAsDate(text);
    }

    public static DateWrapper parse(String pattern, String text, @JsOptional JsTimeZone tz) {
        return getFormat(pattern).parse(text, tz);
    }

    private final String pattern;
    private final DateTimeFormat wrappedStart;
    private final DateTimeFormat wrappedEnd;

    private final int nanoCount;

    @JsConstructor
    public JsDateTimeFormat(String pattern) {
        // Look for repeated 'S', or fractions of a second. If we see max of 3, then the
        // default impl can handle it, as its just milliseconds (and our nano<->milli math
        // is enough), if it is 4-6 digits, reduce to 3 in the string that we pass to
        // GWT's DateTimeFormat, and track those remaining 'S's. Any 'S's past 6 will be
        // filled with zeros, or error out.
        // Any format which occurs after the 4th-6th requires a second format for formatting,
        // and pre-processing for parsing.

        // TODO assuming/requiring for now that all fractions of a second are adjacent
        int count = 0;
        for (int i = 0; i < pattern.length(); i++) {
            if (pattern.charAt(i) == 'S') {
                count++;
            } else {
                if (count > 0) {
                    if (pattern.indexOf('S', i) != -1) {
                        throw new IllegalArgumentException("'S' tokens in pattern not contiguous");
                    }
                    break;
                }
            }
        }

        this.pattern = pattern;
        if (count <= 3) {
            // use as-is, no need for additional work
            // this puts us in a sort of "lenient mode", where we don't mind extra or missing
            // sub-second digits, as long as at least 1 is present
            this.wrappedStart = DateTimeFormat.getFormat(pattern);
            this.wrappedEnd = null;
            nanoCount = 0;
        } else {
            int subsecondOffset = pattern.indexOf('S');
            this.wrappedStart = DateTimeFormat.getFormat(pattern.substring(0, subsecondOffset));
            if (subsecondOffset + count < pattern.length()) {
                this.wrappedEnd = DateTimeFormat.getFormat(pattern.substring(subsecondOffset + count));
            } else {
                this.wrappedEnd = null;
            }

            if (count <= 9) {
                // split after the first 3
                nanoCount = count;
            } else {
                // throw or assume padded with zeros?
                throw new IllegalStateException("Max of 9 'S' tokens allowed: " + pattern);
            }
        }
    }

    // TODO accept a TimeZone object here, or perhaps just a string
    // -(new Date()).getTimezoneOffset()/60 lets you read your current offset
    // new Intl.DateTimeFormat().resolvedOptions().timeZone lets you read your current tz name
    // It may be possible to compute the offset of a given date/time from DateTimeFormat and
    // synthesize a gwt TimeZone with the correct offset data to get nice output in some tz
    // other than the browser's current or UTC+/-OFFSET
    public String format(Object date, @JsOptional JsTimeZone timeZone) {
        long nanos = longFromDate(date)
                .orElseThrow(() -> new IllegalStateException("Can't format non-number, non-date value " + date));
        return formatAsLongNanos(nanos, timeZone);
    }

    @JsIgnore
    public static OptionalLong longFromDate(Object date) {
        if (date instanceof String) {
            // assume passed in value is a long in nanos
            return OptionalLong.of(Long.parseLong((String) date));
        } else if (date instanceof JsDate) {
            // no nanos here, just format as is
            return OptionalLong.of(NANOS_PER_MILLI * (long) ((JsDate) date).getTime());
        } else if (date instanceof LongWrapper) {
            // note that this reads in a LongWrapper, which includes subclass DateWrapper
            return OptionalLong.of(((LongWrapper) date).getWrapped());
        } else if (date instanceof Number) {
            // we'll assume that you are passing nanos, but if it looks like it might be
            // in millis, we'll log a warning
            double jsNumber = (double) date;

            // 2^43 is September, 2248 in millis, but jan 1, 1970 1:13am in nanos.
            // it casts a wide net of millis to make sure downstream code doesn't
            // accidentally pass in millis, with a fairly low chance of false
            // positives (less than a three hour window).
            if (Math.abs(jsNumber) < Math.pow(2, 43)) {
                JsLog.warn(
                        "Number passed as date looks suspiciously small, as though it might be millis since Jan 1, 1970, but will be interpreted as if it were nanoseconds since that date.");
            }
            return OptionalLong.of((long) jsNumber);
        } else {
            return OptionalLong.empty();
        }
    }

    @Nonnull
    private String formatAsLongNanos(long unwrapped, JsTimeZone timeZone) {
        TimeZone tz = timeZone == null ? null : timeZone.unwrap();
        int nanos = (int) (unwrapped % NANOS_PER_SECOND);
        Date dateWithoutNanos = new Date(unwrapped / NANOS_PER_MILLI);
        StringBuilder sb = new StringBuilder(wrappedStart.format(dateWithoutNanos, tz));
        if (nanoCount > 0) {
            int remainingNanoDigits = nanoCount;

            // append required digits (may truncate the number if requested)
            String nanoString = Integer.toString(nanos);
            assert nanoString.length() <= 9;
            for (int i = nanoString.length(); i < 9; i++) {
                sb.append('0');
                remainingNanoDigits--;
            }
            sb.append(nanoString, 0, remainingNanoDigits);

            if (wrappedEnd != null) {
                sb.append(wrappedEnd.format(dateWithoutNanos, tz));
            }
        }
        return sb.toString();
    }

    public DateWrapper parse(String text, @JsOptional JsTimeZone tz) {
        if (tz != null) {
            return DateWrapper.of(parseWithTimezoneAsLong(text, tz.unwrap(), true));
        }

        // based on DateTimeFormat.parse, start a date to accumulate values in
        Date curDate = new Date();
        @SuppressWarnings("deprecation")
        Date date = new Date(curDate.getYear(), curDate.getMonth(),
                curDate.getDate());

        // pass the date to each formatter and let it parse its own part of the string
        int endOfStart = wrappedStart.parse(text, 0, date);

        if (endOfStart == 0) {
            // failed to parse
            throw new IllegalArgumentException(text);
        }
        if (nanoCount == 0 && endOfStart < text.length()) {
            // not planning on parsing more, but extra content in the input string
            throw new IllegalArgumentException(text);
        }

        int nanosInParsedText = 0;
        if (nanoCount > 0) {
            // read expected nano chars
            int nano;
            for (nano = 0; nano < nanoCount; nano++) {
                char ch = text.charAt(nano + endOfStart);
                if (ch < '0' || ch > '9') {
                    throw new IllegalArgumentException(text);
                }
                nanosInParsedText = nanosInParsedText * 10 + (ch - '0');
            }
            // for absent nano chars, add trailing zeros
            for (; nano < 9; nano++) {
                nanosInParsedText *= 10;
            }

            if (wrappedEnd != null) {
                int endOfEnd = wrappedEnd.parse(text, endOfStart + nanoCount, date);
                if (endOfEnd == 0 || (endOfStart + nanoCount + endOfEnd) < text.length()) {
                    throw new IllegalArgumentException(text);
                }
            }
        }

        return new DateWrapper(date.getTime() * NANOS_PER_MILLI + nanosInParsedText);
    }

    public JsDate parseAsDate(String text) {
        return new JsDate(parse(text, null).getWrapped() / NANOS_PER_MILLI);
    }

    @JsIgnore
    public long parseWithTimezoneAsLong(String dateTimeString, com.google.gwt.i18n.client.TimeZone timeZone,
            boolean needsAdjustment) {
        final long nanos = parse(dateTimeString, null).getWrapped();
        final int remainder = (int) (nanos % JsDateTimeFormat.NANOS_PER_MILLI);
        long millis = nanos / JsDateTimeFormat.NANOS_PER_MILLI;

        final Date date = new Date(millis);
        final int diff = (date.getTimezoneOffset() - timeZone.getStandardOffset()) * NUM_MILLISECONDS_IN_MINUTE;
        // Adjust time for timezone offset difference
        millis -= diff;

        final int[] transitionPoints = getTransitionPoints(timeZone);
        // If there are no transition points, skip the dst adjustment
        if (needsAdjustment && transitionPoints != null) {
            final int timeInHours = (int) (millis / 1000 / 3600);
            int index = Arrays.binarySearch(transitionPoints, timeInHours);
            if (index < 0) {
                index = Math.abs(index) - 1;
            }
            // Change to a zero based index
            index = index - 1;
            final int transitionPoint = (index < 0) ? 0 : transitionPoints[index];

            final int[] adjustments = getAdjustments(timeZone);
            final int adjustment = (index < 0) ? 0 : adjustments[index];
            // Adjust time for DST transition
            millis = millis - (adjustment * NUM_MILLISECONDS_IN_MINUTE);

            // Look for times that occur during the DST transition
            // Adjustment is in minutes, so convert everything to minutes
            final int timeInMinutes = (int) (millis / NUM_MILLISECONDS_IN_MINUTE);
            final int transitionMinutes = transitionPoint * 60;

            // This is the Spring DST transition Check
            if (timeInMinutes > transitionMinutes && timeInMinutes < transitionMinutes + adjustment) {
                // The format call is expensive, so we check the transition plus adjustment first
                final String formatAfterAdjustment =
                        format(LongWrapper.of(millis * NANOS_PER_MILLI), new JsTimeZone(timeZone));
                if (!formatAfterAdjustment.equals(dateTimeString)) {
                    throw new IllegalArgumentException(dateTimeString + " occurs during a DST transition" +
                            " timeInMinutes = " + timeInMinutes +
                            " transitionMinutes = " + transitionMinutes +
                            " adjustment = " + adjustment);
                }
            }
            if (index < transitionPoints.length - 1) {
                final int nextTransitionMinutes = transitionPoints[index + 1] * 60;
                final int nextAdjustment = adjustments[index + 1];
                if (timeInMinutes > nextTransitionMinutes && timeInMinutes < nextTransitionMinutes + nextAdjustment) {
                    // The format call is expensive, so we check the transition plus adjustment first
                    final String formatAfterAdjustment =
                            format(LongWrapper.of(millis * NANOS_PER_MILLI), new JsTimeZone(timeZone));
                    if (!formatAfterAdjustment.equals(dateTimeString)) {
                        throw new IllegalArgumentException(dateTimeString + " occurs during a DST transition" +
                                " timeInMinutes = " + timeInMinutes +
                                " nextTransitionMinutes = " + nextTransitionMinutes +
                                " nextAdjustment = " + nextAdjustment);
                    }
                }
            }

            // This is the Fall DST transition check
            if (adjustment == 0 && index > 0) {
                final int prevAdjustment = adjustments[index - 1];
                if (timeInMinutes > transitionMinutes && timeInMinutes < transitionMinutes + prevAdjustment) {
                    throw new IllegalArgumentException(dateTimeString + " occurs during a DST transition" +
                            " timeInMinutes = " + timeInMinutes +
                            " transitionMinutes = " + transitionMinutes +
                            " prevAdjustment = " + prevAdjustment);
                }
            }
            if (index < transitionPoints.length - 1) {
                final int nextAdjustment = adjustments[index + 1];
                if (nextAdjustment == 0) {
                    final int nextTransitionMinutes = transitionPoints[index + 1] * 60;
                    if (timeInMinutes < nextTransitionMinutes && timeInMinutes > nextTransitionMinutes - adjustment) {
                        throw new IllegalArgumentException(dateTimeString + " occurs during a DST transition" +
                                " timeInMinutes = " + timeInMinutes +
                                " nextTransitionMinutes = " + nextTransitionMinutes +
                                " adjustment = " + adjustment);
                    }
                }
            }
        }

        return millis * JsDateTimeFormat.NANOS_PER_MILLI + remainder;
    }

    private static native int[] getTransitionPoints(TimeZone tz) /*-{
      return tz.@com.google.gwt.i18n.client.TimeZone::transitionPoints;
    }-*/;

    private static native int[] getAdjustments(TimeZone tz) /*-{
      return tz.@com.google.gwt.i18n.client.TimeZone::adjustments;
    }-*/;

    @Override
    public String toString() {
        return "DateTimeFormat { " +
                "pattern='" + pattern + '\'' +
                ", wrappedStart=" + wrappedStart +
                ", wrappedEnd=" + wrappedEnd +
                ", nanoCount=" + nanoCount +
                " }";
    }
}

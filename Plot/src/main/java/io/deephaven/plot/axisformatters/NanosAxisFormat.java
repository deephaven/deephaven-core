//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.plot.axisformatters;

import io.deephaven.time.DateTimeUtils;

import java.io.Serializable;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParsePosition;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/**
 * A formatter for converting nanoseconds into formatted strings.
 * <p>
 * For details on the supported patterns see the javadoc for
 * <a href="https://docs.oracle.com/javase/8/docs/api/java/time/format/DateTimeFormatter.html">DateTimeFormatter</a>
 */
public class NanosAxisFormat implements AxisFormat, Serializable {

    private static final long serialVersionUID = -2096650974534906333L;
    private ZoneId tz;
    private String pattern;
    private NanosFormat instance = null;

    /**
     * Creates a new NanosAxisFormat with the specified timezone.
     *
     * @param tz timezone
     */
    public NanosAxisFormat(ZoneId tz) {
        this.tz = tz;
    }

    /**
     * Creates a new NanosAxisFormat with the default timezone.
     */
    public NanosAxisFormat() {
        this(DateTimeUtils.timeZone());
    }

    @Override
    public void setPattern(String pattern) {
        // check for validity
        if (pattern != null) {
            DateTimeFormatter.ofPattern(pattern);
        }

        this.pattern = pattern;
        if (instance != null) {
            instance.updateFormatter(pattern);
        }
    }

    @Override
    public NumberFormat getNumberFormatter() {
        if (instance == null) {
            instance = new NanosFormat();
        }
        return instance;
    }

    /**
     * Formatter for date time values.
     */
    public class NanosFormat extends NumberFormat {
        private static final long serialVersionUID = 6037426284760469353L;
        private DateTimeFormatter formatter;

        private NanosFormat() {
            updateFormatter(pattern);
        }

        public void updateTimeZone(final ZoneId tz) {
            NanosAxisFormat.this.tz = tz;

            if (formatter != null) {
                formatter = formatter.withZone(tz);
            }
        }

        private void updateFormatter(String format) {
            format = format == null ? "yyyy-MM-dd" : format;
            this.formatter = DateTimeFormatter.ofPattern(format).withZone(tz);
        }

        @Override
        public StringBuffer format(final double number, final StringBuffer toAppendTo, final FieldPosition pos) {
            return format((long) number, toAppendTo, pos);
        }

        @Override
        public StringBuffer format(final long number, final StringBuffer toAppendTo, final FieldPosition pos) {
            // noinspection DataFlowIssue
            return toAppendTo.append(formatter.format(DateTimeUtils.epochNanosToInstant(number)));
        }

        @Override
        public Number parse(String source, ParsePosition parsePosition) {
            throw new UnsupportedOperationException("Not implemented");
        }
    }
}

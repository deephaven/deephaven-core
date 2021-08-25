/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util;

import io.deephaven.base.verify.Require;

import java.text.AttributedCharacterIterator;
import java.text.DateFormat;
import java.text.FieldPosition;
import java.text.NumberFormat;
import java.text.ParseException;
import java.text.ParsePosition;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

// --------------------------------------------------------------------
/**
 * Wraps a {@link DateFormat} to provide a minimal level of thread safety that DateFormat is lacking (namely, preventing
 * simultaneous calls to {@link #format} from separate threads from interfering with each other).
 */
public class ThreadSafeDateFormat extends DateFormat {
    private final DateFormat m_dateFormat;

    public ThreadSafeDateFormat(DateFormat dateFormat) {
        m_dateFormat = dateFormat;
    }

    @Override
    public Date parse(String source, ParsePosition pos) {
        synchronized (m_dateFormat) {
            return m_dateFormat.parse(source, pos);
        }
    }

    @Override
    public StringBuffer format(Date date, StringBuffer toAppendTo, FieldPosition fieldPosition) {
        synchronized (m_dateFormat) {
            return m_dateFormat.format(date, toAppendTo, fieldPosition);
        }
    }

    @Override
    public boolean isLenient() {
        synchronized (m_dateFormat) {
            return m_dateFormat.isLenient();
        }
    }

    @Override
    public void setLenient(boolean lenient) {
        synchronized (m_dateFormat) {
            m_dateFormat.setLenient(lenient);
        }
    }

    @Override
    public NumberFormat getNumberFormat() {
        synchronized (m_dateFormat) {
            return m_dateFormat.getNumberFormat();
        }
    }

    @Override
    public void setNumberFormat(NumberFormat newNumberFormat) {
        synchronized (m_dateFormat) {
            m_dateFormat.setNumberFormat(newNumberFormat);
        }
    }

    @Override
    public Calendar getCalendar() {
        synchronized (m_dateFormat) {
            return m_dateFormat.getCalendar();
        }
    }

    @Override
    public void setCalendar(Calendar newCalendar) {
        synchronized (m_dateFormat) {
            m_dateFormat.setCalendar(newCalendar);
        }
    }

    @Override
    public TimeZone getTimeZone() {
        synchronized (m_dateFormat) {
            return m_dateFormat.getTimeZone();
        }
    }

    @Override
    public void setTimeZone(TimeZone zone) {
        synchronized (m_dateFormat) {
            m_dateFormat.setTimeZone(zone);
        }
    }

    @Override
    public Date parse(String source) throws ParseException {
        synchronized (m_dateFormat) {
            return m_dateFormat.parse(source);
        }
    }

    @Override
    public Object parseObject(String source, ParsePosition pos) {
        synchronized (m_dateFormat) {
            return m_dateFormat.parseObject(source, pos);
        }
    }

    @Override
    public Object parseObject(String source) throws ParseException {
        synchronized (m_dateFormat) {
            return m_dateFormat.parseObject(source);
        }
    }

    @Override
    public AttributedCharacterIterator formatToCharacterIterator(Object obj) {
        synchronized (m_dateFormat) {
            return m_dateFormat.formatToCharacterIterator(obj);
        }
    }

    @Override
    public String toString() {
        synchronized (m_dateFormat) {
            return m_dateFormat.toString();
        }
    }

    // ################################################################

    @Override
    public boolean equals(Object obj) {
        Require.statementNeverExecuted();
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        Require.statementNeverExecuted();
        return super.hashCode();
    }

    @Override
    public Object clone() {
        Require.statementNeverExecuted();
        return super.clone();
    }

}

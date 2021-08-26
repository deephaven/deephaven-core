/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import io.deephaven.base.verify.Assert;
import org.apache.log4j.helpers.PatternConverter;
import org.apache.log4j.spi.LoggingEvent;

import java.util.Calendar;

// --------------------------------------------------------------------
/**
 * A customized {@link org.apache.log4j.PatternLayout} that uses a customized
 * {@link org.apache.log4j.helpers.PatternParser} that creates a customized {@link PatternConverter}
 * that is very efficient for dates in our preferred format.
 */
public class PatternLayout extends org.apache.log4j.PatternLayout {

    // ----------------------------------------------------------------
    @Override
    protected PatternParser createPatternParser(String pattern) {
        return new PatternParser(pattern);
    }

    // ----------------------------------------------------------------
    private static class PatternParser extends org.apache.log4j.helpers.PatternParser {

        public PatternParser(String pattern) {
            super(pattern);
        }

        @Override
        protected void finalizeConverter(char c) {
            if ('m' == c) {
                currentLiteral.setLength(0);
                addConverter(new MessagePatternConverter());
                return;
            }
            if ('d' == c) {
                int saveI = i;
                String sOption = extractOption();
                if (null != sOption && DatePatternConverter.FORMAT.equals(sOption)) {
                    currentLiteral.setLength(0);
                    addConverter(new DatePatternConverter());
                    return;
                }
                // defer to the original implementation
                i = saveI;
            }
            // defer to the original implementation
            super.finalizeConverter(c);
        }
    }

    // ----------------------------------------------------------------
    public static class MessagePatternConverter extends PatternConverter {

        // ----------------------------------------------------------------
        @Override
        public void format(StringBuffer stringBuffer, LoggingEvent loggingEvent) {
            Object message = loggingEvent.getMessage();
            if (null == message) {
                // append nothing
            } else if (message instanceof CharSequence) {
                stringBuffer.append((CharSequence) message);
            } else {
                stringBuffer.append(loggingEvent.getRenderedMessage());
            }
        }

        // ------------------------------------------------------------
        @Override
        protected String convert(LoggingEvent event) {
            Assert.statementNeverExecuted();
            return null;
        }
    }

    // ----------------------------------------------------------------
    public static class DatePatternConverter extends PatternConverter {

        // 0123456789012345678
        public static final String FORMAT = "MMM dd HH:mm:ss.SSS";

        private final char[] m_buffer = FORMAT.toCharArray();
        private final Calendar m_calendar = Calendar.getInstance();

        private long m_nLastTimestamp = -1;
        private long m_nLastTimestampNoMillis = -1;

        // ------------------------------------------------------------
        /** Appends a date in the format "MMM dd HH:mm:ss.SSS". */
        @Override
        public void format(StringBuffer stringBuffer, LoggingEvent loggingEvent) {

            if (loggingEvent.timeStamp != m_nLastTimestamp) {
                // the millis changed - update the milli field at least
                m_nLastTimestamp = loggingEvent.timeStamp;
                int nMilli = (int) (loggingEvent.timeStamp % 1000);

                if ((loggingEvent.timeStamp - nMilli) != m_nLastTimestampNoMillis) {
                    // the seconds changed - update all the fields
                    m_nLastTimestampNoMillis = loggingEvent.timeStamp - nMilli;
                    m_calendar.setTimeInMillis(loggingEvent.timeStamp);

                    switch (m_calendar.get(Calendar.MONTH)) {
                        case Calendar.JANUARY:
                            m_buffer[0] = 'J';
                            m_buffer[1] = 'a';
                            m_buffer[2] = 'n';
                            break;
                        case Calendar.FEBRUARY:
                            m_buffer[0] = 'F';
                            m_buffer[1] = 'e';
                            m_buffer[2] = 'b';
                            break;
                        case Calendar.MARCH:
                            m_buffer[0] = 'M';
                            m_buffer[1] = 'a';
                            m_buffer[2] = 'r';
                            break;
                        case Calendar.APRIL:
                            m_buffer[0] = 'A';
                            m_buffer[1] = 'p';
                            m_buffer[2] = 'r';
                            break;
                        case Calendar.MAY:
                            m_buffer[0] = 'M';
                            m_buffer[1] = 'a';
                            m_buffer[2] = 'y';
                            break;
                        case Calendar.JUNE:
                            m_buffer[0] = 'J';
                            m_buffer[1] = 'u';
                            m_buffer[2] = 'n';
                            break;
                        case Calendar.JULY:
                            m_buffer[0] = 'J';
                            m_buffer[1] = 'u';
                            m_buffer[2] = 'l';
                            break;
                        case Calendar.AUGUST:
                            m_buffer[0] = 'A';
                            m_buffer[1] = 'u';
                            m_buffer[2] = 'g';
                            break;
                        case Calendar.SEPTEMBER:
                            m_buffer[0] = 'S';
                            m_buffer[1] = 'e';
                            m_buffer[2] = 'p';
                            break;
                        case Calendar.OCTOBER:
                            m_buffer[0] = 'O';
                            m_buffer[1] = 'c';
                            m_buffer[2] = 't';
                            break;
                        case Calendar.NOVEMBER:
                            m_buffer[0] = 'N';
                            m_buffer[1] = 'o';
                            m_buffer[2] = 'v';
                            break;
                        case Calendar.DECEMBER:
                            m_buffer[0] = 'D';
                            m_buffer[1] = 'e';
                            m_buffer[2] = 'c';
                            break;
                        default:
                            Assert.statementNeverExecuted();
                            break;
                    }

                    int nDay = m_calendar.get(Calendar.DAY_OF_MONTH);
                    m_buffer[4] = (char) ('0' + nDay / 10);
                    m_buffer[5] = (char) ('0' + nDay % 10);

                    int nHour = m_calendar.get(Calendar.HOUR_OF_DAY);
                    m_buffer[7] = (char) ('0' + nHour / 10);
                    m_buffer[8] = (char) ('0' + nHour % 10);

                    int nMinute = m_calendar.get(Calendar.MINUTE);
                    m_buffer[10] = (char) ('0' + nMinute / 10);
                    m_buffer[11] = (char) ('0' + nMinute % 10);

                    int nSecond = m_calendar.get(Calendar.SECOND);
                    m_buffer[13] = (char) ('0' + nSecond / 10);
                    m_buffer[14] = (char) ('0' + nSecond % 10);
                }

                m_buffer[16] = (char) ('0' + nMilli / 100);
                m_buffer[17] = (char) ('0' + nMilli / 10 % 10);
                m_buffer[18] = (char) ('0' + nMilli % 10);
            }

            stringBuffer.append(m_buffer);
        }


        int lastHour = -1;

        // ------------------------------------------------------------
        /** Appends a date in the format "MMM dd HH:mm:ss.SSS". */
        public void format(byte[] bytes, long timestamp) {
            if (timestamp != m_nLastTimestamp) {
                // the millis changed - update the milli field at least
                int nMilli = (int) (timestamp % 1000);

                long noMillis = timestamp % 1000;
                if (noMillis != m_nLastTimestampNoMillis) {
                    // the seconds changed - update all the fields
                    m_calendar.setTimeInMillis(timestamp);

                    int nHour = m_calendar.get(Calendar.HOUR_OF_DAY);
                    if (lastHour != nHour) {
                        switch (m_calendar.get(Calendar.MONTH)) {
                            case Calendar.JANUARY:
                                bytes[0] = 'J';
                                bytes[1] = 'a';
                                bytes[2] = 'n';
                                break;
                            case Calendar.FEBRUARY:
                                bytes[0] = 'F';
                                bytes[1] = 'e';
                                bytes[2] = 'b';
                                break;
                            case Calendar.MARCH:
                                bytes[0] = 'M';
                                bytes[1] = 'a';
                                bytes[2] = 'r';
                                break;
                            case Calendar.APRIL:
                                bytes[0] = 'A';
                                bytes[1] = 'p';
                                bytes[2] = 'r';
                                break;
                            case Calendar.MAY:
                                bytes[0] = 'M';
                                bytes[1] = 'a';
                                bytes[2] = 'y';
                                break;
                            case Calendar.JUNE:
                                bytes[0] = 'J';
                                bytes[1] = 'u';
                                bytes[2] = 'n';
                                break;
                            case Calendar.JULY:
                                bytes[0] = 'J';
                                bytes[1] = 'u';
                                bytes[2] = 'l';
                                break;
                            case Calendar.AUGUST:
                                bytes[0] = 'A';
                                bytes[1] = 'u';
                                bytes[2] = 'g';
                                break;
                            case Calendar.SEPTEMBER:
                                bytes[0] = 'S';
                                bytes[1] = 'e';
                                bytes[2] = 'p';
                                break;
                            case Calendar.OCTOBER:
                                bytes[0] = 'O';
                                bytes[1] = 'c';
                                bytes[2] = 't';
                                break;
                            case Calendar.NOVEMBER:
                                bytes[0] = 'N';
                                bytes[1] = 'o';
                                bytes[2] = 'v';
                                break;
                            case Calendar.DECEMBER:
                                bytes[0] = 'D';
                                bytes[1] = 'e';
                                bytes[2] = 'c';
                                break;
                            default:
                                Assert.statementNeverExecuted();
                                break;
                        }
                        bytes[3] = ' ';

                        int nDay = m_calendar.get(Calendar.DAY_OF_MONTH);
                        bytes[4] = (byte) ('0' + nDay / 10);
                        bytes[5] = (byte) ('0' + nDay % 10);
                        bytes[6] = (byte) ' ';
                        bytes[7] = (byte) ('0' + nHour / 10);
                        bytes[8] = (byte) ('0' + nHour % 10);
                        bytes[9] = ':';
                        bytes[12] = ':';
                        bytes[15] = '.';
                        lastHour = nHour;
                    }

                    int nMinute = m_calendar.get(Calendar.MINUTE);
                    bytes[10] = (byte) ('0' + nMinute / 10);
                    bytes[11] = (byte) ('0' + nMinute % 10);

                    int nSecond = m_calendar.get(Calendar.SECOND);
                    bytes[13] = (byte) ('0' + nSecond / 10);
                    bytes[14] = (byte) ('0' + nSecond % 10);
                }

                bytes[16] = (byte) ('0' + nMilli / 100);
                bytes[17] = (byte) ('0' + nMilli / 10 % 10);
                bytes[18] = (byte) ('0' + nMilli % 10);
                bytes[19] = 0;
            }
        }

        // ------------------------------------------------------------
        @Override
        protected String convert(LoggingEvent event) {
            Assert.statementNeverExecuted();
            return null;
        }
    }
}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.util.loggers;

import io.deephaven.base.verify.Require;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.DateUtil;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;

import java.util.Arrays;

public final class LoggerUtil {

    public static boolean isNull(byte b) {
        return b == Byte.MIN_VALUE;
    }

    public static boolean isNull(char c) {
        return c == Character.MIN_VALUE;
    }

    public static boolean isNull(short s) {
        return s == Short.MIN_VALUE;
    }

    public static boolean isNull(int i) {
        return i == Integer.MIN_VALUE;
    }

    public static boolean isNull(long l) {
        return l == Long.MIN_VALUE;
    }

    public static boolean isNull(float f) {
        return Float.isNaN(f);
    }

    public static boolean isNull(double d) {
        return Double.isNaN(d);
    }

    public static void append(final LogOutput out, final int v) {
        if (!isNull(v)) {
            out.append(v);
        }
        out.nf();
    }

    public static void append(final LogOutput out, final char v) {
        if (!isNull(v)) {
            out.append(v);
        }
        out.nf();
    }

    public static void append(final LogOutput out, final short v) {
        if (!isNull(v)) {
            out.append(v);
        }
        out.nf();
    }

    public static void append(final LogOutput out, final long v) {
        if (!isNull(v)) {
            out.append(v);
        }
        out.nf();
    }

    /**
     * Appends the character value of the byte ("A", not the integer value ("65").
     */
    public static void append(final LogOutput out, final byte v) {
        if (!isNull(v)) {
            out.append((char) v);
        }
        out.nf();
    }

    public static void append(final LogOutput out, final double v) {
        if (!isNull(v)) {
            out.appendDouble(v);
        }
        out.nf();
    }

    // ################################################################

    /**
     * Formats an arbitrary object, using a fast method if we can recognize the type, or toString
     * otherwise. Usually you should make the object LogOutputAppendable or create an ObjFormatter,
     * but if the object is truly of unknown type, this is better than calling toString() directly.
     * Outputs "null" if the given object is null.
     */
    public static final LogOutput.ObjFormatter<Object> OBJECT_FORMATTER =
        new LogOutput.ObjFormatter<Object>() {
            @Override
            public void format(LogOutput logOutput, Object object) {
                Require.neqNull(logOutput, "logOutput");
                if (null == object) {
                    logOutput.append("null");
                } else if (object instanceof Throwable) {
                    logOutput.append((Throwable) object);
                } else if (object instanceof LogOutputAppendable) {
                    ((LogOutputAppendable) object).append(logOutput);
                } else if (object instanceof CharSequence) {
                    logOutput.append((CharSequence) object);
                } else if (object instanceof byte[]) {
                    byte[] array = (byte[]) object;
                    logOutput.append('[');
                    for (int nIndex = 0, nLength = array.length; nIndex < nLength; nIndex++) {
                        if (0 != nIndex) {
                            logOutput.append(", ");
                        }
                        logOutput.append(array[nIndex]);
                    }
                    logOutput.append(']');
                } else if (object instanceof short[]) {
                    short[] array = (short[]) object;
                    logOutput.append('[');
                    for (int nIndex = 0, nLength = array.length; nIndex < nLength; nIndex++) {
                        if (0 != nIndex) {
                            logOutput.append(", ");
                        }
                        logOutput.append(array[nIndex]);
                    }
                    logOutput.append(']');
                } else if (object instanceof int[]) {
                    int[] array = (int[]) object;
                    logOutput.append('[');
                    for (int nIndex = 0, nLength = array.length; nIndex < nLength; nIndex++) {
                        if (0 != nIndex) {
                            logOutput.append(", ");
                        }
                        logOutput.append(array[nIndex]);
                    }
                    logOutput.append(']');
                } else if (object instanceof long[]) {
                    long[] array = (long[]) object;
                    logOutput.append('[');
                    for (int nIndex = 0, nLength = array.length; nIndex < nLength; nIndex++) {
                        if (0 != nIndex) {
                            logOutput.append(", ");
                        }
                        logOutput.append(array[nIndex]);
                    }
                    logOutput.append(']');
                } else if (object instanceof char[]) {
                    char[] array = (char[]) object;
                    logOutput.append('[');
                    for (int nIndex = 0, nLength = array.length; nIndex < nLength; nIndex++) {
                        if (0 != nIndex) {
                            logOutput.append(", ");
                        }
                        logOutput.append(array[nIndex]);
                    }
                    logOutput.append(']');
                } else if (object instanceof float[]) {
                    float[] array = (float[]) object;
                    logOutput.append('[');
                    for (int nIndex = 0, nLength = array.length; nIndex < nLength; nIndex++) {
                        if (0 != nIndex) {
                            logOutput.append(", ");
                        }
                        logOutput.appendDouble(array[nIndex]);
                    }
                    logOutput.append(']');
                } else if (object instanceof double[]) {
                    double[] array = (double[]) object;
                    logOutput.append('[');
                    for (int nIndex = 0, nLength = array.length; nIndex < nLength; nIndex++) {
                        if (0 != nIndex) {
                            logOutput.append(", ");
                        }
                        logOutput.appendDouble(array[nIndex]);
                    }
                    logOutput.append(']');
                } else if (object instanceof boolean[]) {
                    boolean[] array = (boolean[]) object;
                    logOutput.append('[');
                    for (int nIndex = 0, nLength = array.length; nIndex < nLength; nIndex++) {
                        if (0 != nIndex) {
                            logOutput.append(", ");
                        }
                        logOutput.append(array[nIndex]);
                    }
                    logOutput.append(']');
                } else if (object.getClass().isArray()) {
                    Object[] array = (Object[]) object;
                    logOutput.append('[');
                    for (int nIndex = 0, nLength = array.length; nIndex < nLength; nIndex++) {
                        if (0 != nIndex) {
                            logOutput.append(", ");
                        }
                        logOutput.append(OBJECT_FORMATTER, array[nIndex]);
                    }
                    logOutput.append(']');
                } else if (object instanceof Byte) {
                    logOutput.append((Byte) object);
                } else if (object instanceof Short) {
                    logOutput.append((Short) object);
                } else if (object instanceof Integer) {
                    logOutput.append((Integer) object);
                } else if (object instanceof Long) {
                    logOutput.append((Long) object);
                } else if (object instanceof Character) {
                    logOutput.append((Character) object);
                } else if (object instanceof Float) {
                    logOutput.appendDouble((Float) object);
                } else if (object instanceof Double) {
                    logOutput.appendDouble((Double) object);
                } else if (object instanceof Boolean) {
                    logOutput.append((Boolean) object);
                } else {
                    // note: we could also handle a ByteBuffer, but I'm not sure that
                    // LogOutput.append(ByteBuffer) is appropriate if you weren't expecting it.
                    logOutput.append(object.toString());
                }
            }
        };

    /**
     * Formats as "" if the object is null, or " (object)" if not null.
     */
    public static final LogOutput.ObjFormatter<Object> OPTIONAL_OBJECT_FORMATTER =
        new LogOutput.ObjFormatter<Object>() {
            @Override
            public void format(LogOutput logOutput, Object object) {
                Require.neqNull(logOutput, "logOutput");
                if (null != object) {
                    logOutput.append(" (").append(OBJECT_FORMATTER, object).append(")");
                }
            }
        };

    /**
     * Formats an exception as "className: message" just like {@link Throwable#toString()}.
     */
    public static final LogOutput.ObjFormatter<Throwable> SIMPLE_EXCEPTION_FORMATTER =
        new LogOutput.ObjFormatter<Throwable>() {
            @Override
            public void format(LogOutput logOutput, Throwable throwable) {
                Require.neqNull(logOutput, "logOutput");
                if (null == throwable) {
                    logOutput.append("null");
                    return;
                }
                logOutput.append(throwable.getClass().getName());
                String sMessage = throwable.getLocalizedMessage();
                if (null != sMessage) {
                    logOutput.append(": ").append(sMessage);
                }
            }
        };

    /** Formats an int[] as "size:[a, b, c, ..]" or "null". */
    public static final LogOutput.ObjFormatter<int[]> SIZE_INT_ARRAY_FORMATTER =
        new LogOutput.ObjFormatter<int[]>() {
            @Override
            public void format(LogOutput logOutput, int[] ints) {
                if (null == ints) {
                    logOutput.append("null");
                } else {
                    logOutput.append(ints.length).append(":[");
                    for (int nIndex = 0, nLength = ints.length; nIndex < nLength; nIndex++) {
                        if (0 != nIndex) {
                            logOutput.append(", ");
                        }
                        logOutput.append(ints[nIndex]);
                    }
                    logOutput.append("]");
                }
            }
        };

    // ################################################################

    // ----------------------------------------------------------------
    /**
     * Appends a string in "0d 0h 0m 0.000'000'000s" format from a time interval in nanoseconds.
     */
    public static final LogOutput.LongFormatter FORMAT_INTERVAL_NANOS =
        new LogOutput.LongFormatter() {
            @Override
            public void format(LogOutput logOutput, long tsInterval) {
                internalFormatInterval(logOutput, tsInterval, 3);
            }
        };

    // ----------------------------------------------------------------
    /**
     * Appends a string in "0d 0h 0m 0.000'000s" format from a time interval in microseconds.
     */
    public static final LogOutput.LongFormatter FORMAT_INTERVAL_MICROS =
        new LogOutput.LongFormatter() {
            @Override
            public void format(LogOutput logOutput, long tsInterval) {
                internalFormatInterval(logOutput, tsInterval, 2);
            }
        };

    // ----------------------------------------------------------------
    /**
     * Appends a string in "0d 0h 0m 0.000s" format from a time interval in milliseconds.
     */
    public static final LogOutput.LongFormatter FORMAT_INTERVAL_MILLIS =
        new LogOutput.LongFormatter() {
            @Override
            public void format(LogOutput logOutput, long tsInterval) {
                internalFormatInterval(logOutput, tsInterval, 1);
            }
        };

    // ----------------------------------------------------------------
    private static void internalFormatInterval(LogOutput logOutput, long tsInterval,
        int nThousands) {

        if (tsInterval < 0) {
            logOutput.append("-");
            tsInterval = -tsInterval;
        }

        long tsSeconds = tsInterval / DateUtil.THOUSANDS[nThousands];

        boolean bNeedUnit = false;
        if (tsSeconds > DateUtil.SECONDS_PER_DAY) {
            long nDays = tsSeconds / DateUtil.SECONDS_PER_DAY;
            tsSeconds %= DateUtil.SECONDS_PER_DAY;
            logOutput.append(nDays).append("d ");
            bNeedUnit = true;
        }
        if (tsSeconds > DateUtil.SECONDS_PER_HOUR || bNeedUnit) {
            long nHours = tsSeconds / DateUtil.SECONDS_PER_HOUR;
            tsSeconds %= DateUtil.SECONDS_PER_HOUR;
            logOutput.append(nHours).append("h ");
            bNeedUnit = true;
        }
        if (tsSeconds > DateUtil.SECONDS_PER_MINUTE || bNeedUnit) {
            long nMinutes = tsSeconds / DateUtil.SECONDS_PER_MINUTE;
            tsSeconds %= DateUtil.SECONDS_PER_MINUTE;
            logOutput.append(nMinutes).append("m ");
        }
        logOutput.append(tsSeconds).append('.');

        long tsFractions = tsInterval % DateUtil.THOUSANDS[nThousands];

        for (int nIndex = nThousands; nIndex > 0; nIndex--) {
            // if (nIndex!=nThousands) { logOutput.append('\''); }
            long tsThousand = tsFractions / DateUtil.THOUSANDS[nIndex - 1];
            tsFractions %= DateUtil.THOUSANDS[nIndex - 1];

            String sLeadingZeros;
            if (tsThousand >= 100) {
                sLeadingZeros = "";
            } else if (tsThousand >= 10) {
                sLeadingZeros = "0";
            } else {
                sLeadingZeros = "00";
            }
            logOutput.append(sLeadingZeros).append(tsThousand);
        }
        logOutput.append("s");
    }

    // ################################################################

    /**
     * Attempt to log a line of items at level to log. Fails silently if any Throwable is thrown,
     * including Throwable's one might ordinarily prefer not to catch (e.g. InterruptedException,
     * subclasses of Error, etc). This is intended for use in processes that are shutting down.
     *
     * @param log
     * @param level
     * @param items
     */
    public static void tryLog(final Logger log, final LogLevel level, final Object... items) {
        try {
            final LogEntry entry = log.getEntry(level);
            Arrays.stream(items).forEach(item -> entry.append(OBJECT_FORMATTER, item));
            entry.endl();
        } catch (Throwable ignored) {
            // NB: It would be better to add support for releasing log entries and invoke that here.
        }
    }
}

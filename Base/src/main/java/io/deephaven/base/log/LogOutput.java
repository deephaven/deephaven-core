/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.base.log;

import io.deephaven.base.text.TimestampBuffer;
import io.deephaven.base.text.TimestampBufferMicros;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.TimeZone;

// --------------------------------------------------------------------
/**
 * Collects output for efficient textual logging. The append methods are intended to behave like StringBuilder to the
 * extent possible, but the fast loggers make no pretense about handling Unicode or producing anything but good old
 * ASCII.
 * <P>
 * Note that although the output will probably be single byte ASCII, we behave like StringBuilder and by the standard
 * overload and promotion rules appending a byte actually appends an integer ("65") not a character ("A").
 */
public interface LogOutput {

    TimestampBuffer millisFormatter = new TimestampBuffer(TimeZone.getDefault());

    LogOutput start();

    LogOutput append(boolean b);

    LogOutput append(char c);

    LogOutput append(short s);

    LogOutput append(int i);

    LogOutput append(long l);

    LogOutput appendDouble(double f);

    LogOutput append(LogOutputAppendable appendable);

    LogOutput append(LongFormatter formatter, long n);

    <T> LogOutput append(ObjFormatter<T> objFormatter, T t);

    <T> LogOutput append(ObjIntIntFormatter<T> objFormatter, T t, int nOffset, int nLength);

    <T, U> LogOutput append(ObjObjFormatter<T, U> objObjFormatter, T t, U u);

    LogOutput append(CharSequence seq);

    LogOutput append(CharSequence seq, int start, int length);

    /**
     * Appends the character equivalent of each byte remaining in the given ByteBuffer ([65 66 67] == "ABC" not
     * "656667"), but does not modify the mark, position, or limit of the ByteBuffer.
     */
    LogOutput append(ByteBuffer bb);

    LogOutput appendTimestamp(long utcMillis, TimestampBuffer tb);

    LogOutput appendTimestampMicros(long utcMicros, TimestampBufferMicros tb);

    LogOutput append(Throwable t);

    LogOutput append(byte[] ba);

    LogOutput append(byte[] ba, int pos, int length);

    LogOutput append(byte[] ba, byte terminator);

    // appenders for boxed types - have to handle nulls
    default LogOutput append(Boolean b) {
        return b == null ? append("null") : append((boolean) b);
    }

    default LogOutput append(Character c) {
        return c == null ? append("null") : append((char) c);
    }

    default LogOutput append(Short s) {
        return s == null ? append("null") : append((short) s);
    }

    default LogOutput append(Integer i) {
        return i == null ? append("null") : append((int) i);
    };

    default LogOutput append(Long l) {
        return l == null ? append("null") : append((long) l);
    };

    default LogOutput appendDouble(Double f) {
        return f == null ? append("null") : appendDouble((double) f);
    }

    // Helpers for loggers that need to know where headers end
    @SuppressWarnings("UnusedReturnValue")
    LogOutput markEndOfHeader();

    int getEndOfHeaderOffset();

    // Looking for append(Object)? Make the object LogOutputAppendable
    // or create an ObjFormatter. If all else fails,
    // use append(LoggerUtil.OBJECT_FORMATTER, object)

    LogOutput nf();

    LogOutput nl();

    LogOutput close();

    // some implementations can't estimate size b/c they are off thread...
    // can use this to tell if size has changed
    int relativeSize();

    int size();

    int getBufferCount();

    ByteBuffer getBuffer(int i);

    LogOutput clear();

    interface LongFormatter {
        void format(LogOutput logOutput, long n);
    }

    interface ObjFormatter<T> {
        void format(LogOutput logOutput, T t);
    }

    interface ObjIntIntFormatter<T> {
        void format(LogOutput logOutput, T t, int nOffset, int nLength);
    }

    interface ObjObjFormatter<T, U> {
        void format(LogOutput logOutput, T t, U u);
    }

    // ---------------------------------------------------------------------------------------------
    // Some handy formatters
    // ---------------------------------------------------------------------------------------------

    /**
     * Formats an arbitrary object similar to Object.toString()
     */
    ObjFormatter<Object> BASIC_FORMATTER = new ObjFormatter<Object>() {
        @Override
        public void format(LogOutput logOutput, Object o) {
            if (o == null) {
                logOutput.append("null");
            } else {
                logOutput.append(o.getClass().getName()).append('@').append(o.hashCode());
            }
        }
    };

    /**
     * Formats an InetSocketAddress
     */
    ObjFormatter<SocketAddress> SOCKADDR_FORMATTER = new LogOutput.ObjFormatter<java.net.SocketAddress>() {
        @Override
        public void format(LogOutput logOutput, SocketAddress sockaddr) {
            if (sockaddr instanceof InetSocketAddress) {
                InetSocketAddress addr = (InetSocketAddress) sockaddr;
                if (addr.getAddress() == null) {
                    logOutput.append("null");
                } else {
                    byte[] b = addr.getAddress().getAddress();
                    logOutput.append((int) b[0] & 0xff);
                    for (int i = 1; i < b.length; ++i) {
                        logOutput.append('.').append((int) b[i] & 0xff);
                    }
                    logOutput.append(':').append(addr.getPort());
                }
            } else {
                BASIC_FORMATTER.format(logOutput, sockaddr);
            }
        }
    };

    /**
     * Formats an int array
     */
    ObjFormatter<int[]> INT_ARRAY_FORMATTER = new LogOutput.ObjFormatter<int[]>() {
        @Override
        public void format(LogOutput logOutput, int[] array) {
            if (array == null) {
                logOutput.append("null");
            } else if (array.length == 0) {
                logOutput.append("{}");
            } else {
                char delim = '{';
                for (int i = 0; i < array.length; ++i) {
                    logOutput.append(delim).append(array[i]);
                    delim = ',';
                }
                logOutput.append('}');
            }
        }
    };

    /**
     * Formats a String array
     */
    ObjFormatter<String[]> STRING_ARRAY_FORMATTER = new LogOutput.ObjFormatter<String[]>() {
        @Override
        public void format(LogOutput logOutput, String[] array) {
            if (array == null) {
                logOutput.append("null");
            } else if (array.length == 0) {
                logOutput.append("{}");
            } else {
                char delim = '{';
                for (int i = 0; i < array.length; ++i) {
                    logOutput.append(delim).append(array[i]);
                    delim = ',';
                }
                logOutput.append('}');
            }
        }
    };

    /**
     * Formats a String Collection
     */
    ObjFormatter<Collection<String>> STRING_COLLECTION_FORMATTER = new LogOutput.ObjFormatter<Collection<String>>() {
        @Override
        public void format(LogOutput logOutput, Collection<String> collection) {
            if (collection == null) {
                logOutput.append("null");
            } else if (collection.isEmpty()) {
                logOutput.append("{}");
            } else {
                char delim = '{';
                for (final String elem : collection) {
                    logOutput.append(delim).append(elem);
                    delim = ',';
                }
                logOutput.append('}');
            }
        }
    };

    /**
     * Formats a String Collection
     */
    ObjFormatter<Collection<LogOutputAppendable>> APPENDABLE_COLLECTION_FORMATTER =
            new LogOutput.ObjFormatter<Collection<LogOutputAppendable>>() {
                @Override
                public void format(LogOutput logOutput, Collection<LogOutputAppendable> collection) {
                    if (collection == null) {
                        logOutput.append("null");
                    } else if (collection.isEmpty()) {
                        logOutput.append("{}");
                    } else {
                        char delim = '{';
                        for (final LogOutputAppendable elem : collection) {
                            logOutput.append(delim).append(elem);
                            delim = ',';
                        }
                        logOutput.append('}');
                    }
                }
            };

    /**
     * Formats a boolean array
     */
    ObjFormatter<boolean[]> BOOLEAN_ARRAY_FORMATTER = new LogOutput.ObjFormatter<boolean[]>() {
        @Override
        public void format(LogOutput logOutput, boolean[] array) {
            if (array == null) {
                logOutput.append("null");
            } else if (array.length == 0) {
                logOutput.append("{}");
            } else {
                char delim = '{';
                for (int i = 0; i < array.length; ++i) {
                    logOutput.append(delim).append(array[i]);
                    delim = ',';
                }
                logOutput.append('}');
            }
        }
    };

    /**
     * Formats byte array as a null-terminated string
     */
    ObjFormatter<byte[]> NULL_TERMINATED_STRING_FORMATTER = new LogOutput.ObjFormatter<byte[]>() {
        @Override
        public void format(LogOutput logOutput, byte[] array) {
            if (array == null) {
                logOutput.append("null");
            } else {
                for (int i = 0; i < array.length && array[i] != 0; ++i) {
                    logOutput.append((char) array[i]);
                }
            }
        }
    };

    /**
     * Formats LocalDateTime based on the default timezone
     */
    ObjFormatter<LocalDateTime> LOCAL_DATE_TIME_FORMATTER = (logOutput, localDateTime) -> {
        if (localDateTime == null) {
            logOutput.append("null");
        } else {
            // This involves some overhead, but a timezone is really useful in logs
            logOutput.append(millisFormatter
                    .getTimestamp(localDateTime.atZone(TimeZone.getDefault().toZoneId()).toInstant().toEpochMilli()));
        }
    };

    /**
     * Formats long millis from epoch based on the default timezone
     */
    LongFormatter MILLIS_FROM_EPOCH_FORMATTER =
            (logOutput, millis) -> logOutput.append(millisFormatter.getTimestamp(millis));

    // ---------------------------------------------------------------------------------------------
    // null implementation
    // ---------------------------------------------------------------------------------------------

    Null NULL = new Null();

    class Null implements LogOutput {
        @Override
        public LogOutput start() {
            return this;
        }

        @Override
        public LogOutput append(boolean b) {
            return this;
        }

        @Override
        public LogOutput append(char c) {
            return this;
        }

        @Override
        public LogOutput append(short s) {
            return this;
        }

        @Override
        public LogOutput append(int i) {
            return this;
        }

        @Override
        public LogOutput append(long l) {
            return this;
        }

        @Override
        public LogOutput appendDouble(double f) {
            return this;
        }

        private LogOutput appendDouble(double f, int digits, boolean forceScientific, boolean trailingZeroes) {
            return this;
        }

        @Override
        public LogOutput append(LogOutputAppendable appendable) {
            return this;
        }

        @Override
        public <T> LogOutput append(ObjFormatter<T> objFormatter, T t) {
            return this;
        }

        @Override
        public <T> LogOutput append(ObjIntIntFormatter<T> objFormatter, T t, int nOffset, int nLength) {
            return this;
        }

        @Override
        public <T, U> LogOutput append(ObjObjFormatter<T, U> objObjFormatter, T t, U u) {
            return this;
        }

        @Override
        public LogOutput append(LongFormatter formatter, long n) {
            return this;
        }

        @Override
        public LogOutput append(CharSequence seq) {
            return this;
        }

        @Override
        public LogOutput append(CharSequence seq, int start, int length) {
            return this;
        }

        @Override
        public LogOutput append(final ByteBuffer bb) {
            return this;
        }

        @Override
        public LogOutput appendTimestamp(long utcMillis, TimestampBuffer tb) {
            return this;
        }

        @Override
        public LogOutput appendTimestampMicros(long utcMicros, TimestampBufferMicros tb) {
            return this;
        }

        @Override
        public LogOutput append(Throwable t) {
            return this;
        }

        @Override
        public LogOutput append(byte[] ba) {
            return this;
        }

        @Override
        public LogOutput append(byte[] ba, int pos, int length) {
            return this;
        }

        @Override
        public LogOutput append(byte[] ba, byte terminator) {
            return this;
        }

        @Override
        public LogOutput markEndOfHeader() {
            return this;
        }

        @Override
        public int getEndOfHeaderOffset() {
            return 0;
        }

        @Override
        public LogOutput nf() {
            return this;
        }

        @Override
        public LogOutput nl() {
            return this;
        }

        @Override
        public LogOutput close() {
            return this;
        }

        @Override
        public int relativeSize() {
            return 0;
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public int getBufferCount() {
            return 0;
        }

        @Override
        public ByteBuffer getBuffer(int i) {
            return null;
        }

        @Override
        public LogOutput clear() {
            return this;
        }
    }
}

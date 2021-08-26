/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import io.deephaven.base.Function;
import io.deephaven.base.pool.Pool;
import io.deephaven.base.pool.ThreadSafeLenientFixedSizePool;
import io.deephaven.io.log.LogBufferPool;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.log.LogSink;
import io.deephaven.io.log.impl.LogEntryImpl;
import org.apache.log4j.Level;

import java.nio.ByteBuffer;

@Deprecated
public class Log4jLoggerImpl implements Logger {

    /**
     * Static buffer pool, shared among all log4j loggers
     */
    private static final Pool<ByteBuffer> buffers = new ThreadSafeLenientFixedSizePool<>(2048,
            new Function.Nullary<ByteBuffer>() {
                @Override
                public ByteBuffer call() {
                    return ByteBuffer.allocate(512);
                }
            },
            null);

    private static final LogBufferPool logBufferPool = new LogBufferPool() {
        @Override
        public ByteBuffer take(int minSize) {
            return buffers.take();
        }

        @Override
        public ByteBuffer take() {
            return buffers.take();
        }

        @Override
        public void give(ByteBuffer item) {
            buffers.give(item);
        }
    };

    /**
     * Specialized entries for log4j loggers
     */
    private static class Entry extends LogEntryImpl {
        private org.apache.log4j.Logger log4jLogger;
        private org.apache.log4j.Level log4jLevel;

        public Entry(LogBufferPool logBufferPool) {
            super(logBufferPool);
        }

        public Entry start(LogSink sink, org.apache.log4j.Logger log4jLogger, LogLevel level, long currentTime) {
            return start(sink, log4jLogger, level, currentTime, null);
        }

        public Entry start(LogSink sink, org.apache.log4j.Logger log4jLogger, LogLevel level, long currentTime,
                Throwable t) {
            super.start(sink, level, currentTime, t);
            this.log4jLogger = log4jLogger;
            this.log4jLevel = getLog4jLevel(level);
            return this;
        }

        /**
         * Override endl() to *not* add a newline - log4j will do that.
         */
        @Override
        public LogEntry endl() {
            super.end();
            return this;
        }
    }

    /**
     * Static pool shared among all loggers
     */
    private static final Pool<Entry> entries = new ThreadSafeLenientFixedSizePool<>(1024,
            new Function.Nullary<Entry>() {
                @Override
                public Entry call() {
                    return new Entry(logBufferPool);
                }
            },
            null);

    /**
     * Specialized sink for log4j loggers
     */
    private static class Sink implements LogSink<Entry> {

        @Override
        public void write(Entry e) {
            if (e.getBufferCount() == 1 && e.getBuffer(0).hasArray()) {
                ByteBuffer b = e.getBuffer(0);
                b.flip();
                byte[] ba = b.array();
                e.log4jLogger.log(e.log4jLevel, new String(ba, 0, b.limit()), e.getThrowable());
            } else {
                StringBuilder sb = new StringBuilder(e.size());
                for (int i = 0; i < e.getBufferCount(); ++i) {
                    ByteBuffer b = e.getBuffer(i);
                    b.flip();
                    while (b.remaining() > 0) {
                        sb.append((char) b.get());
                    }
                }
                e.log4jLogger.log(e.log4jLevel, sb, e.getThrowable());
            }
            e.clear();
            entries.give(e);
        }

        @Override
        public void shutdown() {
            // empty
        }

        @Override
        public void terminate() {
            // empty
        }

        @Override
        public void addInterceptor(Interceptor<Entry> entryInterceptor) {
            // not implemented
        }
    }

    private static final Sink SINK = new Sink();

    // ---------------------------------------------------------------------------------------------

    private final org.apache.log4j.Logger log4jlogger;

    public Log4jLoggerImpl(Class<?> clazz) {
        this(org.apache.log4j.Logger.getLogger(clazz));
    }

    public Log4jLoggerImpl(org.apache.log4j.Logger log4jlogger) {
        this.log4jlogger = log4jlogger;
    }

    private Entry startEntry(LogLevel level, long currentTime) {
        return entries.take().start(SINK, log4jlogger, level, currentTime);
    }

    private Entry startEntry(LogLevel level, long currentTime, Throwable t) {
        return entries.take().start(SINK, log4jlogger, level, currentTime, t);
    }

    @Override
    public LogEntry getEntry(LogLevel level) {
        return isLevelEnabled(level) ? startEntry(level, System.currentTimeMillis() * 1000) : LogEntry.NULL;
    }

    @Override
    public LogEntry getEntry(LogLevel level, Throwable t) {
        return isLevelEnabled(level) ? startEntry(level, System.currentTimeMillis() * 1000, t) : LogEntry.NULL;
    }

    @Override
    public LogEntry getEntry(LogLevel level, long currentTime) {
        return isLevelEnabled(level) ? startEntry(level, currentTime) : LogEntry.NULL;
    }

    @Override
    public LogEntry getEntry(LogLevel level, long currentTime, Throwable t) {
        return isLevelEnabled(level) ? startEntry(level, currentTime, t) : LogEntry.NULL;
    }

    @Override
    public void setLevel(LogLevel level) {
        log4jlogger.setLevel(getLog4jLevel(level));
    }

    @Override
    public LogSink getSink() {
        return SINK;
    }

    public org.apache.log4j.Logger getLog4jLogger() {
        return log4jlogger;
    }

    /** See also {@link Log4jAdapter#getLogLevel}. */
    public static org.apache.log4j.Level getLog4jLevel(LogLevel level) {
        // common case first
        if (level == LogLevel.INFO) {
            return org.apache.log4j.Level.INFO;
        } else if (level == LogLevel.DEBUG) {
            return org.apache.log4j.Level.DEBUG;
        } else if (level == LogLevel.TRACE) {
            return org.apache.log4j.Level.TRACE;
        } else if (level == LogLevel.WARN) {
            return org.apache.log4j.Level.WARN;
        } else if (level == LogLevel.ERROR) {
            return org.apache.log4j.Level.ERROR;
        } else if (level == LogLevel.FATAL) {
            return org.apache.log4j.Level.FATAL;
        } else if (level == LogLevel.EMAIL) {
            return CustomLog4jLevel.EMAIL;
        } else if (level instanceof LogLevel.MailLevel) {
            return new MailLevel(((LogLevel.MailLevel) level).getSubject());
        } else {
            throw new IllegalArgumentException(level.toString());
        }
    }

    @Override
    public boolean isLevelEnabled(LogLevel level) {
        return log4jlogger.isEnabledFor(getLog4jLevel(level));
    }

    @Override
    public void shutdown() {
        // empty
    }

    @Override
    public LogEntry fatal() {
        return getEntry(LogLevel.FATAL);
    }

    @Override
    public LogEntry error() {
        return getEntry(LogLevel.ERROR);
    }

    @Override
    public LogEntry warn() {
        return getEntry(LogLevel.WARN);
    }

    @Override
    public LogEntry info() {
        return getEntry(LogLevel.INFO);
    }

    @Override
    public LogEntry debug() {
        return getEntry(LogLevel.DEBUG);
    }

    @Override
    public LogEntry trace() {
        return getEntry(LogLevel.TRACE);
    }

    @Override
    public LogEntry email() {
        return getEntry(LogLevel.EMAIL);
    }

    @Override
    public LogEntry fatal(Throwable t) {
        return getEntry(LogLevel.FATAL, t);
    }

    @Override
    public LogEntry error(Throwable t) {
        return getEntry(LogLevel.ERROR, t);
    }

    @Override
    public LogEntry warn(Throwable t) {
        return getEntry(LogLevel.WARN, t);
    }

    @Override
    public LogEntry info(Throwable t) {
        return getEntry(LogLevel.INFO, t);
    }

    @Override
    public LogEntry debug(Throwable t) {
        return getEntry(LogLevel.DEBUG, t);
    }

    @Override
    public LogEntry trace(Throwable t) {
        return getEntry(LogLevel.TRACE, t);
    }

    @Override
    public void fatal(Object object) {
        log4jlogger.fatal(object == null ? "" : object.toString());
    }

    @Override
    public void fatal(Object object, Throwable t) {
        log4jlogger.fatal(object, t);
    }

    @Override
    public void error(Object object) {
        log4jlogger.error(object);
    }

    @Override
    public void error(Object object, Throwable t) {
        log4jlogger.error(object, t);
    }

    @Override
    public void warn(Object object) {
        log4jlogger.warn(object);
    }

    @Override
    public void warn(Object object, Throwable t) {
        log4jlogger.warn(object, t);
    }

    @Override
    public void info(Object object) {
        log4jlogger.info(object);
    }

    @Override
    public void info(Object object, Throwable t) {
        log4jlogger.info(object, t);
    }

    @Override
    public void debug(Object object) {
        log4jlogger.debug(object);
    }

    @Override
    public void debug(Object object, Throwable t) {
        log4jlogger.debug(object, t);
    }

    @Override
    public void trace(Object object) {
        log4jlogger.trace(object);
    }

    @Override
    public void trace(Object object, Throwable t) {
        log4jlogger.trace(object, t);
    }

    @Override
    public void email(Object object) {
        log4jlogger.log(CustomLog4jLevel.EMAIL, object);
    }

    @Override
    public void email(Object object, Throwable t) {
        log4jlogger.log(CustomLog4jLevel.EMAIL, object, t);
    }

    @Override
    public boolean isFatalEnabled() {
        return log4jlogger.isEnabledFor(Level.FATAL);
    }

    @Override
    public boolean isErrorEnabled() {
        return log4jlogger.isEnabledFor(Level.ERROR);
    }

    @Override
    public boolean isWarnEnabled() {
        return log4jlogger.isEnabledFor(Level.WARN);
    }

    @Override
    public boolean isInfoEnabled() {
        return log4jlogger.isInfoEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return log4jlogger.isDebugEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return log4jlogger.isTraceEnabled();
    }

    @Override
    public boolean isEmailEnabled() {
        return log4jlogger.isEnabledFor(CustomLog4jLevel.EMAIL);
    }

}

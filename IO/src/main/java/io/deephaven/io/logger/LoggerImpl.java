/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import io.deephaven.base.text.TimestampBufferMicros;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogEntryPool;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.log.LogSink;
import org.jetbrains.annotations.Nullable;

import java.util.TimeZone;

public class LoggerImpl implements Logger {

    protected static final LogEntry EMPTY_LOG_ENTRY = LogEntry.NULL;
    protected final LogEntryPool logEntryPool;
    protected final LogSink logSink;
    protected final String name;
    protected final TimeZone tz;
    protected final LoggerTimeSource timeSource;
    protected TimestampBufferMicros localTimestamp;
    protected LogLevel loggingLevel;
    protected final boolean showLevel;
    protected final boolean showThreadName;

    private static final ThreadLocal<String> THREAD_NAME = new ThreadLocal<String>() {
        private String threadName;

        protected String initialValue() {
            threadName = Thread.currentThread().getName();

            return threadName;
        }
    };

    public LoggerImpl(LogEntryPool logEntryPool, LogSink logSink, String prefix, LogLevel loggingLevel,
            LoggerTimeSource timeSource, TimeZone tz, boolean showLevel, boolean showThreadName) {
        this.logEntryPool = logEntryPool;
        this.logSink = logSink;

        this.name = prefix;
        this.timeSource = timeSource;
        this.tz = tz;
        this.localTimestamp = tz == null ? null : new TimestampBufferMicros(tz);
        this.showLevel = showLevel;
        this.showThreadName = showThreadName;

        this.loggingLevel = loggingLevel;
    }

    @Override
    public void shutdown() {
        logSink.shutdown();
        logEntryPool.shutdown();
    }

    @Override
    public LogEntry getEntry(LogLevel level) {
        return getEntry(level, timeSource.currentTimeMicros(), null);
    }

    @Override
    public LogEntry getEntry(LogLevel level, Throwable t) {
        return getEntry(level, timeSource.currentTimeMicros(), t);
    }

    @Override
    public LogEntry getEntry(LogLevel level, long currentTimeMicros) {
        return getEntry(level, currentTimeMicros, null);
    }

    @Override
    public LogEntry getEntry(LogLevel level, long currentTimeMicros, @Nullable Throwable t) {
        if (!isLevelEnabled(level)) {
            return EMPTY_LOG_ENTRY;
        } else {
            LogEntry entry = logEntryPool.take().start(logSink, level, currentTimeMicros, t);

            if (tz != null) {
                entry.append("[").appendTimestampMicros(entry.getTimestampMicros(), localTimestamp).append("] ");
            }

            if (showLevel) {
                entry.append("- ").append(level.toString()).append(" ");
            }

            if (name != null) {
                entry.append("- ").append(name).append(" ");
            }

            if (showThreadName) {
                entry.append("- ").append(THREAD_NAME.get()).append(" ");
            }

            if (tz != null || showLevel || name != null || showThreadName) {
                entry.append("- ");
            }

            entry.markEndOfHeader();
            return entry;
        }
    }

    @Override
    public void setLevel(LogLevel level) {
        loggingLevel = level;
    }

    @Override
    public LogSink getSink() {
        return logSink;
    }

    @Override
    public boolean isLevelEnabled(LogLevel level) {
        return loggingLevel.getPriority() <= level.getPriority();
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

    private void logObjectWithThrowable(LogLevel level, Object object, Throwable t) {
        LogEntry entry = getEntry(level, timeSource.currentTimeMicros());

        if (object != null) {
            entry.append(object.toString());
        }

        if (t != null) {
            if (object != null) {
                entry.nl();
            }
            entry.append(t);
        }

        entry.endl();
    }

    @Override
    public void fatal(Object object) {
        if (isFatalEnabled()) {
            LogEntry e = fatal();
            e.append(object == null ? "" : object.toString());
            e.endl();
        }
    }

    @Override
    public void fatal(Object object, Throwable t) {
        if (isFatalEnabled()) {
            logObjectWithThrowable(LogLevel.FATAL, object, t);
        }
    }

    @Override
    public void error(Object object) {
        if (isErrorEnabled()) {
            LogEntry e = error();
            e.append(object.toString());
            e.endl();
        }
    }

    @Override
    public void error(Object object, Throwable t) {
        if (isErrorEnabled()) {
            logObjectWithThrowable(LogLevel.ERROR, object, t);
        }
    }

    @Override
    public void warn(Object object) {
        if (isWarnEnabled()) {
            LogEntry e = warn();
            e.append(object.toString());
            e.endl();
        }
    }

    @Override
    public void warn(Object object, Throwable t) {
        if (isErrorEnabled()) {
            logObjectWithThrowable(LogLevel.WARN, object, t);
        }
    }

    @Override
    public void info(Object object) {
        if (isInfoEnabled()) {
            LogEntry e = info();
            e.append(object.toString());
            e.endl();
        }
    }

    @Override
    public void info(Object object, Throwable t) {
        if (isErrorEnabled()) {
            logObjectWithThrowable(LogLevel.INFO, object, t);
        }
    }

    @Override
    public void debug(Object object) {
        if (isDebugEnabled()) {
            LogEntry e = debug();
            e.append(object.toString());
            e.endl();
        }
    }

    @Override
    public void debug(Object object, Throwable t) {
        if (isErrorEnabled()) {
            logObjectWithThrowable(LogLevel.DEBUG, object, t);
        }
    }

    @Override
    public void trace(Object object) {
        if (isTraceEnabled()) {
            LogEntry e = trace();
            e.append(object.toString());
            e.endl();
        }
    }

    @Override
    public void trace(Object object, Throwable t) {
        if (isErrorEnabled()) {
            logObjectWithThrowable(LogLevel.TRACE, object, t);
        }
    }

    @Override
    public void email(Object object) {
        if (isEmailEnabled()) {
            LogEntry e = email();
            e.append(object.toString());
            e.endl();
        }
    }

    @Override
    public void email(Object object, Throwable t) {
        if (isErrorEnabled()) {
            logObjectWithThrowable(LogLevel.EMAIL, object, t);
        }
    }

    @Override
    public boolean isFatalEnabled() {
        return isLevelEnabled(LogLevel.FATAL);
    }

    @Override
    public boolean isErrorEnabled() {
        return isLevelEnabled(LogLevel.ERROR);
    }

    @Override
    public boolean isWarnEnabled() {
        return isLevelEnabled(LogLevel.WARN);
    }

    @Override
    public boolean isInfoEnabled() {
        return isLevelEnabled(LogLevel.INFO);
    }

    @Override
    public boolean isDebugEnabled() {
        return isLevelEnabled(LogLevel.DEBUG);
    }

    @Override
    public boolean isTraceEnabled() {
        return isLevelEnabled(LogLevel.TRACE);
    }

    @Override
    public boolean isEmailEnabled() {
        return isLevelEnabled(LogLevel.EMAIL);
    }
}

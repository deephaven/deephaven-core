/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.log.LogSink;

/**
 * Allocationless logger interface For testing help, see ConsolidatingLogEntry.
 */
public interface Logger {

    /**
     * May return empty LogEntry if that's what the level dictates...
     */
    LogEntry getEntry(LogLevel level);

    LogEntry getEntry(LogLevel level, Throwable t);

    LogEntry getEntry(LogLevel level, long currentTime);

    LogEntry getEntry(LogLevel level, long currentTime, Throwable t);

    void setLevel(LogLevel level);

    LogSink getSink();

    boolean isLevelEnabled(LogLevel level);

    void shutdown();

    // syntactic sugar for getEntry(Level)
    @Deprecated
    LogEntry fatal();

    LogEntry error();

    LogEntry warn();

    LogEntry info();

    LogEntry debug();

    LogEntry trace();

    LogEntry email();

    @Deprecated
    LogEntry fatal(Throwable t);

    LogEntry error(Throwable t);

    LogEntry warn(Throwable t);

    LogEntry info(Throwable t);

    LogEntry debug(Throwable t);

    LogEntry trace(Throwable t);

    @Deprecated
    void fatal(Object object);

    @Deprecated
    void fatal(Object object, Throwable t);

    void error(Object object);

    void error(Object object, Throwable t);

    void warn(Object object);

    void warn(Object object, Throwable t);

    void info(Object object);

    void info(Object object, Throwable t);

    void debug(Object object);

    void debug(Object object, Throwable t);

    void trace(Object object);

    void trace(Object object, Throwable t);

    void email(Object object);

    void email(Object object, Throwable t);

    boolean isFatalEnabled();

    boolean isErrorEnabled();

    boolean isWarnEnabled();

    boolean isInfoEnabled();

    boolean isDebugEnabled();

    boolean isTraceEnabled();

    boolean isEmailEnabled();

    public static final Null NULL = new Null();

    public static class Null implements Logger {
        public LogEntry getEntry(LogLevel level) {
            return LogEntry.NULL;
        }

        public LogEntry getEntry(LogLevel level, Throwable t) {
            return LogEntry.NULL;
        }

        public LogEntry getEntry(LogLevel level, long currentTime) {
            return LogEntry.NULL;
        }

        public LogEntry getEntry(LogLevel level, long currentTime, Throwable t) {
            return LogEntry.NULL;
        }

        public void setLevel(LogLevel level) {}

        public LogSink getSink() {
            return LogSink.NULL;
        }

        public boolean isLevelEnabled(LogLevel level) {
            return false;
        }

        public void shutdown() {}

        public LogEntry fatal() {
            return LogEntry.NULL;
        }

        public LogEntry error() {
            return LogEntry.NULL;
        }

        public LogEntry warn() {
            return LogEntry.NULL;
        }

        public LogEntry info() {
            return LogEntry.NULL;
        }

        public LogEntry debug() {
            return LogEntry.NULL;
        }

        public LogEntry trace() {
            return LogEntry.NULL;
        }

        public LogEntry email() {
            return LogEntry.NULL;
        }

        public LogEntry fatal(Throwable t) {
            return LogEntry.NULL;
        }

        public LogEntry error(Throwable t) {
            return LogEntry.NULL;
        }

        public LogEntry warn(Throwable t) {
            return LogEntry.NULL;
        }

        public LogEntry info(Throwable t) {
            return LogEntry.NULL;
        }

        public LogEntry debug(Throwable t) {
            return LogEntry.NULL;
        }

        public LogEntry trace(Throwable t) {
            return LogEntry.NULL;
        }

        public LogEntry email(Throwable t) {
            return LogEntry.NULL;
        }

        public void fatal(Object object) {}

        public void fatal(Object object, Throwable t) {}

        public void error(Object object) {}

        public void error(Object object, Throwable t) {}

        public void warn(Object object) {}

        public void warn(Object object, Throwable t) {}

        public void info(Object object) {}

        public void info(Object object, Throwable t) {}

        public void debug(Object object) {}

        public void debug(Object object, Throwable t) {}

        public void trace(Object object) {}

        public void trace(Object object, Throwable t) {}

        public void email(Object object) {}

        public void email(Object object, Throwable t) {}

        public boolean isFatalEnabled() {
            return false;
        }

        public boolean isErrorEnabled() {
            return false;
        }

        public boolean isWarnEnabled() {
            return false;
        }

        public boolean isInfoEnabled() {
            return false;
        }

        public boolean isDebugEnabled() {
            return false;
        }

        public boolean isTraceEnabled() {
            return false;
        }

        public boolean isEmailEnabled() {
            return false;
        }
    }
}

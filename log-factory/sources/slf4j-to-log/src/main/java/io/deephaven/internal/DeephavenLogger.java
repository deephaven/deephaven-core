/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.internal;

import io.deephaven.io.log.LogLevel;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.helpers.FormattingTuple;
import org.slf4j.helpers.MarkerIgnoringBase;
import org.slf4j.helpers.MessageFormatter;

public final class DeephavenLogger extends MarkerIgnoringBase implements Logger {

    // private static final long serialVersionUID = 7138766507167124376L;

    private final io.deephaven.io.logger.Logger log; // todo: serializable?

    public DeephavenLogger(io.deephaven.io.logger.Logger log, String name) {
        this.log = Objects.requireNonNull(log);
        this.name = Objects.requireNonNull(name);
    }

    private void log(LogLevel level, String msg) {
        log.getEntry(level).append(msg).endl();
    }

    private void log(LogLevel level, String format, Object arg) {
        if (!log.isLevelEnabled(level)) {
            return;
        }
        // todo: could be more efficient and destructure into append calls if we wanted to parse
        // the string ourselves
        final FormattingTuple tp = MessageFormatter.format(format, arg);
        log.getEntry(level, tp.getThrowable()).append(tp.getMessage()).endl();
    }

    private void log(LogLevel level, String format, Object arg1, Object arg2) {
        if (!log.isLevelEnabled(level)) {
            return;
        }
        // todo: could be more efficient and destructure into append calls if we wanted to parse
        // the string ourselves
        final FormattingTuple tp = MessageFormatter.format(format, arg1, arg2);
        log.getEntry(level, tp.getThrowable()).append(tp.getMessage()).endl();
    }

    private void log(LogLevel level, String format, Object... args) {
        if (!log.isLevelEnabled(level)) {
            return;
        }
        // todo: could be more efficient and destructure into append calls if we wanted to parse
        // the string ourselves
        final FormattingTuple tp = MessageFormatter.arrayFormat(format, args);
        log.getEntry(level, tp.getThrowable()).append(tp.getMessage()).endl();
    }

    @Override
    public boolean isTraceEnabled() {
        return log.isTraceEnabled();
    }

    @Override
    public void trace(String msg) {
        log(LogLevel.TRACE, msg);
    }

    @Override
    public void trace(String format, Object arg) {
        log(LogLevel.TRACE, format, arg);
    }

    @Override
    public void trace(String format, Object arg1, Object arg2) {
        log(LogLevel.TRACE, format, arg1, arg2);
    }

    @Override
    public void trace(String format, Object... arguments) {
        log(LogLevel.TRACE, format, arguments);
    }

    @Override
    public void trace(String msg, Throwable t) {
        log.trace(t).append(msg).endl();
    }

    @Override
    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    @Override
    public void debug(String msg) {
        log(LogLevel.DEBUG, msg);
    }

    @Override
    public void debug(String format, Object arg) {
        log(LogLevel.DEBUG, format, arg);
    }

    @Override
    public void debug(String format, Object arg1, Object arg2) {
        log(LogLevel.DEBUG, format, arg1, arg2);
    }

    @Override
    public void debug(String format, Object... arguments) {
        log(LogLevel.DEBUG, format, arguments);
    }

    @Override
    public void debug(String msg, Throwable t) {
        log.debug(t).append(msg).endl();
    }

    @Override
    public boolean isInfoEnabled() {
        return log.isInfoEnabled();
    }

    @Override
    public void info(String msg) {
        log(LogLevel.INFO, msg);
    }

    @Override
    public void info(String format, Object arg) {
        log(LogLevel.INFO, format, arg);
    }

    @Override
    public void info(String format, Object arg1, Object arg2) {
        log(LogLevel.INFO, format, arg1, arg2);
    }

    @Override
    public void info(String format, Object... arguments) {
        log(LogLevel.INFO, format, arguments);
    }

    @Override
    public void info(String msg, Throwable t) {
        log.info(t).append(msg).endl();
    }

    @Override
    public boolean isWarnEnabled() {
        return log.isWarnEnabled();
    }

    @Override
    public void warn(String msg) {
        log(LogLevel.WARN, msg);
    }

    @Override
    public void warn(String format, Object arg) {
        log(LogLevel.WARN, format, arg);
    }

    @Override
    public void warn(String format, Object... arguments) {
        log(LogLevel.WARN, format, arguments);
    }

    @Override
    public void warn(String format, Object arg1, Object arg2) {
        log(LogLevel.WARN, format, arg1, arg2);
    }

    @Override
    public void warn(String msg, Throwable t) {
        log.warn(t).append(msg).endl();
    }

    @Override
    public boolean isErrorEnabled() {
        return log.isErrorEnabled();
    }

    @Override
    public void error(String msg) {
        log(LogLevel.ERROR, msg);
    }

    @Override
    public void error(String format, Object arg) {
        log(LogLevel.ERROR, format, arg);
    }

    @Override
    public void error(String format, Object arg1, Object arg2) {
        log(LogLevel.ERROR, format, arg1, arg2);
    }

    @Override
    public void error(String format, Object... arguments) {
        log(LogLevel.ERROR, format, arguments);
    }

    @Override
    public void error(String msg, Throwable t) {
        log.error(t).append(msg).endl();
    }
}

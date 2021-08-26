package io.deephaven.internal.log;

import io.deephaven.io.log.LogBufferPool;
import io.deephaven.io.log.LogEntryPool;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.log.LogSink;
import io.deephaven.io.log.impl.DynamicDelayedLogEntryUnsafePoolImpl;
import io.deephaven.io.log.impl.DynamicLogBufferPoolImpl;
import io.deephaven.io.log.impl.LogOutputCsvImpl;
import io.deephaven.io.log.impl.LogSinkImpl;
import io.deephaven.io.logger.Logger;
import io.deephaven.io.logger.LoggerImpl;
import io.deephaven.io.logger.LoggerTimeSource;
import io.deephaven.io.logger.NullLoggerImpl.NullLoggerTimeSource;
import java.util.TimeZone;

public final class LoggerFactoryFile extends LoggerFactorySingleCache {

    private static String getPath() {
        return System.getProperty("io.deephaven.internal.log.LoggerFactoryFile.path", "log.txt");
    }

    private static boolean append() {
        return Boolean.parseBoolean(
                System.getProperty("io.deephaven.internal.log.LoggerFactoryFile.append", "true"));
    }

    private static boolean showLevel() {
        return Boolean.parseBoolean(
                System.getProperty("io.deephaven.internal.log.LoggerFactoryFile.showLevel", "true"));
    }

    private static boolean showThreadName() {
        return Boolean.parseBoolean(System
                .getProperty("io.deephaven.internal.log.LoggerFactoryFile.showThreadName", "true"));
    }

    private static TimeZone timeZone() {
        final String timeZone =
                System.getProperty("io.deephaven.internal.log.LoggerFactoryFile.timeZone");
        return timeZone == null ? TimeZone.getDefault() : TimeZone.getTimeZone(timeZone);
    }

    private static LogLevel level() {
        return LogLevel
                .valueOf(System.getProperty("io.deephaven.internal.log.LoggerFactoryFile.level", "INFO")
                        .toUpperCase());
    }

    @Override
    public final Logger createInternal() {
        // todo: parameterize based on config
        final LogBufferPool bufferPool = new DynamicLogBufferPoolImpl("LogBufferPool", 1024, 1024);
        final LogEntryPool logEntryPool =
                new DynamicDelayedLogEntryUnsafePoolImpl("LogEntryPool", 32768);
        // note: this calls a thread per call; need to change dynamics
        final String header = null;
        final LogSink<?> logSink = new LogSinkImpl<>(getPath(), Integer.MAX_VALUE, null,
                logEntryPool, append(), new LogOutputCsvImpl(bufferPool), header, null);
        final String prefix = null;
        final LoggerTimeSource timeSource = new NullLoggerTimeSource();
        return new LoggerImpl(logEntryPool, logSink, prefix, level(), timeSource, timeZone(),
                showLevel(), showThreadName());
    }
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.logger;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.ClassUtil;
import io.deephaven.io.log.*;
import io.deephaven.io.log.impl.LogEntryPoolImpl;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.time.ZoneId;
import java.util.TimeZone;

/**
 * Preserve some of the simplicity of StreamLoggerImpl while also retaining the formatting functionality of LoggerImpl.
 */
public class ProcessStreamLoggerImpl extends LoggerImpl {

    public static Logger makeLogger(@NotNull final LoggerTimeSource timeSource,
            @NotNull final ZoneId zoneId) {
        return makeLogger(System.out, LogLevel.INFO, 1024, 2048, 1024, timeSource, zoneId);
    }

    /**
     * @deprecated prefer {@link #makeLogger(OutputStream, LogLevel, int, int, int, LoggerTimeSource, ZoneId)}
     */
    @Deprecated
    public static Logger makeLogger(@NotNull final LoggerTimeSource timeSource,
            @NotNull final TimeZone tz) {
        return makeLogger(System.out, LogLevel.INFO, 1024, 2048, 1024, timeSource, tz.toZoneId());
    }

    @SuppressWarnings({"WeakerAccess", "SameParameterValue"})
    public static Logger makeLogger(@NotNull final OutputStream outputStream,
            @NotNull final LogLevel loggingLevel,
            final int bufferSize,
            final int bufferCount,
            final int entryCount,
            @NotNull final LoggerTimeSource timeSource,
            @NotNull final ZoneId zoneId) {
        final LogEntryPool logEntryPool =
                new LogEntryPoolImpl(entryCount, LogBufferPool.ofStrict(bufferCount, bufferSize));
        return new ProcessStreamLoggerImpl(logEntryPool, outputStream, loggingLevel, timeSource, zoneId);
    }

    /**
     * @deprecated prefer {@link #makeLogger(OutputStream, LogLevel, int, int, int, LoggerTimeSource, ZoneId)}
     */
    @SuppressWarnings({"WeakerAccess", "SameParameterValue"})
    @Deprecated
    public static Logger makeLogger(@NotNull final OutputStream outputStream,
            @NotNull final LogLevel loggingLevel,
            final int bufferSize,
            final int bufferCount,
            final int entryCount,
            @NotNull final LoggerTimeSource timeSource,
            @NotNull final TimeZone tz) {
        return makeLogger(outputStream, loggingLevel, bufferSize, bufferCount, entryCount, timeSource, tz.toZoneId());
    }

    private ProcessStreamLoggerImpl(@NotNull final LogEntryPool logEntryPool,
            @NotNull final OutputStream outputStream,
            @NotNull final LogLevel loggingLevel,
            @NotNull final LoggerTimeSource timeSource,
            @NotNull final ZoneId zoneId) {
        super(logEntryPool, new Sink(outputStream, logEntryPool), null, loggingLevel, timeSource, zoneId, true, false);
    }

    /**
     * Specialized sink for stream loggers
     */
    private static class Sink implements LogSink<LogEntry> {

        private final OutputStream outputStream;
        private final LogEntryPool logEntryPool;

        private Interceptor<LogEntry>[] interceptors = null;

        private Sink(@NotNull final OutputStream outputStream,
                @NotNull final LogEntryPool logEntryPool) {
            this.outputStream = outputStream;
            this.logEntryPool = logEntryPool;
        }

        @Override
        public void write(@NotNull final LogEntry e) {
            try {
                InternalLoggerUtil.writeEntryToStream(e, outputStream, interceptors);
            } catch (IOException x) {
                throw new UncheckedIOException(x);
            } finally {
                e.clear();
                logEntryPool.give(e);
            }
        }

        @Override
        public void shutdown() {}

        @Override
        public void terminate() {}

        @Override
        public void addInterceptor(@NotNull final Interceptor<LogEntry> logEntryInterceptor) {
            interceptors =
                    ArrayUtil.pushArray(logEntryInterceptor, interceptors, ClassUtil.generify(Interceptor.class));
        }
    }
}

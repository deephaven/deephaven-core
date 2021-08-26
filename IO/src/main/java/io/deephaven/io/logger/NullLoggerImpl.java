package io.deephaven.io.logger;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.ClassUtil;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogEntryPool;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.log.LogSink;
import io.deephaven.io.log.impl.LogBufferPoolImpl;
import io.deephaven.io.log.impl.LogEntryPoolImpl;
import java.io.IOException;
import java.io.UncheckedIOException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.TimeZone;

/**
 * Logger implementation that calls Interceptors on log entries, but doesn't write them to anything. Note that this
 * implementation does not append the timestamp or level to the entries.
 */
public class NullLoggerImpl extends LoggerImpl {

    public static class NullLoggerTimeSource implements LoggerTimeSource {

        @Override
        public long currentTimeMicros() {
            return System.currentTimeMillis() * 1000;
        }
    }

    private NullLoggerImpl(@NotNull final LogEntryPool logEntryPool,
            @NotNull final LogLevel loggingLevel,
            @NotNull final LoggerTimeSource timeSource,
            final TimeZone tz) {
        super(logEntryPool, new NullLoggerImpl.Sink(logEntryPool), null, loggingLevel, timeSource, tz, true, false);
    }

    public NullLoggerImpl(@NotNull final LogLevel loggingLevel) {
        this(new LogEntryPoolImpl(1024, new LogBufferPoolImpl(2048, 1024)),
                loggingLevel,
                new NullLoggerTimeSource(),
                null);
    }

    /** Override to avoid writing timestamp and level to the entry, as it's assumed they'll be handled independently */
    @Override
    public LogEntry getEntry(LogLevel level, long currentTimeMicros, @Nullable Throwable t) {
        if (!isLevelEnabled(level)) {
            return EMPTY_LOG_ENTRY;
        } else {
            return logEntryPool.take().start(logSink, level, currentTimeMicros, t);
        }
    }

    /**
     * Specialized sink for null loggers
     */
    private static class Sink implements LogSink<LogEntry> {

        private final LogEntryPool logEntryPool;

        private Interceptor<LogEntry>[] interceptors = null;

        private Sink(@NotNull final LogEntryPool logEntryPool) {
            this.logEntryPool = logEntryPool;
        }

        @Override
        public void write(@NotNull final LogEntry entry) {
            try {
                for (int i = 0; i < entry.getBufferCount(); ++i) {
                    final ByteBuffer b = entry.getBuffer(i);
                    b.flip();
                }

                if (interceptors != null) {
                    for (final LogSink.Interceptor interceptor : interceptors) {
                        try {
                            // noinspection unchecked
                            interceptor.element(entry, entry);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                }
            } finally {
                entry.clear();
                logEntryPool.give(entry);
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

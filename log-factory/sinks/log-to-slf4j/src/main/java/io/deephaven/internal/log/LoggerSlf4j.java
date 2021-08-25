package io.deephaven.internal.log;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.ClassUtil;
import io.deephaven.base.Function;
import io.deephaven.base.pool.Pool;
import io.deephaven.base.pool.ThreadSafeLenientFixedSizePool;
import io.deephaven.io.log.LogBufferPool;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.log.LogSink;
import io.deephaven.io.log.impl.LogEntryImpl;
import io.deephaven.io.logger.Logger;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.slf4j.event.Level;

public final class LoggerSlf4j implements Logger {

    private static final Pool<ByteBuffer> buffers =
            new ThreadSafeLenientFixedSizePool<>(2048, new Function.Nullary<ByteBuffer>() {
                @Override
                public ByteBuffer call() {
                    return ByteBuffer.allocate(512);
                }
            }, null);

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

    private static Level getLevelSlf4j(LogLevel level) {
        if (level == LogLevel.INFO) {
            return Level.INFO;
        }
        if (level == LogLevel.WARN) {
            return Level.WARN;
        }
        if (level == LogLevel.ERROR) {
            return Level.ERROR;
        }
        if (level == LogLevel.DEBUG) {
            return Level.DEBUG;
        }
        if (level == LogLevel.TRACE) {
            return Level.TRACE;
        }
        if (level == LogLevel.FATAL) {
            return Level.ERROR;
        }
        throw new IllegalArgumentException("Unsupported log level " + level.getName());
    }

    private static class Entry extends LogEntryImpl {
        private org.slf4j.Logger log;
        private Level level;
        private boolean isFatal;

        public Entry(LogBufferPool logBufferPool) {
            super(logBufferPool);
            isFatal = false;
        }

        public Entry start(LogSink sink, org.slf4j.Logger log, LogLevel level, long currentTime) {
            return start(sink, log, level, currentTime, null);
        }

        public Entry start(LogSink sink, org.slf4j.Logger log, LogLevel level, long currentTime,
                Throwable t) {
            super.start(sink, level, currentTime, t);
            this.log = log;
            this.level = getLevelSlf4j(level);
            this.isFatal = level.equals(LogLevel.FATAL);
            return this;
        }

        public boolean isFatal() {
            return isFatal;
        }

        @Override
        public LogEntry endl() {
            super.end();
            return this;
        }
    }

    /** Static pool shared among all loggers */
    private static final Pool<Entry> entries =
            new ThreadSafeLenientFixedSizePool<>(1024, new Function.Nullary<Entry>() {
                @Override
                public Entry call() {
                    return new Entry(logBufferPool);
                }
            }, null);

    /** Specialized sink for DH loggers */
    private enum Sink implements LogSink<Entry> {
        INSTANCE;

        private Interceptor<Entry>[] interceptors = new Interceptor[0];

        static String toString(Entry e) {
            if (!e.isFatal() && e.getBufferCount() == 1 && e.getBuffer(0).hasArray()) {
                ByteBuffer b = e.getBuffer(0);
                // note: not flipping and going from [0, limit) b/c we want to leave the buffer in
                // the
                // same position for the interceptors to flip themselves
                // TODO (core#88): Figure out appropriate stdout / LogBuffer encoding
                return new String(b.array(), 0, b.position(), StandardCharsets.ISO_8859_1);
            } else {
                StringBuilder sb = new StringBuilder(e.size());
                if (e.isFatal()) {
                    sb.append("FATAL: ");
                }
                for (int i = 0; i < e.getBufferCount(); ++i) {
                    ByteBuffer b = e.getBuffer(i);
                    b.flip();
                    while (b.remaining() > 0) {
                        sb.append((char) b.get());
                    }
                    // note: after consuming buffer, the interceptors will be able to flip buffers
                }
                return sb.toString();
            }
        }

        @Override
        public void write(Entry e) {
            try {
                final String msg = toString(e);
                switch (e.level) {
                    case ERROR:
                        e.log.error(msg, e.getThrowable());
                        break;
                    case WARN:
                        e.log.warn(msg, e.getThrowable());
                        break;
                    case INFO:
                        e.log.info(msg, e.getThrowable());
                        break;
                    case DEBUG:
                        e.log.debug(msg, e.getThrowable());
                        break;
                    case TRACE:
                        e.log.trace(msg, e.getThrowable());
                        break;
                    default:
                        throw new IllegalStateException("Unexpected level " + e.level);
                }

                for (int i = 0; i < e.getBufferCount(); ++i) {
                    final ByteBuffer b = e.getBuffer(i);
                    b.flip();
                }

                for (final LogSink.Interceptor interceptor : interceptors) {
                    // noinspection unchecked
                    interceptor.element(e, e);
                }
            } catch (IOException ioException) {
                throw new UncheckedIOException(ioException);
            } finally {
                if (e.isFatal()) {
                    System.exit(1);
                }
                e.clear();
                entries.give(e);
            }
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
            interceptors = ArrayUtil.pushArray(entryInterceptor, interceptors,
                    ClassUtil.generify(Interceptor.class));
        }
    }

    // ---------------------------------------------------------------------------------------------

    private final org.slf4j.Logger log;

    public LoggerSlf4j(org.slf4j.Logger log) {
        this.log = Objects.requireNonNull(log);
    }

    private Entry startEntry(LogLevel level, long currentTime) {
        return entries.take().start(Sink.INSTANCE, log, level, currentTime);
    }

    private Entry startEntry(LogLevel level, long currentTime, Throwable t) {
        return entries.take().start(Sink.INSTANCE, log, level, currentTime, t);
    }

    @Override
    public LogEntry getEntry(LogLevel level) {
        return isLevelEnabled(level) ? startEntry(level, System.currentTimeMillis() * 1000)
                : LogEntry.NULL;
    }

    @Override
    public LogEntry getEntry(LogLevel level, Throwable t) {
        return isLevelEnabled(level) ? startEntry(level, System.currentTimeMillis() * 1000, t)
                : LogEntry.NULL;
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
        throw new UnsupportedOperationException("not supported");
    }

    @Override
    public LogSink getSink() {
        return Sink.INSTANCE;
    }

    @Override
    public boolean isLevelEnabled(LogLevel level) {
        if (level == LogLevel.INFO) {
            return log.isInfoEnabled();
        }
        if (level == LogLevel.WARN) {
            return log.isWarnEnabled();
        }
        if (level == LogLevel.ERROR) {
            return log.isErrorEnabled();
        }
        if (level == LogLevel.DEBUG) {
            return log.isDebugEnabled();
        }
        if (level == LogLevel.TRACE) {
            return log.isTraceEnabled();
        }
        if (level == LogLevel.FATAL) {
            return log.isErrorEnabled();
        }
        throw new IllegalArgumentException("Do not support level " + level.getName());
    }

    @Override
    public void shutdown() {}

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
        throw new UnsupportedOperationException();
    }

    @Override
    public void fatal(Object object, Throwable t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void error(Object object) {
        log.error(object.toString());
    }

    @Override
    public void error(Object object, Throwable t) {
        log.error(object.toString(), t);
    }

    @Override
    public void warn(Object object) {
        log.warn(object.toString());
    }

    @Override
    public void warn(Object object, Throwable t) {
        log.warn(object.toString(), t);
    }

    @Override
    public void info(Object object) {
        log.info(object.toString());
    }

    @Override
    public void info(Object object, Throwable t) {
        log.info(object.toString(), t);
    }

    @Override
    public void debug(Object object) {
        log.debug(object.toString());
    }

    @Override
    public void debug(Object object, Throwable t) {
        log.debug(object.toString(), t);
    }

    @Override
    public void trace(Object object) {
        log.trace(object.toString());
    }

    @Override
    public void trace(Object object, Throwable t) {
        log.trace(object.toString(), t);
    }

    @Override
    public void email(Object object) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void email(Object object, Throwable t) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isFatalEnabled() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isErrorEnabled() {
        return log.isErrorEnabled();
    }

    @Override
    public boolean isWarnEnabled() {
        return log.isWarnEnabled();
    }

    @Override
    public boolean isInfoEnabled() {
        return log.isInfoEnabled();
    }

    @Override
    public boolean isDebugEnabled() {
        return log.isDebugEnabled();
    }

    @Override
    public boolean isTraceEnabled() {
        return log.isTraceEnabled();
    }

    @Override
    public boolean isEmailEnabled() {
        throw new UnsupportedOperationException();
    }
}

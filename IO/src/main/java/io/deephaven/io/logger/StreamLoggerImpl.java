/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.ClassUtil;
import io.deephaven.base.pool.Pool;
import io.deephaven.base.pool.ThreadSafeLenientFixedSizePool;
import io.deephaven.io.log.LogBufferPool;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.log.LogSink;
import io.deephaven.io.log.impl.LogEntryImpl;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

public class StreamLoggerImpl implements Logger {

    /**
     * Static buffer pool, shared among all SystemOut loggers
     */
    private static final Pool<ByteBuffer> buffers = new ThreadSafeLenientFixedSizePool<>(2048,
        () -> ByteBuffer.allocate(512),
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
     * Specialized entries for SystemOut loggers
     */
    private static class Entry extends LogEntryImpl {
        private OutputStream stream;

        public Entry(LogBufferPool logBufferPool) {
            super(logBufferPool);
        }

        public Entry start(LogSink sink, OutputStream stream, LogLevel level, long currentTime) {
            return start(sink, stream, level, currentTime, null);
        }

        public Entry start(LogSink sink, OutputStream stream, LogLevel level, long currentTime,
            Throwable t) {
            super.start(sink, level, currentTime, t);
            this.stream = stream;
            return this;
        }
    }

    /**
     * Static pool shared among all loggers
     */
    private static final Pool<Entry> entries = new ThreadSafeLenientFixedSizePool<>(1024,
        () -> new Entry(logBufferPool),
        null);

    /**
     * Specialized sink for stream loggers
     */
    private static class Sink implements LogSink<Entry> {

        private Interceptor<Entry>[] interceptors = null;

        @Override
        public void write(Entry e) {
            try {
                InternalLoggerUtil.writeEntryToStream(e, e.stream, interceptors);
            } catch (IOException x) {
                throw new UncheckedIOException(x);
            } finally {
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

    private static final Sink SINK = new Sink();

    // ---------------------------------------------------------------------------------------------

    private final OutputStream stream;
    private LogLevel loggingLevel;

    public StreamLoggerImpl() {
        this(System.out, LogLevel.INFO);
    }

    public StreamLoggerImpl(OutputStream stream, LogLevel loggingLevel) {
        this.stream = stream;
        this.loggingLevel = loggingLevel;
    }

    private Entry startEntry(LogLevel level, long currentTime) {
        return entries.take().start(SINK, stream, level, currentTime);
    }

    private Entry startEntry(LogLevel level, long currentTime, Throwable t) {
        return entries.take().start(SINK, stream, level, currentTime, t);
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
        loggingLevel = level;
    }

    @Override
    public LogSink getSink() {
        return SINK;
    }

    @Override
    public boolean isLevelEnabled(LogLevel level) {
        return loggingLevel.getPriority() <= level.getPriority();
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

    private void logObjectWithThrowable(LogLevel level, Object object, Throwable t) {
        LogEntry entry = getEntry(level, System.currentTimeMillis() * 1000);

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

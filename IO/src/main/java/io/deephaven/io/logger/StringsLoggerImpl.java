/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.logger;

import io.deephaven.base.Function;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.pool.Pool;
import io.deephaven.base.pool.ThreadSafeLenientFixedSizePool;
import io.deephaven.io.log.LogEntry;
import io.deephaven.io.log.LogLevel;
import io.deephaven.io.log.LogSink;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Queue;

public class StringsLoggerImpl<E extends LogEntry> implements Logger {

    // the entry pool
    private final Pool<E> entries;

    // the output buffer
    private final LogOutput outputBuffer;

    // the output strings, and a sink that writes them
    private final Queue<String> output;

    private final LogSink<E> sink = new LogSink<E>() {
        private final ByteArrayOutputStream stream = new ByteArrayOutputStream(1024);
        private Interceptor<E>[] interceptors = null;

        @Override
        public void write(E e) {
            LogOutput outputData = e.writing(outputBuffer);
            try {
                if (null != e.getThrowable()) {
                    outputData.append(e.getThrowable());
                }
                flushOutput(e, outputData);
            } finally {
                e.written(outputData);
                entries.give(e);
            }
        }

        private void flushOutput(E e, LogOutput outputData) {
            stream.reset();
            if (outputData.getBufferCount() == 1 && outputData.getBuffer(0).hasArray()) {
                ByteBuffer b = outputData.getBuffer(0);
                b.flip();
                byte[] ba = b.array();
                synchronized (stream) {
                    stream.write(ba, 0, b.limit());
                }
            } else {
                synchronized (stream) {
                    for (int i = 0; i < outputData.getBufferCount(); ++i) {
                        ByteBuffer b = outputData.getBuffer(i);
                        b.flip();
                        while (b.remaining() > 0) {
                            stream.write(b.get());
                        }
                    }
                }
            }
            output.add(stream.toString());
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
        public void addInterceptor(Interceptor<E> entryInterceptor) {
            throw new UnsupportedOperationException("no interceptors allowed");
        }
    };

    // ---------------------------------------------------------------------------------------------

    private LogLevel loggingLevel;

    public StringsLoggerImpl(Function.Nullary<E> entryFactory, int entryPoolSize, LogOutput outputBuffer,
            LogLevel loggingLevel) {
        this.loggingLevel = loggingLevel;
        this.entries = new ThreadSafeLenientFixedSizePool<E>(entryPoolSize, entryFactory, null);
        this.outputBuffer = outputBuffer;
        this.output = new ArrayDeque<>();
    }

    public String next() {
        return output.poll();
    }

    public String[] takeAll() {
        String[] result = output.toArray(new String[output.size()]);
        output.clear();
        return result;
    }

    private LogEntry startEntry(LogLevel level, long currentTime) {
        return entries.take().start(sink, level, currentTime);
    }

    private LogEntry startEntry(LogLevel level, long currentTime, Throwable t) {
        return entries.take().start(sink, level, currentTime, t);
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
        loggingLevel = level;
    }

    @Override
    public LogSink getSink() {
        return sink;
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

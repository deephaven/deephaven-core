/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.pool.Pool;
import io.deephaven.io.log.impl.LogSinkImpl;

import java.io.IOException;
import java.text.DateFormat;
import java.util.concurrent.CopyOnWriteArraySet;

public interface LogSink<T extends LogSink.Element> {

    /**
     * Write an element.
     * 
     * @param e
     */
    void write(T e);

    /**
     * Shutdown cleanly, flushing all current elements.
     */
    void shutdown();

    /**
     * Shutdown aggressively, without trying to flush.
     */
    void terminate();

    /**
     * One element of a log sink - guaranteed to be logged without being split over rolling file
     * boundaries, etc.
     */
    interface Element {
        long getTimestampMicros();

        LogLevel getLevel();

        Throwable getThrowable();

        LogOutput writing(LogOutput outputBuffer);

        void written(LogOutput outputBuffer);
    }

    /**
     * An interceptor is called with each element logged, *and* with the formatted output. It will
     * receive buffers that are flipped, and should not change the position or limit of these
     * buffers.
     */
    interface Interceptor<T extends Element> {
        void element(T e, LogOutput output) throws IOException;
    }

    /**
     * You can add as many interceptors to a log sink as you like.
     */
    void addInterceptor(Interceptor<T> interceptor);

    /**
     * Shutdown helpers to force flush of outstanding elements
     */
    class Shutdown {
        // static list of log sinks for shutdown
        private static CopyOnWriteArraySet<LogSink> allSinks = new CopyOnWriteArraySet<LogSink>();

        public static void addSink(LogSink sink) {
            allSinks.add(sink);
        }

        public static void removeSink(LogSink sink) {
            allSinks.remove(sink);
        }

        public static LogSink[] getSinks() {
            return allSinks.toArray(new LogSink[0]);
        }

        public static void shutdown() {
            for (LogSink sink : allSinks) {
                sink.shutdown();
            }
        }

        public static void terminate() {
            for (LogSink sink : allSinks) {
                sink.terminate();
            }
        }
    }

    // must also be a thread!
    interface LogSinkWriter<S extends LogSink<? extends Element>> {
        void addLogSink(S sink);
    }

    interface Factory<T extends LogSink.Element> {
        LogSink<T> create(String basePath, int rollInterval, DateFormat rollFormat,
            Pool<T> elementPool, boolean append, LogOutput outputBuffer, String header,
            LogSinkWriter<LogSinkImpl<T>> maybeWriter);
    }

    public static final Null NULL = new Null();

    public static class Null implements LogSink {
        @Override
        public void write(Element e) {}

        @Override
        public void shutdown() {}

        @Override
        public void terminate() {}

        @Override
        public void addInterceptor(Interceptor interceptor) {}
    }
}

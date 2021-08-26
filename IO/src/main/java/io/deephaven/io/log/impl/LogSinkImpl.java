/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log.impl;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.ClassUtil;
import io.deephaven.base.LockFreeArrayQueue;
import io.deephaven.base.UnfairSemaphore;
import io.deephaven.base.pool.Pool;
import io.deephaven.base.system.AsyncSystem;
import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.base.verify.Assert;
import io.deephaven.base.log.LogOutput;
import io.deephaven.io.log.LogSink;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

public class LogSinkImpl<T extends LogSink.Element> implements LogSink<T> {

    private static final int EXIT_STATUS = 1;

    private static Factory _FACTORY = new Factory() {
        @Override
        public LogSink create(String basePath, int rollInterval, DateFormat rollFormat, Pool elementPool,
                boolean append, LogOutput outputBuffer, String header, LogSinkWriter maybeWriter) {
            // noinspection unchecked
            return new LogSinkImpl(basePath, rollInterval, rollFormat, elementPool, append, outputBuffer, header,
                    maybeWriter);
        }
    };

    public static <T extends LogSink.Element> Factory<T> FACTORY() {
        // noinspection unchecked
        return _FACTORY;
    }

    // ################################################################

    // complete path to log file; rolling files will have date/time appended
    private final String basePath;

    // roll interval, in micros
    private final long rollIntervalMicros;

    // the format for rolling file timestamps
    private final DateFormat rollFormat;

    // pool to which written buffers are returned
    private final Pool<T> elementPool;

    // should we append to the output files?
    private final boolean append;

    // queue of buffers with data to write
    private final LockFreeArrayQueue<T> outputQueue;

    // Log output passed to Element.writing(); elements can format themselves here or
    // return some other LogOutput object. This may also be null, in which case the elements
    // must create their own output.
    private final LogOutput outputBuffer;

    // File header (optional)
    private final String header;

    // the interceptors
    private Interceptor<T>[] interceptors = null;

    private final boolean passedInWriter;
    private final LogSinkWriter<LogSinkImpl<T>> writer;
    private final Thread writerThread;

    // base timestamp in micros of the current interval
    private long currentIntervalMicros;

    // current output path
    private String currentPath;

    // the current output file
    private FileChannel outputFile;

    // the name of the link to the current file
    private Path linkPath;

    // true if the platform doesn't support creating links
    private boolean supportsLinks;

    // the writer thread
    // private final WriterThread writerThread;

    // shutdown flag
    private volatile boolean shutdown;

    // released on shutdown
    private final UnfairSemaphore writtenOnShutdown;

    // how many times to spin on enqueue before yielding
    private static final int ENQUEUE_SPIN_COUNT = 10000;

    // the default roll interval
    public static final int ROLL_INTERVAL = 3600 * 1000;

    // the default date format
    public static final DateFormat DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd-HHmmss.SSSZ");

    public static final BigWriterThread globalWriterThread =
            new BigWriterThread("LogSinkImpl.GlobalWriterThread", 1000000); // park for 1 milli

    /**
     * Constructor
     */
    public LogSinkImpl(String basePath, long rollIntervalMillis, Pool<T> elementPool) {
        this(basePath, rollIntervalMillis, DATE_FORMAT, elementPool, true, null, null);
    }

    /**
     * Constructor
     */
    public LogSinkImpl(String basePath, long rollIntervalMillis, Pool<T> elementPool, LogOutput outputBuffer) {
        this(basePath, rollIntervalMillis, DATE_FORMAT, elementPool, true, outputBuffer, null);
    }

    /**
     * Constructor
     */
    public LogSinkImpl(String basePath, long rollIntervalMillis, Pool<T> elementPool, boolean append) {
        this(basePath, rollIntervalMillis, DATE_FORMAT, elementPool, append, null, null);
    }

    /**
     * Constructor
     */
    public LogSinkImpl(String basePath, long rollIntervalMillis, Pool<T> elementPool, boolean append,
            LogOutput outputBuffer) {
        this(basePath, rollIntervalMillis, DATE_FORMAT, elementPool, append, outputBuffer, null);
    }

    /**
     * Constructor
     */
    public LogSinkImpl(String basePath, long rollIntervalMillis, DateFormat rollFormat, Pool<T> elementPool,
            boolean append) {
        this(basePath, rollIntervalMillis, rollFormat, elementPool, append, null, null);
    }

    /**
     * Constructor
     */
    public LogSinkImpl(String basePath, long rollIntervalMillis, DateFormat rollFormat, Pool<T> elementPool,
            boolean append,
            LogOutput outputBuffer, String header) {
        this(basePath, rollIntervalMillis, rollFormat, elementPool, append, outputBuffer, header, null);
    }

    /**
     * Constructor
     */
    public LogSinkImpl(String basePath, long rollIntervalMillis, DateFormat rollFormat, Pool<T> elementPool,
            boolean append,
            LogOutput outputBuffer, String header, LogSinkWriter<LogSinkImpl<T>> maybeWriter) {
        this.basePath = basePath;
        this.rollIntervalMicros = rollIntervalMillis * 1000L;
        this.rollFormat = null == rollFormat ? null : (DateFormat) rollFormat.clone(); // make sure we have one instance
                                                                                       // per sink
        this.elementPool = elementPool;
        this.append = append;
        this.outputQueue = new LockFreeArrayQueue<>(20); // 2^20 = 1m entries
        this.outputBuffer = outputBuffer;
        this.header = header;

        this.linkPath = new File(basePath + ".current").toPath();
        this.supportsLinks = !System.getProperty("os.name").toLowerCase().contains("win"); // enabled until we find out
                                                                                           // otherwise, except on
                                                                                           // windows

        // don't create the output file until we actually write to it
        this.currentIntervalMicros = 0;
        this.currentPath = null;
        this.outputFile = null;

        shutdown = false;
        writtenOnShutdown = new UnfairSemaphore(1, 1000);
        writtenOnShutdown.acquire(1);

        passedInWriter = maybeWriter != null;
        if (passedInWriter) {
            writer = maybeWriter;
            writer.addLogSink(this); // will start if not started
        } else {
            final WriterThread<T> thread = new WriterThread<>("LogSinkImpl.WriterThread-" + basePath);
            thread.setDaemon(true);
            writer = thread;
            writer.addLogSink(this); // will start it
        }

        writerThread = (Thread) writer;
        Shutdown.addSink(this);
    }

    /**
     * Return a string representation
     */
    public String toString() {
        return "LogSinkImpl(" + basePath + ")";
    }

    /**
     * add a buffer to the output queue
     */
    @Override
    public void write(T e) {
        if (shutdown)
            return; // don't want to add anything new to the queue, b/c we can cause isOpenAfterWrite() to never return
                    // (if we keep the queue full)

        int spins = 0;
        while (!outputQueue.enqueue(e)) {

            if (shutdown)
                return; // don't want to spin forever if nothing is trying to drain the queue

            if (++spins > ENQUEUE_SPIN_COUNT) {
                LockSupport.unpark(writerThread);
                Thread.yield();
                spins = 0;
            }
        }

        if (!passedInWriter)
            LockSupport.unpark(writerThread);
    }

    private void notifyShutdownWritten() {
        writtenOnShutdown.release(1);
    }

    /**
     * Shutdown the sink - does not return until all entries have been written.
     */
    @Override
    public void shutdown() {
        shutdown = true;
        LockSupport.unpark(writerThread);
        writtenOnShutdown.acquire(1);
        writtenOnShutdown.release(1);
    }

    /**
     * Terminate the sink - does not guarantee entries will ever be written, but will not block shutdown() calls.
     */
    @Override
    public void terminate() {
        shutdown = true;
        // We're not going to write any more entries, so don't block shutdown calls.
        // If we release the semaphore extra times, it won't hurt anything - the semaphore could be replaced
        // by a boolean and a condition variable in its current usage.
        writtenOnShutdown.release(1);
    }

    /**
     * Add an interceptor
     */
    @Override
    public void addInterceptor(Interceptor<T> interceptor) {
        interceptors =
                ArrayUtil.pushArray(interceptor, interceptors, ClassUtil.<Interceptor<T>>generify(Interceptor.class));
    }

    // -------------------------------------------------------------------------------------------

    private boolean isOpenAfterWrite() throws IOException {
        while (didWrite()) {
        }
        return isOpen();
    }

    private boolean isOpen() throws IOException {
        if (shutdown) {
            if (outputFile != null) {
                outputFile.close();
            }
            return false;
        }
        return true;
    }

    private boolean didWrite() throws IOException {
        final T e = outputQueue.dequeue();
        if (e == null)
            return false;
        writeOut(e);
        return true;
    }

    private void writeOut(T e) throws IOException {
        checkOutputFile(e.getTimestampMicros());
        LogOutput outputData = e.writing(outputBuffer);
        if (null != e.getThrowable()) {
            outputData.append(e.getThrowable());
        }
        flushOutput(e, outputData);
        e.written(outputData);
        elementPool.give(e);
    }

    /**
     * The writer thread
     */
    private class WriterThread<T extends LogSink.Element> extends Thread implements LogSinkWriter<LogSinkImpl<T>> {

        private final AtomicBoolean started;
        private final PrintStream err;

        private WriterThread(String name) {
            super(name);
            started = new AtomicBoolean(false);
            err = PrintStreamGlobals.getErr();
        }

        @Override
        public void addLogSink(final LogSinkImpl<T> sink) {
            Assert.eq(sink, "sink", LogSinkImpl.this);
            if (started.compareAndSet(false, true))
                start();
        }

        @Override
        public void run() {
            try {
                started.set(true);
                while (isOpenAfterWrite()) {
                    LockSupport.park(this);
                }
                notifyShutdownWritten();
            } catch (Throwable t) {
                try {
                    terminate();
                } catch (Throwable t2) {
                    t.addSuppressed(t2);
                } finally {
                    AsyncSystem.exitCaught(this, t, EXIT_STATUS, err,
                            "LogSinkImpl: unable to write log entry");
                }
            }
        }
    }


    // a spinning thread that takes care of all log sinks
    public static class BigWriterThread extends Thread implements LogSinkWriter<LogSinkImpl<?>> {
        private final LockFreeArrayQueue<LogSinkImpl<? extends LogSink.Element>> toWriteOut;
        private final UnfairSemaphore semaphoreEntries;
        private final AtomicBoolean started;
        private final long parkNanos;
        private final PrintStream err;

        private BigWriterThread(String name, long parkNanos) {
            super(name);
            this.parkNanos = parkNanos;

            // hopefully we don't need more than 16384 log sinks!
            toWriteOut = new LockFreeArrayQueue<>(14);
            semaphoreEntries = new UnfairSemaphore(0, 1000);
            started = new AtomicBoolean(false);
            err = PrintStreamGlobals.getErr();
        }

        @Override
        public void addLogSink(final LogSinkImpl<?> impl) {
            Assert.eqTrue(toWriteOut.enqueue(impl), "toWriteOut.add(impl)");
            semaphoreEntries.release(1);
            if (started.compareAndSet(false, true))
                start();
        }

        @Override
        public void run() {
            started.set(true);

            waitForSomeEntries();

            LogSinkImpl<? extends Element> impl;
            int spinsSinceLastChange = 0;
            while (true) {
                impl = toWriteOut.dequeue();
                Assert.neqNull(impl, "impl"); // b/c we are using a semaphore to wait, there should always be something
                                              // here
                try {
                    if (impl.didWrite()) {
                        spinsSinceLastChange = 0;
                        Assert.eqTrue(toWriteOut.enqueue(impl), "toWriteOut.enqueue(impl)");
                    } else if (impl.isOpen()) {
                        Assert.eqTrue(toWriteOut.enqueue(impl), "toWriteOut.enqueue(impl)");
                    } else {
                        spinsSinceLastChange = 0;
                        impl.notifyShutdownWritten();
                        Assert.eqTrue(semaphoreEntries.tryAcquire(1), "semaphore.tryAcquire(1)");
                        waitForSomeEntries();
                    }

                    // give the writer two passes over all the log sinks...
                    // if nothing has changed, park for up to 1 millis
                    if (++spinsSinceLastChange > 2 * semaphoreEntries.availablePermits()) {
                        spinsSinceLastChange = 0;
                        LockSupport.parkNanos(this, parkNanos); // 1 millis
                    }

                } catch (Throwable t) {
                    try {
                        terminateAll(impl);
                    } catch (Throwable t2) {
                        t.addSuppressed(t2);
                    } finally {
                        AsyncSystem.exitCaught(this, t, EXIT_STATUS, err,
                                "LogSinkImpl: unable to write log entry");
                    }
                    return;
                }
            }
        }

        private void terminateAll(LogSinkImpl<? extends Element> impl) {
            impl.terminate();
            while ((impl = toWriteOut.dequeue()) != null) {
                impl.terminate();
            }
        }

        private void waitForSomeEntries() {
            semaphoreEntries.acquire(1); // wait until we have at least one
            semaphoreEntries.release(1);
        }
    }

    private void checkOutputFile(long nowMicros) throws IOException {
        boolean updateLink = false;
        if (outputFile == null) {
            currentIntervalMicros = nowMicros - (rollIntervalMicros == 0 ? 0 : (nowMicros % rollIntervalMicros));
            // note: first file has complete timestamp
            currentPath = stampedOutputFilePath(nowMicros);
            outputFile = new FileOutputStream(currentPath, append).getChannel();
            writeHeader();
            updateLink = true;
        } else if (rollIntervalMicros > 0 && nowMicros > currentIntervalMicros + rollIntervalMicros) {
            outputFile.close();
            currentIntervalMicros = nowMicros - (nowMicros % rollIntervalMicros);
            currentPath = stampedOutputFilePath(currentIntervalMicros);
            outputFile = new FileOutputStream(currentPath, append).getChannel();
            writeHeader();
            updateLink = true;
        }

        if (updateLink && supportsLinks) {
            try {
                Files.deleteIfExists(linkPath);
                Files.createLink(linkPath, new File(currentPath).toPath());
            } catch (UnsupportedOperationException x) {
                supportsLinks = false;
            }
        }
    }

    private String stampedOutputFilePath(long nowMicros) {
        return rollFormat == null ? basePath : basePath + "." + rollFormat.format(new Date(nowMicros / 1000));
    }

    private void writeHeader() throws IOException {
        if (header != null) {
            outputBuffer.start().append(this.header).nl().close();
            flushOutput(null, outputBuffer);
            outputBuffer.clear();
        }
    }

    private void flushOutput(T e, LogOutput data) throws IOException {
        int n = data.getBufferCount();
        for (int i = 0; i < n; ++i) {
            ByteBuffer b = data.getBuffer(i);
            b.flip();
        }

        if (e != null) {
            Interceptor<T>[] localInterceptors = interceptors;
            if (localInterceptors != null) {
                for (int i = 0; i < localInterceptors.length; ++i) {
                    localInterceptors[i].element(e, data);
                }
            }
        }

        for (int i = 0; i < n; ++i) {
            ByteBuffer b = data.getBuffer(i);
            while (b.remaining() > 0) {
                if (outputFile.write(b) == 0) {
                    // this is a file channel, so if we write zero bytes the disk is full - don't bang our heads against
                    // the wall
                    break;
                }
            }
        }
    }
}

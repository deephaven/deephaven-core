/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.tablelogger;

import io.deephaven.base.stats.Counter;
import io.deephaven.base.stats.Stats;
import io.deephaven.base.stats.Value;
import io.deephaven.base.system.AsyncSystem;
import io.deephaven.base.system.PrintStreamGlobals;
import io.deephaven.util.pool.PoolEx;
import io.deephaven.util.pool.ThreadSafeFixedSizePool;

import java.io.IOException;
import java.io.PrintStream;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Common parts of the generated TableLoggers.
 *
 * It is "2" so that we can change the implementation details from TableLoggerImpl; and existing client code will still
 * compile. Otherwise, there is a chicken and egg problem, with clients not being able to run the new TableLoggerFactory
 * using modules that contain their logger classes.
 */
public abstract class TableLoggerImpl2<T extends WritableRowContainer> implements TableLogger {
    protected TableWriter writer;
    protected PoolEx<T> setterPool;
    protected volatile boolean isClosed;
    protected volatile boolean isShuttingDown;
    private boolean initialized = false;
    protected final AtomicInteger outstandingSetters = new AtomicInteger(0);
    private volatile boolean isWriting = false;

    private final Value writtenCount;

    public TableLoggerImpl2(String tablename) {
        Objects.requireNonNull(tablename);
        final String loggerName = "Logger-" + "-" + tablename;
        writtenCount = Stats.makeItem(loggerName, "writtenCount", Counter.FACTORY).getValue();
    }

    public abstract class BaseSetter<T2 extends BaseSetter<T2>> implements WritableRowContainer {
        protected Row row;
        protected T2 next;

        public T2 getNext() {
            return next;
        }

        public void setNext(final T2 other) {
            next = other;
        }

        protected BaseSetter() {
            row = writer.getRowWriter();
        }

        @Override
        public Row getRow() {
            return row;
        }

        protected void setRowFlags(Row.Flags rowFlags) {
            row.setFlags(rowFlags);
        }

        @Override
        public void writeRow() throws IOException {
            row.writeRow();
        }

        @Override
        public void release() {
            // noinspection unchecked
            setterPool.give((T) this);
        }
    }

    @Override
    public final synchronized void init(final TableWriter tableWriter, final int queueSize) throws IOException {
        if (this.initialized) {
            return;
        }

        writer = tableWriter;

        setterPool = new ThreadSafeFixedSizePool<>(queueSize, this::createSetter, null);

        this.initialized = true;
    }

    protected abstract T createSetter();

    protected abstract String threadName();

    protected final boolean isInitialized() {
        return initialized;
    }

    /**
     * Check the condition and throw an IllegalStateException if it is not met.
     *
     * @param condition the condition to check
     * @param message useful information for the IllegalStateException
     * @throws IllegalStateException if the condition is false
     */
    protected final void verifyCondition(final boolean condition, final String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    @Override
    public final void shutdown() {
        isShuttingDown = true;
        while (outstandingSetters.getAndDecrement() > 0) {
            setterPool.take();
        }
    }

    private static final int EXIT_STATUS = 1;
    private static final PrintStream err = PrintStreamGlobals.getErr();

    private void exit(Throwable t) {
        AsyncSystem.exitCaught(Thread.currentThread(), t, EXIT_STATUS, err, "Unable to write log entry");
    }

    protected final void flush(final T setter) {
        try {
            tryWrite(setter);
        } catch (IOException x) {
            if (isClosed()) {
                err.println(String.format(
                        "TableLogger.flush: caught exception in thread %s. Unable to write log entry. "
                                + "Logger already closed, not invoking shutdown.",
                        Thread.currentThread().getName()));
                x.printStackTrace(err);
            } else {
                exit(x);
            }
        } catch (Throwable t) {
            exit(t);
        }
    }

    /**
     * Wait for all currently populated rows to be written.
     */
    public final void waitDone() {
        while (notDone()) {
            LockSupport.parkNanos(1000000L);
        }
    }

    public final boolean waitDone(final long millis) {
        final long endTime = System.currentTimeMillis() + millis;
        while (endTime > System.currentTimeMillis() && notDone()) {
            LockSupport.parkNanos(1000000L);
        }
        return !notDone();
    }

    private boolean notDone() {
        return !isClosed && isWriting;
    }

    @Override
    public final void close() throws IOException {
        isClosed = true;
        writer.close();
    }

    @Override
    public final boolean isClosed() {
        return isClosed;
    }

    private void tryWrite(final T setter) throws IOException {
        isWriting = true;
        try {
            if (isClosed) {
                return;
            }
            setter.writeRow();
            setter.release();
            writtenCount.increment(1);
        } finally {
            isWriting = false;
        }
    }
}

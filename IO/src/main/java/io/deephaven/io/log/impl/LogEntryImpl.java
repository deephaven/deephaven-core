/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.text.TimestampBuffer;
import io.deephaven.base.text.TimestampBufferMicros;
import io.deephaven.io.log.*;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class LogEntryImpl extends LogOutputCsvImpl implements LogEntry {

    private static final AtomicLong starts = new AtomicLong(0);
    private static final AtomicLong ends = new AtomicLong(0);

    public static long getDifferenceInStartToEnds() {
        final long e = ends.get(); // getting this one first
        final long s = starts.get();
        return s - e; // should always be >= 0 ?
    }


    // the timestamp of this entry (when start() was last called)
    private long timestamp;

    // the level of this entry (set when start() was last called)
    private LogLevel level;

    // the throwable attached to this entry, set on every call to start()
    private Throwable throwable;

    // the sink where this entry will go, set on every call to start()
    private LogSink sink;

    /**
     * Constructor
     * 
     * @param bufferPool where we get our buffers
     */
    public LogEntryImpl(LogBufferPool bufferPool) {
        super(bufferPool);
        this.timestamp = System.currentTimeMillis() * 1000;
        this.sink = null;
    }

    /**
     * Notifies us that the log driver has given this instance out as a new entry
     */
    @Override
    public LogEntry start(final LogSink sink, final LogLevel level) {
        return start(sink, level, System.currentTimeMillis() * 1000);
    }

    @Override
    public LogEntry start(final LogSink sink, final LogLevel level, final long currentTimeMicros) {
        return start(sink, level, currentTimeMicros, null);
    }

    @Override
    public LogEntry start(final LogSink sink, final LogLevel level, final long currentTimeMicros, final Throwable t) {
        super.start();
        starts.getAndIncrement();
        this.timestamp = currentTimeMicros;
        this.level = level;
        this.sink = sink;
        this.throwable = t;
        return this;
    }

    @Override
    public LogEntry end() {
        close();
        sink.write(this);
        ends.getAndIncrement();
        return this;
    }

    @Override
    public LogEntry endl() {
        nl();
        end();
        return this;
    }

    // ------------------------------------------------------------------------------------------------
    // LogSink.Element implementation
    // ------------------------------------------------------------------------------------------------

    @Override
    public long getTimestampMicros() {
        return timestamp;
    }

    @Override
    public LogLevel getLevel() {
        return level;
    }

    @Override
    public Throwable getThrowable() {
        return throwable;
    }

    @Override
    public LogOutput writing(LogOutput outputBuffer) {
        return this;
    }

    @Override
    public void written(LogOutput outputBuffer) {
        // assert outputBuffer == this;
        super.clear();
    }

    // ------------------------------------------------------------------------------------------------
    // LogEntry methods covariant with LogOutput methods
    // ------------------------------------------------------------------------------------------------

    @Override
    public LogEntry append(final boolean b) {
        super.append(b);
        return this;
    }

    @Override
    public LogEntry append(final char c) {
        super.append(c);
        return this;
    }

    @Override
    public LogEntry append(final short s) {
        super.append(s);
        return this;
    }

    @Override
    public LogEntry append(final int i) {
        super.append(i);
        return this;
    }

    @Override
    public LogEntry append(final long l) {
        super.append(l);
        return this;
    }

    @Override
    public LogEntry appendDouble(final double f) {
        super.appendDouble(f);
        return this;
    }

    @Override
    public LogEntry append(final LogOutputAppendable appendable) {
        super.append(appendable);
        return this;
    }

    @Override
    public LogEntry append(LongFormatter formatter, long n) {
        super.append(formatter, n);
        return this;
    }

    @Override
    public <T> LogEntry append(ObjFormatter<T> objFormatter, T t) {
        super.append(objFormatter, t);
        return this;
    }

    @Override
    public <T> LogEntry append(ObjIntIntFormatter<T> objFormatter, T t, int nOffset, int nLength) {
        super.append(objFormatter, t, nOffset, nLength);
        return this;
    }

    @Override
    public <T, U> LogEntry append(ObjObjFormatter<T, U> objObjFormatter, T t, U u) {
        super.append(objObjFormatter, t, u);
        return this;
    }

    @Override
    public LogEntry append(final CharSequence seq) {
        super.append(seq);
        return this;
    }

    @Override
    public LogEntry append(final CharSequence seq, final int start, final int length) {
        super.append(seq, start, length);
        return this;
    }

    @Override
    public LogEntry append(final ByteBuffer bb) {
        super.append(bb);
        return this;
    }

    @Override
    public LogEntry appendTimestamp(final long utcMillis, final TimestampBuffer tb) {
        super.appendTimestamp(utcMillis, tb);
        return this;
    }

    @Override
    public LogEntry appendTimestampMicros(final long utcMicros, final TimestampBufferMicros tb) {
        super.appendTimestampMicros(utcMicros, tb);
        return this;
    }

    @Override
    public LogEntry append(final Throwable t) {
        super.append(t);
        return this;
    }

    @Override
    public LogEntry append(final byte[] ba) {
        super.append(ba);
        return this;
    }

    @Override
    public LogEntry append(final byte[] ba, int pos, int length) {
        super.append(ba, pos, length);
        return this;
    }

    @Override
    public LogEntry append(final byte[] ba, byte terminator) {
        super.append(ba, terminator);
        return this;
    }

    @Override
    public LogEntry nf() {
        super.nf();
        return this;
    }

    @Override
    public LogEntry nl() {
        super.nl();
        return this;
    }
}

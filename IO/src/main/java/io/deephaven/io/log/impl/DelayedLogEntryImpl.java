/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log.impl;

import io.deephaven.base.array.*;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.text.TimestampBuffer;
import io.deephaven.base.text.TimestampBufferMicros;
import io.deephaven.base.verify.Assert;
import io.deephaven.io.log.*;
import io.deephaven.io.logger.LoggerTimeSource;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class DelayedLogEntryImpl implements LogEntry {

    private final LoggerTimeSource timeSource;

    private static final AtomicLong starts = new AtomicLong(0);
    private static final AtomicLong ends = new AtomicLong(0);

    @SuppressWarnings("unused")
    public static long getDifferenceInStartToEnds() {
        final long e = ends.get(); // getting this one first
        final long s = starts.get();
        return s - e; // should always be
    }

    @Override
    public LogOutput markEndOfHeader() {
        actions.add(Action.END_OF_HEADER);
        return this;
    }

    // This doesn't apply to this implementation, it's intended for when the buffer is being created
    @Override
    public int getEndOfHeaderOffset() {
        return 0;
    }

    private enum Action {
        APPEND_BOOLEAN, APPEND_CHAR, APPEND_SHORT, APPEND_INT, APPEND_LONG, APPEND_DOUBLE, APPEND_CHARSEQ, APPEND_CHARSEQ_RANGE, APPEND_TIMESTAMP, APPEND_NULL, APPEND_THROWABLE, NF, NL, APPEND_TIMESTAMP_MICROS, END_OF_HEADER
    }

    private final FastArray<Action> actions = new FastArray<>(Action.class, null, 8, false);
    private final FastBooleanArray booleans = new FastBooleanArray(8);
    private final FastCharArray chars = new FastCharArray(8);
    private final FastShortArray shorts = new FastShortArray(8);
    private final FastIntArray ints = new FastIntArray(8);
    private final FastLongArray longs = new FastLongArray(8);
    private final FastFloatArray floats = new FastFloatArray(8);
    private final FastDoubleArray doubles = new FastDoubleArray(8);
    private final FastArray<CharSequence> sequences = new FastArray<>(CharSequence.class, null, 8, false);
    private final FastArray<TimestampBuffer> timestamps = new FastArray<>(TimestampBuffer.class, null, 8, false);
    private final FastArray<TimestampBufferMicros> timestampsMicros =
            new FastArray<>(TimestampBufferMicros.class, null, 8, false);
    private final FastArray<Throwable> throwables = new FastArray<>(Throwable.class, null, 8, false);

    private void reset() {
        actions.quickReset();
        booleans.quickReset();
        chars.quickReset();
        shorts.quickReset();
        ints.quickReset();
        longs.quickReset();
        floats.quickReset();
        doubles.quickReset();
        sequences.normalReset();
        timestamps.normalReset();
        timestampsMicros.normalReset();
        throwables.normalReset();
    }

    // the timestamp of this entry (when start() was last called)
    private long timestamp;

    // the level of this entry (when start() was last called)
    private LogLevel level;

    // the throwable attached to this entry (when start() was last called)
    private Throwable throwable;

    // the sink where this entry will go, set on every call to start()
    private LogSink sink;

    @SuppressWarnings("WeakerAccess")
    public DelayedLogEntryImpl(LoggerTimeSource timeSource) {
        this.timeSource = timeSource;
        this.timestamp = timeSource.currentTimeMicros();
        this.sink = null;
    }

    /**
     * Notifies us that the log driver has given this instance out as a new entry
     */
    @Override
    public LogEntry start(final LogSink sink, final LogLevel level) {
        return start(sink, level, timeSource.currentTimeMicros());
    }

    @Override
    public LogEntry start(final LogSink sink, final LogLevel level, final long currentTimeMicros) {
        return start(sink, level, currentTimeMicros, null);
    }

    @Override
    public LogEntry start(final LogSink sink, final LogLevel level, final long currentTimeMicros, final Throwable t) {
        starts.getAndIncrement();
        this.timestamp = currentTimeMicros;
        this.level = level;
        this.throwable = t;
        this.sink = sink;
        return this;
    }

    @Override
    public LogEntry end() {
        // noinspection unchecked
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
        outputBuffer.start();
        pushToDelegate(outputBuffer);
        outputBuffer.close();
        return outputBuffer;
    }

    @Override
    public void written(LogOutput outputBuffer) {
        outputBuffer.clear();
        reset();
    }

    @Override
    public LogEntry append(final boolean b) {
        actions.add(Action.APPEND_BOOLEAN);
        booleans.add(b);
        return this;
    }

    @Override
    public LogEntry append(final char c) {
        actions.add(Action.APPEND_CHAR);
        chars.add(c);
        return this;
    }

    @Override
    public LogEntry append(final short s) {
        actions.add(Action.APPEND_SHORT);
        shorts.add(s);
        return this;
    }

    @Override
    public LogEntry append(final int i) {
        actions.add(Action.APPEND_INT);
        ints.add(i);
        return this;
    }

    @Override
    public LogEntry append(final long l) {
        actions.add(Action.APPEND_LONG);
        longs.add(l);
        return this;
    }

    @Override
    public LogEntry appendDouble(final double f) {
        actions.add(Action.APPEND_DOUBLE);
        doubles.add(f);
        return this;
    }

    @Override
    public LogEntry append(final LogOutputAppendable appendable) {
        if (appendable == null) {
            actions.add(Action.APPEND_NULL);
        } else {
            appendable.append(this);
        }
        return this;
    }

    @Override
    public LogEntry append(LongFormatter formatter, long n) {
        formatter.format(this, n);
        return this;
    }

    @Override
    public <T> LogEntry append(ObjFormatter<T> objFormatter, T t) {
        objFormatter.format(this, t);
        return this;
    }

    @Override
    public <T> LogEntry append(ObjIntIntFormatter<T> objFormatter, T t, int nOffset, int nLength) {
        objFormatter.format(this, t, nOffset, nLength);
        return this;
    }

    @Override
    public <T, U> LogEntry append(ObjObjFormatter<T, U> objObjFormatter, T t, U u) {
        objObjFormatter.format(this, t, u);
        return this;
    }

    @Override
    public LogEntry append(final CharSequence seq) {
        if (seq == null) {
            actions.add(Action.APPEND_NULL);
        } else {
            actions.add(Action.APPEND_CHARSEQ);
            sequences.add(seq);
        }
        return this;
    }

    @Override
    public LogEntry append(final CharSequence seq, final int start, final int length) {
        if (seq == null) {
            actions.add(Action.APPEND_NULL);
        } else {
            actions.add(Action.APPEND_CHARSEQ_RANGE);
            sequences.add(seq);
            ints.add(start);
            ints.add(length);
        }
        return this;
    }

    @Override
    public LogEntry append(final ByteBuffer bb) {
        if (bb == null) {
            actions.add(Action.APPEND_NULL);
        } else {
            final int pos = bb.position();
            final int limit = bb.limit();
            for (int i = pos; i < limit; ++i) {
                actions.add(Action.APPEND_CHAR);
                chars.add((char) bb.get(i));
            }
        }
        return this;
    }

    @Override
    public LogEntry appendTimestamp(final long utcMillis, final TimestampBuffer tb) {
        actions.add(Action.APPEND_TIMESTAMP);
        longs.add(utcMillis);
        timestamps.add(tb);
        return this;
    }

    @Override
    public LogEntry appendTimestampMicros(final long utcMicros, final TimestampBufferMicros tb) {
        actions.add(Action.APPEND_TIMESTAMP_MICROS);
        longs.add(utcMicros);
        timestampsMicros.add(tb);
        return this;
    }

    @Override
    public LogEntry append(final Throwable t) {
        actions.add(Action.APPEND_THROWABLE);
        throwables.add(t);
        return this;
    }

    @Override
    public LogEntry append(final byte[] ba) {
        for (byte baValue : ba) {
            actions.add(Action.APPEND_CHAR);
            chars.add((char) baValue);
        }
        return this;
    }

    @Override
    public LogEntry append(final byte[] ba, int pos, int length) {
        for (int i = pos; i < pos + length; ++i) {
            actions.add(Action.APPEND_CHAR);
            chars.add((char) ba[i]);
        }
        return this;
    }

    @Override
    public LogEntry append(final byte[] ba, byte terminator) {
        for (int i = 0; i < ba.length && ba[i] != terminator; ++i) {
            actions.add(Action.APPEND_CHAR);
            chars.add((char) ba[i]);
        }
        return this;
    }

    @Override
    public LogEntry nf() {
        actions.add(Action.NF);
        return this;
    }

    @Override
    public LogEntry nl() {
        actions.add(Action.NL);
        return this;
    }

    private void pushToDelegate(LogOutput logOutputBuffer) {
        int booleanPosition = 0;
        int charPosition = 0;
        int shortPosition = 0;
        int intPosition = 0;
        int longPosition = 0;
        int floatPosition = 0;
        int doublePosition = 0;
        int seqPosition = 0;
        int timePosition = 0;
        int timeMicrosPosition = 0;
        int throwablePosition = 0;

        final Action[] as = actions.getUnsafeArray();
        final boolean[] bs = booleans.getUnsafeArray();
        final char[] cs = chars.getUnsafeArray();
        final short[] ss = shorts.getUnsafeArray();
        final int[] is = ints.getUnsafeArray();
        final long[] ls = longs.getUnsafeArray();
        final float[] fs = floats.getUnsafeArray();
        final double[] ds = doubles.getUnsafeArray();
        final CharSequence[] seqs = sequences.getUnsafeArray();
        final TimestampBuffer[] times = timestamps.getUnsafeArray();
        final TimestampBufferMicros[] timesMicros = timestampsMicros.getUnsafeArray();
        final Throwable[] throwables = this.throwables.getUnsafeArray();

        final int numActions = actions.getLength();
        for (int actionPosition = 0; actionPosition < numActions; ++actionPosition) {
            switch (as[actionPosition]) {
                case APPEND_BOOLEAN:
                    logOutputBuffer = logOutputBuffer.append(bs[booleanPosition++]);
                    break;
                case APPEND_CHAR:
                    logOutputBuffer = logOutputBuffer.append(cs[charPosition++]);
                    break;
                case APPEND_SHORT:
                    logOutputBuffer = logOutputBuffer.append(ss[shortPosition++]);
                    break;
                case APPEND_INT:
                    logOutputBuffer = logOutputBuffer.append(is[intPosition++]);
                    break;
                case APPEND_LONG:
                    logOutputBuffer = logOutputBuffer.append(ls[longPosition++]);
                    break;
                case APPEND_DOUBLE:
                    logOutputBuffer = logOutputBuffer.appendDouble(ds[doublePosition++]);
                    break;
                case APPEND_CHARSEQ:
                    logOutputBuffer = logOutputBuffer.append(seqs[seqPosition++]);
                    break;
                case APPEND_CHARSEQ_RANGE:
                    logOutputBuffer = logOutputBuffer.append(seqs[seqPosition++], is[intPosition++], is[intPosition++]);
                    break;
                case APPEND_TIMESTAMP:
                    logOutputBuffer = logOutputBuffer.appendTimestamp(ls[longPosition++], times[timePosition++]);
                    break;
                case APPEND_TIMESTAMP_MICROS:
                    logOutputBuffer = logOutputBuffer.appendTimestampMicros(ls[longPosition++],
                            timesMicros[timeMicrosPosition++]);
                    break;
                case APPEND_NULL:
                    logOutputBuffer = logOutputBuffer.append((LogOutputAppendable) null);
                    break;
                case APPEND_THROWABLE:
                    logOutputBuffer = logOutputBuffer.append(throwables[throwablePosition++]);
                    break;
                case NF:
                    logOutputBuffer = logOutputBuffer.nf();
                    break;
                case NL:
                    logOutputBuffer = logOutputBuffer.nl();
                    break;
                case END_OF_HEADER:
                    logOutputBuffer.markEndOfHeader();
                    break;

                default:
                    throw Assert.statementNeverExecuted("Unexpected action: " + as[actionPosition]);

            }
        }

        Assert.eq(booleanPosition, "booleanPosition", booleans.getLength());
        Assert.eq(charPosition, "charPosition", chars.getLength());
        Assert.eq(shortPosition, "shortPosition", shorts.getLength());
        Assert.eq(intPosition, "intPosition", ints.getLength());
        Assert.eq(longPosition, "longPosition", longs.getLength());
        Assert.eq(floatPosition, "floatPosition", floats.getLength());
        Assert.eq(doublePosition, "doublePosition", doubles.getLength());
        Assert.eq(seqPosition, "seqPosition", sequences.getLength());
        Assert.eq(timePosition, "timePosition", timestamps.getLength());
        Assert.eq(timeMicrosPosition, "timePosition", timestampsMicros.getLength());
    }

    @Override
    public int relativeSize() {
        return actions.getLength();
    }

    @Override
    public LogOutput start() {
        // noinspection ConstantConditions
        throw Assert.statementNeverExecuted("Not implemented");
    }

    @Override
    public LogOutput close() {
        // noinspection ConstantConditions
        throw Assert.statementNeverExecuted("Not implemented");
    }

    @Override
    public int size() {
        // noinspection ConstantConditions
        throw Assert.statementNeverExecuted("Not implemented");
    }

    @Override
    public int getBufferCount() {
        // noinspection ConstantConditions
        throw Assert.statementNeverExecuted("Not implemented");
    }

    @Override
    public ByteBuffer getBuffer(final int i) {
        // noinspection ConstantConditions
        throw Assert.statementNeverExecuted("Not implemented");
    }

    @Override
    public LogEntry clear() {
        // noinspection ConstantConditions
        throw Assert.statementNeverExecuted("Not implemented");
    }
}

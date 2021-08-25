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

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

public class DelayedLogEntryUnsafeImpl implements LogEntry {

    private static final AtomicLong starts = new AtomicLong(0);
    private static final AtomicLong ends = new AtomicLong(0);

    @SuppressWarnings("unused")
    public static long getDifferenceInStartToEnds() {
        final long e = ends.get(); // getting this one first
        final long s = starts.get();
        return s - e; // should always be >= 0 ?
    }

    private static final int NUM_ACTIONS = Action.values().length;

    private enum Action {
        APPEND_BOOLEAN, APPEND_CHAR, APPEND_SHORT, APPEND_INT, APPEND_LONG, APPEND_DOUBLE, APPEND_CHARSEQ, APPEND_CHARSEQ_RANGE, APPEND_TIMESTAMP, APPEND_NULL, APPEND_THROWABLE, NF, NL, APPEND_CHAR_AS_BYTE, APPEND_CHARS_AS_BYTES, APPEND_BYTE, APPEND_TIMESTAMP_MICROS, END_OF_HEADER;

        public static Action getAction(byte ordinal) {
            switch (ordinal) {
                case 0:
                    return APPEND_BOOLEAN;
                case 1:
                    return APPEND_CHAR;
                case 2:
                    return APPEND_SHORT;
                case 3:
                    return APPEND_INT;
                case 5:
                    return APPEND_LONG;
                case 11:
                    return APPEND_DOUBLE;
                case 13:
                    return APPEND_CHARSEQ;
                case 14:
                    return APPEND_CHARSEQ_RANGE;
                case 15:
                    return APPEND_TIMESTAMP;
                case 16:
                    return APPEND_NULL;
                case 17:
                    return APPEND_THROWABLE;
                case 18:
                    return NF;
                case 19:
                    return NL;
                case 20:
                    return APPEND_CHAR_AS_BYTE;
                case 21:
                    return APPEND_CHARS_AS_BYTES;
                case 22:
                    return APPEND_BYTE;
                case 23:
                    return APPEND_TIMESTAMP_MICROS;
                case 24:
                    return END_OF_HEADER;
            }
            return null;
        }
    }

    private final PrimitiveReaderImpl reader = new PrimitiveReaderImpl(4096);
    private final FastArray<Object> immutables = new FastArray<>(Object.class, null, 1024, false);
    private int actionCount;

    private void reset() {
        reader.reset();
        immutables.normalReset();
        actionCount = 0;
    }

    @Override
    public LogOutput markEndOfHeader() {
        reader.put((byte) Action.END_OF_HEADER.ordinal());
        ++actionCount;
        return this;
    }

    // This doesn't apply to this implementation, it's intended for when the buffer is being created
    @Override
    public int getEndOfHeaderOffset() {
        return 0;
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
    public DelayedLogEntryUnsafeImpl() {
        this.timestamp = System.currentTimeMillis() * 1000;
        this.sink = null;
    }

    @SuppressWarnings("WeakerAccess")
    public void free() {
        reader.free();
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
        reader.put((byte) Action.APPEND_BOOLEAN.ordinal());
        reader.put(b);
        ++actionCount;
        return this;
    }

    @Override
    public LogEntry append(final char c) {
        if (c <= Byte.MAX_VALUE) {
            return appendAsChar((byte) c); // does the action count...
        } else {
            reader.put((byte) Action.APPEND_CHAR.ordinal());
            reader.put(c);
        }
        ++actionCount;
        return this;
    }


    // byte to char
    @SuppressWarnings("WeakerAccess")
    public LogEntry appendAsChar(final byte c) {
        if (c >= NUM_ACTIONS) {
            reader.put(c);
        } else {
            reader.put((byte) Action.APPEND_CHAR_AS_BYTE.ordinal());
            reader.put(c);
        }
        ++actionCount;
        return this;
    }

    // byte to decimal
    private LogEntry append(final byte b) {
        reader.put((byte) Action.APPEND_BYTE.ordinal());
        reader.put(b);
        ++actionCount;
        return this;
    }

    @Override
    public LogEntry append(final short s) {
        if (s <= Byte.MAX_VALUE && s >= Byte.MIN_VALUE) {
            return append((byte) s); // does the action count
        }

        reader.put((byte) Action.APPEND_SHORT.ordinal());
        reader.put(s);
        ++actionCount;
        return this;
    }

    @Override
    public LogEntry append(final int i) {
        if (i <= Short.MAX_VALUE && i >= Short.MIN_VALUE) {
            return append((short) i);
        }

        reader.put((byte) Action.APPEND_INT.ordinal());
        reader.put(i);
        ++actionCount;
        return this;
    }

    @Override
    public LogEntry append(final long l) {
        if (l <= Integer.MAX_VALUE && l >= Integer.MIN_VALUE) {
            return append((int) l);
        }
        reader.put((byte) Action.APPEND_LONG.ordinal());
        reader.put(l);
        ++actionCount;
        return this;
    }

    @Override
    public LogEntry appendDouble(final double f) {
        reader.put((byte) Action.APPEND_DOUBLE.ordinal());
        reader.put(f);
        ++actionCount;
        return this;
    }

    @Override
    public LogEntry append(final LogOutputAppendable appendable) {
        if (appendable == null) {
            reader.put((byte) Action.APPEND_NULL.ordinal());
            ++actionCount;
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
            reader.put((byte) Action.APPEND_NULL.ordinal());
        } else {
            reader.put((byte) Action.APPEND_CHARSEQ.ordinal());
            immutables.add(seq);
        }
        ++actionCount;
        return this;
    }

    @Override
    public LogEntry append(final CharSequence seq, final int start, final int length) {
        if (seq == null) {
            reader.put((byte) Action.APPEND_NULL.ordinal());
        } else {
            reader.put((byte) Action.APPEND_CHARSEQ_RANGE.ordinal());
            immutables.add(seq);
            reader.put(start);
            reader.put(length);
        }
        ++actionCount;
        return this;
    }

    @Override
    public LogEntry append(final ByteBuffer bb) {
        if (bb == null) {
            reader.put((byte) Action.APPEND_NULL.ordinal());
            ++actionCount;
        } else {

            final int pos = bb.position();
            final int limit = bb.limit();
            final int remaining = limit - pos;

            Assert.leq(remaining, "remaining", Short.MAX_VALUE, "Short.MAX_VALUE");

            // final int encodedWay = 3 + 1 * remaining;
            // final int simpleWay = 2 * remaining;
            if (remaining < 3) {
                for (int i = pos; i < limit; ++i) {
                    appendAsChar(bb.get(i)); // does the action count
                }
            } else {
                reader.put((byte) Action.APPEND_CHARS_AS_BYTES.ordinal());
                reader.put((short) remaining);
                for (int i = pos; i < limit; ++i) {
                    reader.put(bb.get(i));
                }
                ++actionCount;
            }

        }
        return this;
    }

    @Override
    public LogEntry appendTimestamp(final long utcMillis, final TimestampBuffer tb) {
        reader.put((byte) Action.APPEND_TIMESTAMP.ordinal());
        reader.put(utcMillis);
        immutables.add(tb);
        ++actionCount;
        return this;
    }

    @Override
    public LogEntry appendTimestampMicros(final long utcMicros, final TimestampBufferMicros tb) {
        reader.put((byte) Action.APPEND_TIMESTAMP_MICROS.ordinal());
        reader.put(utcMicros);
        immutables.add(tb);
        ++actionCount;
        return this;
    }

    @Override
    public LogEntry append(final Throwable t) {
        reader.put((byte) Action.APPEND_THROWABLE.ordinal());
        immutables.add(t);
        ++actionCount;
        return this;
    }

    @Override
    public LogEntry append(final byte[] ba) {

        Assert.leq(ba.length, "ab.length", Short.MAX_VALUE, "Short.MAX_VALUE");

        // final int encodedWay = 3 + 1 * ba.length;
        // final int simpleWay = 2 * ba.length;

        if (ba.length < 3) {
            for (byte baValue : ba) {
                appendAsChar(baValue); // does the action count
            }
        } else {
            reader.put((byte) Action.APPEND_CHARS_AS_BYTES.ordinal());
            reader.put((short) ba.length);
            for (byte baValue : ba) {
                reader.put(baValue);
            }
            ++actionCount;
        }
        return this;
    }

    @Override
    public LogEntry append(final byte[] ba, int pos, int length) {
        Assert.leq(length, "length", Short.MAX_VALUE, "Short.MAX_VALUE");

        // final int encodedWay = 3 + 1 * ba.length;
        // final int simpleWay = 2 * ba.length;

        if (length < 3) {
            for (int i = pos; i < pos + length; ++i) {
                appendAsChar(ba[i]); // does the action count
            }
        } else {
            reader.put((byte) Action.APPEND_CHARS_AS_BYTES.ordinal());
            reader.put((short) length);
            for (int i = pos; i < pos + length; ++i) {
                reader.put(ba[i]);
            }
            ++actionCount;
        }

        return this;
    }

    @Override
    public LogEntry append(final byte[] ba, byte terminator) {
        for (int i = 0; i < ba.length && ba[i] != terminator; ++i) {
            appendAsChar(ba[i]); // handles action count
        }
        return this;
    }

    @Override
    public LogEntry nf() {
        reader.put((byte) Action.NF.ordinal());
        ++actionCount;
        return this;
    }

    @Override
    public LogEntry nl() {
        reader.put((byte) Action.NL.ordinal());
        ++actionCount;
        return this;
    }

    private void pushToDelegate(LogOutput logOutputBuffer) {

        reader.doneAdding();

        final Object[] im = immutables.getUnsafeArray();
        int immutablePosition = 0;

        for (int i = 0; i < actionCount; ++i) {

            Assert.gtZero(reader.remaining(), "reader.remaining");

            final byte ordinal = reader.nextByte();
            final Action action = Action.getAction(ordinal);
            if (action == null) {
                // assume char as direct byte!
                logOutputBuffer = logOutputBuffer.append((char) ordinal);
                continue;
            }

            switch (action) {
                case APPEND_BOOLEAN:
                    logOutputBuffer = logOutputBuffer.append(reader.nextBoolean());
                    break;
                case APPEND_CHAR:
                    logOutputBuffer = logOutputBuffer.append(reader.nextChar());
                    break;
                case APPEND_SHORT:
                    logOutputBuffer = logOutputBuffer.append(reader.nextShort());
                    break;
                case APPEND_INT:
                    logOutputBuffer = logOutputBuffer.append(reader.nextInt());
                    break;
                case APPEND_LONG:
                    logOutputBuffer = logOutputBuffer.append(reader.nextLong());
                    break;
                case APPEND_DOUBLE:
                    logOutputBuffer = logOutputBuffer.appendDouble(reader.nextDouble());
                    break;
                case APPEND_CHARSEQ:
                    logOutputBuffer = logOutputBuffer.append((CharSequence) im[immutablePosition++]);
                    break;
                case APPEND_CHARSEQ_RANGE:
                    logOutputBuffer = logOutputBuffer.append((CharSequence) im[immutablePosition++], reader.nextInt(),
                            reader.nextInt());
                    break;
                case APPEND_TIMESTAMP:
                    logOutputBuffer = logOutputBuffer.appendTimestamp(reader.nextLong(),
                            (TimestampBuffer) im[immutablePosition++]);
                    break;
                case APPEND_TIMESTAMP_MICROS:
                    logOutputBuffer = logOutputBuffer.appendTimestampMicros(reader.nextLong(),
                            (TimestampBufferMicros) im[immutablePosition++]);
                    break;
                case APPEND_NULL:
                    logOutputBuffer = logOutputBuffer.append((LogOutputAppendable) null);
                    break;
                case APPEND_THROWABLE:
                    logOutputBuffer = logOutputBuffer.append((Throwable) im[immutablePosition++]);
                    break;
                case NF:
                    logOutputBuffer = logOutputBuffer.nf();
                    break;
                case NL:
                    logOutputBuffer = logOutputBuffer.nl();
                    break;
                case APPEND_CHAR_AS_BYTE:
                    logOutputBuffer = logOutputBuffer.append((char) reader.nextByte());
                    break;
                case APPEND_CHARS_AS_BYTES:
                    final short len = reader.nextShort();
                    for (int j = 0; j < len; ++j) {
                        logOutputBuffer = logOutputBuffer.append((char) reader.nextByte());
                    }
                    break;
                case APPEND_BYTE:
                    logOutputBuffer = logOutputBuffer.append((short) reader.nextByte()); // cast to short so it gets
                                                                                         // printed as decimal
                    break;
                case END_OF_HEADER:
                    logOutputBuffer.markEndOfHeader();
                    break;
                default:
                    throw Assert.statementNeverExecuted("Unexpected action ordinal: " + ordinal);
            }
        }
        Assert.eqZero(reader.remaining(), "reader.remaining()");
    }

    @Override
    public int relativeSize() {
        return actionCount;
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

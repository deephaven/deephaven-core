/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log.impl;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.pool.Pool;
import io.deephaven.base.text.TimestampBuffer;
import io.deephaven.base.text.TimestampBufferMicros;
import io.deephaven.base.verify.Assert;
import io.deephaven.io.log.*;
import io.deephaven.io.streams.ByteBufferStreams;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;

@SuppressWarnings("unused")
public class DelayedLogEntryImpl2 implements LogEntry {

    private static final byte APPEND_BOOLEAN = 0;
    private static final byte APPEND_CHAR = 1;
    private static final byte APPEND_SHORT = 2;
    private static final byte APPEND_INT = 3;
    private static final byte APPEND_LONG = 5;
    private static final byte APPEND_DOUBLE = 11;
    private static final byte APPEND_CHARSEQ = 13;
    private static final byte APPEND_CHARSEQ_RANGE = 14;
    private static final byte APPEND_BYTES = 15;
    private static final byte APPEND_TIMESTAMP = 16;
    private static final byte APPEND_NULL = 17;
    private static final byte APPEND_THROWABLE = 18;
    private static final byte NF = 19;
    private static final byte NL = 20;
    private static final byte APPEND_TIMESTAMP_MICROS = 21;
    private static final byte END_OF_HEADER = 22;

    private final Pool<ByteBuffer> bufferPool;
    private ByteBuffer[] buffers = new ByteBuffer[4];
    private int bufferPtr = 0;

    @Override
    public LogOutput markEndOfHeader() {
        try {
            primitiveWriter.writeByte(END_OF_HEADER);
            numActions++;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    // This doesn't apply for this implementation, it's intended for when the buffer is being
    // created
    @Override
    public int getEndOfHeaderOffset() {
        return 0;
    }

    private final ByteBufferStreams.Sink SINK = new ByteBufferStreams.Sink() {
        @Override
        public ByteBuffer acceptBuffer(ByteBuffer b, int need) throws IOException {
            b.flip();
            buffers = ArrayUtil.put(buffers, bufferPtr++, b, ByteBuffer.class);
            return bufferPool.take();
        }

        @Override
        public void close(ByteBuffer b) throws IOException {
            buffers = ArrayUtil.put(buffers, bufferPtr++, b, ByteBuffer.class);
        }
    };

    private final ByteBufferStreams.Source SOURCE = new ByteBufferStreams.Source() {
        @Override
        public ByteBuffer nextBuffer(ByteBuffer lastBuffer) {
            if (lastBuffer != null) {
                bufferPool.give(lastBuffer);
            }
            ByteBuffer b = null;
            if (bufferPtr < buffers.length && (b = buffers[bufferPtr++]) != null) {
                buffers[bufferPtr - 1] = null;
            }
            return b;
        }
    };

    private final ByteBufferStreams.Output primitiveWriter =
        new ByteBufferStreams.Output(null, SINK);
    private Object[] objects = null;
    private int objectsPtr = 0;
    private int numActions = 0;

    private final ByteBufferStreams.Input primitiveReader =
        new ByteBufferStreams.Input(null, SOURCE);

    private void reset() {
        primitiveWriter.setBuffer(bufferPool.take());
        bufferPtr = 0;
        Arrays.fill(objects, 0, objectsPtr, null);
        objectsPtr = 0;
        numActions = 0;
    }

    // the timestamp of this entry (when start() was last called)
    private long timestamp;

    // the level of this entry (when start() was last called)
    private LogLevel level;

    // the throwable attached to this entry (when start() was last called)
    private Throwable throwable;

    // the sink where this entry will go, set on every call to start()
    private LogSink logSink;

    public DelayedLogEntryImpl2(Pool<ByteBuffer> bufferPool) {
        this.bufferPool = bufferPool;
        this.timestamp = System.currentTimeMillis() * 1000;
        this.logSink = null;
    }

    /**
     * Notifies us that the log driver has given this instance out as a new entry
     */
    @Override
    public LogEntry start(final LogSink logSink, final LogLevel level) {
        return start(logSink, level, System.currentTimeMillis());
    }

    @Override
    public LogEntry start(final LogSink logSink, final LogLevel level,
        final long currentTimeMicros) {
        return start(logSink, level, currentTimeMicros, null);
    }

    @Override
    public LogEntry start(final LogSink logSink, final LogLevel level, final long currentTimeMicros,
        final Throwable t) {
        this.timestamp = currentTimeMicros;
        this.level = level;
        this.throwable = t;
        this.logSink = logSink;
        return this;
    }

    @Override
    public LogEntry end() {
        // noinspection unchecked
        logSink.write(this);
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
        try {
            primitiveWriter.writeByte(APPEND_BOOLEAN);
            primitiveWriter.writeBoolean(b);
            numActions++;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogEntry append(final char c) {
        try {
            primitiveWriter.writeByte(APPEND_CHAR);
            primitiveWriter.writeChar(c);
            numActions++;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogEntry append(final short s) {
        try {
            primitiveWriter.writeByte(APPEND_SHORT);
            primitiveWriter.writeChar(s);
            numActions++;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogEntry append(final int i) {
        try {
            primitiveWriter.writeByte(APPEND_INT);
            primitiveWriter.writeInt(i);
            numActions++;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogEntry append(final long l) {
        try {
            primitiveWriter.writeByte(APPEND_LONG);
            primitiveWriter.writeLong(l);
            numActions++;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogEntry appendDouble(final double f) {
        try {
            primitiveWriter.writeByte(APPEND_DOUBLE);
            primitiveWriter.writeDouble(f);
            numActions++;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogEntry append(final LogOutputAppendable appendable) {
        if (appendable == null) {
            try {
                primitiveWriter.writeByte(APPEND_NULL);
                numActions++;
            } catch (IOException x) {
                throw new UncheckedIOException(x);
            }
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
            try {
                primitiveWriter.writeByte(APPEND_NULL);
                numActions++;
            } catch (IOException x) {
                throw new UncheckedIOException(x);
            }
        } else {
            try {
                primitiveWriter.writeByte(APPEND_CHARSEQ);
                objects = ArrayUtil.put(objects, objectsPtr++, seq, Object.class);
                numActions++;
            } catch (IOException x) {
                throw new UncheckedIOException(x);
            }
        }
        return this;
    }

    @Override
    public LogEntry append(final CharSequence seq, final int start, final int length) {
        if (seq == null) {
            try {
                primitiveWriter.writeByte(APPEND_NULL);
                numActions++;
            } catch (IOException x) {
                throw new UncheckedIOException(x);
            }
        } else {
            try {
                primitiveWriter.writeByte(APPEND_CHARSEQ);
                objects = ArrayUtil.put(objects, objectsPtr++, seq, Object.class);
                primitiveWriter.writeInt(start);
                primitiveWriter.writeInt(length);
                numActions++;
            } catch (IOException x) {
                throw new UncheckedIOException(x);
            }
        }
        return this;
    }

    @Override
    public LogEntry append(final ByteBuffer bb) {
        if (bb == null) {
            try {
                primitiveWriter.writeByte(APPEND_NULL);
            } catch (IOException x) {
                throw new UncheckedIOException(x);
            }
        } else {
            try {
                int pos = bb.position();
                primitiveWriter.writeByte(APPEND_BYTES);
                primitiveWriter.writeInt(bb.remaining());
                primitiveWriter.write(bb);
                bb.position(pos);
                numActions++;
            } catch (IOException x) {
                throw new UncheckedIOException(x);
            }
        }
        return this;
    }

    @Override
    public LogEntry appendTimestamp(final long utcMillis, final TimestampBuffer tb) {
        try {
            primitiveWriter.writeByte(APPEND_TIMESTAMP);
            primitiveWriter.writeLong(utcMillis);
            objects = ArrayUtil.put(objects, objectsPtr++, tb, Object.class);
            numActions++;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogEntry appendTimestampMicros(final long utcMicros, final TimestampBufferMicros tb) {
        try {
            primitiveWriter.writeByte(APPEND_TIMESTAMP_MICROS);
            primitiveWriter.writeLong(utcMicros);
            objects = ArrayUtil.put(objects, objectsPtr++, tb, Object.class);
            numActions++;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogEntry append(final Throwable t) {
        try {
            primitiveWriter.writeByte(APPEND_THROWABLE);
            objects = ArrayUtil.put(objects, objectsPtr++, t, Object.class);
            numActions++;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogEntry append(final byte[] ba) {
        try {
            primitiveWriter.writeByte(APPEND_BYTES);
            primitiveWriter.writeInt(ba.length);
            primitiveWriter.write(ba, 0, ba.length);
            numActions++;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogEntry append(final byte[] ba, int pos, int length) {
        try {
            primitiveWriter.writeByte(APPEND_BYTES);
            primitiveWriter.writeInt(length);
            primitiveWriter.write(ba, pos, length);
            numActions++;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogEntry append(final byte[] ba, byte terminator) {
        int n = 0;
        while (n < ba.length && ba[n] != terminator) {
            ++n;
        }
        return append(ba, 0, n);
    }

    @Override
    public LogEntry nf() {
        try {
            primitiveWriter.writeByte(NF);
            numActions++;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogEntry nl() {
        try {
            primitiveWriter.writeByte(NL);
            numActions++;
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    private void pushToDelegate(LogOutput logOutputBuffer) {
        try {
            primitiveWriter.close();
            primitiveReader.setBuffer(buffers[0]);
            bufferPtr = 1;
            objectsPtr = 0;
            int action;
            while ((action = primitiveReader.read()) != -1) {
                switch (action) {
                    case APPEND_BOOLEAN: {
                        logOutputBuffer = logOutputBuffer.append(primitiveReader.readBoolean());
                        break;
                    }
                    case APPEND_CHAR: {
                        logOutputBuffer = logOutputBuffer.append(primitiveReader.readChar());
                        break;
                    }
                    case APPEND_SHORT: {
                        logOutputBuffer = logOutputBuffer.append(primitiveReader.readShort());
                        break;
                    }
                    case APPEND_INT: {
                        logOutputBuffer = logOutputBuffer.append(primitiveReader.readInt());
                        break;
                    }
                    case APPEND_LONG: {
                        logOutputBuffer = logOutputBuffer.append(primitiveReader.readLong());
                        break;
                    }
                    case APPEND_DOUBLE: {
                        logOutputBuffer =
                            logOutputBuffer.appendDouble(primitiveReader.readDouble());
                        break;
                    }
                    case APPEND_CHARSEQ: {
                        logOutputBuffer =
                            logOutputBuffer.append((CharSequence) objects[objectsPtr++]);
                        break;
                    }
                    case APPEND_CHARSEQ_RANGE: {
                        CharSequence seq = (CharSequence) objects[objectsPtr++];
                        int pos = primitiveReader.readInt();
                        int length = primitiveReader.readInt();
                        logOutputBuffer = logOutputBuffer.append(seq, pos, length);
                        break;
                    }
                    case APPEND_BYTES: {
                        int n = primitiveReader.readInt();
                        for (int i = 0; i < n; ++i) {
                            logOutputBuffer = logOutputBuffer.append((char) primitiveReader.read());
                        }
                        break;
                    }
                    case APPEND_TIMESTAMP: {
                        TimestampBuffer tb = (TimestampBuffer) objects[objectsPtr++];
                        long utcMillis = primitiveReader.readLong();
                        logOutputBuffer = logOutputBuffer.appendTimestamp(utcMillis, tb);
                        break;
                    }
                    case APPEND_TIMESTAMP_MICROS: {
                        TimestampBufferMicros tb = (TimestampBufferMicros) objects[objectsPtr++];
                        long utcMicros = primitiveReader.readLong();
                        logOutputBuffer = logOutputBuffer.appendTimestampMicros(utcMicros, tb);
                        break;
                    }
                    case APPEND_NULL: {
                        logOutputBuffer = logOutputBuffer.append((LogOutputAppendable) null);
                        break;
                    }
                    case APPEND_THROWABLE: {
                        logOutputBuffer = logOutputBuffer.append((Throwable) objects[objectsPtr++]);
                        break;
                    }
                    case NF: {
                        logOutputBuffer = logOutputBuffer.nf();
                        break;
                    }
                    case NL: {
                        logOutputBuffer = logOutputBuffer.nl();
                        break;
                    }
                    case END_OF_HEADER: {
                        logOutputBuffer.markEndOfHeader();
                        break;
                    }
                    default:
                        // noinspection ConstantConditions
                        throw Assert.statementNeverExecuted("Unexpected action: " + action);
                }
            }
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
    }

    @Override
    public int relativeSize() {
        return numActions;
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

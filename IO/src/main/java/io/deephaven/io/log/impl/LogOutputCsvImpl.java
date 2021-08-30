/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log.impl;

import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;
import io.deephaven.base.text.TimestampBuffer;
import io.deephaven.base.text.TimestampBufferMicros;
import io.deephaven.io.log.*;
import io.deephaven.io.streams.ByteBufferSink;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;

public class LogOutputCsvImpl extends LogOutputBaseImpl implements LogOutput, ByteBufferSink {
    // the line separator
    protected final String lineSeparator = System.getProperty("line.separator");

    /**
     * Constructor
     * 
     * @param bufferPool where we get our buffers
     */
    public LogOutputCsvImpl(LogBufferPool bufferPool) {
        super(bufferPool);
    }

    // ------------------------------------------------------------------------------------------------
    // LogOutput implementation - append methods
    // ------------------------------------------------------------------------------------------------

    @Override
    public LogOutput append(boolean b) {
        if (b) {
            return append("true");
        } else {
            return append("false");
        }
    }

    @Override
    public LogOutput append(char c) {
        try {
            stream.appendByte((byte) c);
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogOutput append(short s) {
        try {
            stream.appendShort(s);
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogOutput append(int i) {
        try {
            stream.appendInt(i);
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogOutput append(long l) {
        try {
            stream.appendLong(l);
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogOutput appendDouble(double f) {
        try {
            stream.appendDouble(f);
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogOutput append(LogOutputAppendable appendable) {
        return appendable == null ? append("null") : appendable.append(this);
    }

    @Override
    public LogOutput append(LongFormatter formatter, long n) {
        formatter.format(this, n);
        return this;
    }

    @Override
    public <T> LogOutput append(ObjFormatter<T> objFormatter, T t) {
        objFormatter.format(this, t);
        return this;
    }

    @Override
    public <T> LogOutput append(ObjIntIntFormatter<T> objFormatter, T t, int nOffset, int nLength) {
        objFormatter.format(this, t, nOffset, nLength);
        return this;
    }

    @Override
    public <T, U> LogOutput append(ObjObjFormatter<T, U> objObjFormatter, T t, U u) {
        objObjFormatter.format(this, t, u);
        return this;
    }

    @Override
    public LogOutput append(CharSequence seq) {
        try {
            stream.appendBytes(seq == null ? "null" : seq);
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogOutput append(final CharSequence seq, final int start, final int length) {
        try {
            if (seq == null)
                stream.appendBytes("null");
            else
                stream.appendBytes(seq, start, length);
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogOutput append(final ByteBuffer bb) {
        try {
            if (bb == null)
                stream.appendBytes("null");
            else
                stream.appendByteBuffer(bb);
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogOutput appendTimestamp(long utcMillis, TimestampBuffer tb) {
        try {
            stream.write(tb.getTimestamp(utcMillis));
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogOutput appendTimestampMicros(long utcMicros, TimestampBufferMicros tb) {
        try {
            stream.write(tb.getTimestamp(utcMicros));
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogOutput append(Throwable t) {
        boolean root = true;
        do {
            if (!root) {
                append("caused by:").nl();
            } else {
                root = false;
            }
            append(t.getClass().getName()).append(": ").append(t.getMessage());
            for (StackTraceElement e : t.getStackTrace()) {
                nl().append("        at ")
                        .append(e.getClassName()).append(".").append(e.getMethodName())
                        .append("(").append(e.getFileName()).append(":").append(e.getLineNumber()).append(")");
            }
            nl();
        } while ((t = t.getCause()) != null);
        return this;
    }

    @Override
    public LogOutput append(final byte[] ba) {
        try {
            stream.write(ba);
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogOutput append(final byte[] ba, int pos, int length) {
        try {
            stream.write(ba, pos, length);
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogOutput append(final byte[] ba, byte terminator) {
        try {
            for (int i = 0; i < ba.length && ba[i] != terminator; ++i) {
                stream.appendByte(ba[i]);
            }
        } catch (IOException x) {
            throw new UncheckedIOException(x);
        }
        return this;
    }

    @Override
    public LogOutput nf() {
        append(',');
        return this;
    }

    @Override
    public LogOutput nl() {
        append(lineSeparator);
        return this;
    }
}

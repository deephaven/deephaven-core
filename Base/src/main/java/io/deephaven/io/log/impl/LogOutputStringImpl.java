/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log.impl;

import io.deephaven.base.text.Convert;
import io.deephaven.base.text.TimestampBuffer;
import io.deephaven.base.text.TimestampBufferMicros;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.log.LogOutputAppendable;

import java.nio.ByteBuffer;

/**
 * A LogOutput implementation, designed solely as an adapter for LogOutputAppendable's to produce
 * Strings.
 */
public class LogOutputStringImpl implements LogOutput, CharSequence {

    /**
     * The line separator.
     */
    protected static final String LINE_SEPARATOR = System.getProperty("line.separator");

    /**
     * The StringBuilder used to hold output.
     */
    protected final StringBuilder builder;

    public LogOutputStringImpl() {
        this.builder = new StringBuilder();
    }

    public LogOutputStringImpl(StringBuilder builder) {
        this.builder = builder;
    }

    @Override
    public int length() {
        return builder.length();
    }

    @Override
    public char charAt(int index) {
        return builder.charAt(index);
    }

    @Override
    public CharSequence subSequence(int start, int end) {
        return builder.subSequence(start, end);
    }

    // Header offsets aren't implemented in this LogOutput implementation
    @Override
    public LogOutput markEndOfHeader() {
        return this;
    }

    @Override
    public int getEndOfHeaderOffset() {
        return 0;
    }

    @Override
    public String toString() {
        return builder.toString();
    }

    // ------------------------------------------------------------------------------------------------
    // LogOutput implementation
    // ------------------------------------------------------------------------------------------------

    @Override
    public LogOutput start() {
        throw new UnsupportedOperationException();
    }

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
        builder.append(c);
        return this;
    }

    @Override
    public LogOutput append(short s) {
        builder.append(s);
        return this;
    }

    @Override
    public LogOutput append(int i) {
        builder.append(i);
        return this;
    }

    @Override
    public LogOutput append(long l) {
        builder.append(l);
        return this;
    }

    @Override
    public LogOutput appendDouble(double d) {
        builder.append(d);
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
        builder.append(seq);
        return this;
    }

    @Override
    public LogOutput append(final CharSequence seq, final int start, final int length) {
        builder.append(seq, start, length);
        return this;
    }

    @Override
    public LogOutput append(final ByteBuffer bb) {
        if (bb == null)
            return append("null");
        final int pos = bb.position();
        final int limit = bb.limit();
        for (int i = pos; i < limit; ++i) {
            builder.append((char) bb.get(i));
        }
        return this;
    }

    @Override
    public LogOutput appendTimestamp(long utcMillis, TimestampBuffer tb) {
        ByteBuffer byteBuffer = tb.getTimestamp(utcMillis);
        for (int bi = byteBuffer.position(); bi < byteBuffer.limit(); ++bi) {
            builder.append((char) byteBuffer.get(bi));
        }
        return this;
    }

    @Override
    public LogOutput appendTimestampMicros(long utcMicros, TimestampBufferMicros tb) {
        ByteBuffer byteBuffer = tb.getTimestamp(utcMicros);
        for (int bi = byteBuffer.position(); bi < byteBuffer.limit(); ++bi) {
            builder.append((char) byteBuffer.get(bi));
        }
        return this;
    }

    @Override
    public LogOutput append(Throwable t) {
        boolean root = true;
        String delim = "[";
        do {
            if (!root) {
                append("; caused by");
            } else {
                root = false;
            }
            append(delim);
            append(t.getClass().getName()).append(": ").append(t.getMessage());
            for (StackTraceElement e : t.getStackTrace()) {
                append(delim)
                    .append(e.getClassName()).append(".").append(e.getMethodName())
                    .append("(").append(e.getFileName()).append(":").append(e.getLineNumber())
                    .append(")");
                delim = ";";
            }
        } while ((t = t.getCause()) != null);
        append("]");
        return this;
    }

    @Override
    public LogOutput append(final byte[] ba) {
        for (int i = 0; i < ba.length; ++i) {
            builder.append((char) ba[i]);
        }
        return this;
    }

    @Override
    public LogOutput append(final byte[] ba, int pos, int length) {
        for (int i = pos; i < pos + length; ++i) {
            builder.append((char) ba[i]);
        }
        return this;
    }

    @Override
    public LogOutput append(final byte[] ba, byte terminator) {
        for (int i = 0; i < ba.length && ba[i] != terminator; ++i) {
            builder.append((char) ba[i]);
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
        append(LINE_SEPARATOR);
        return this;
    }

    @Override
    public LogOutput close() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int relativeSize() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int size() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getBufferCount() {
        throw new UnsupportedOperationException();
    }

    @Override
    public ByteBuffer getBuffer(int i) {
        throw new UnsupportedOperationException();
    }

    @Override
    public LogOutput clear() {
        builder.setLength(0);
        return this;
    }
}

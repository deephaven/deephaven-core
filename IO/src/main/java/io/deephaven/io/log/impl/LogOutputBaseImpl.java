//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.io.log.impl;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.log.LogOutput;
import io.deephaven.base.verify.Assert;
import io.deephaven.io.log.LogBufferPool;
import io.deephaven.io.streams.ByteBufferOutputStream;
import io.deephaven.io.streams.ByteBufferSink;
import org.jetbrains.annotations.VisibleForTesting;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;

public abstract class LogOutputBaseImpl implements LogOutput, ByteBufferSink {
    @VisibleForTesting
    static final int START_SIZE_BYTES = 128;

    // the stream in which we produce our output
    protected final ByteBufferOutputStream stream;

    // Marker for the end of the header position
    private int endOfHeaderPosition;

    // where the output buffers come from
    private final LogBufferPool bufferPool;
    private ByteBuffer[] buffers;
    private int bufferCount;

    @SuppressWarnings("WeakerAccess")
    public LogOutputBaseImpl(LogBufferPool bufferPool) {
        this.stream = new ByteBufferOutputStream(null, this);
        this.bufferPool = bufferPool;
        this.bufferCount = 0;
        this.buffers = new ByteBuffer[2];
        this.endOfHeaderPosition = 0;
    }

    @Override
    public LogOutput markEndOfHeader() {
        endOfHeaderPosition = stream.getBuffer().position();
        return this;
    }

    @Override
    public int getEndOfHeaderOffset() {
        return endOfHeaderPosition;
    }

    private ByteBuffer nextBuffer(int need) {
        ByteBuffer byteBuffer = Objects.requireNonNull(bufferPool.take(need));
        buffers = ArrayUtil.put(buffers, bufferCount, byteBuffer, ByteBuffer.class);
        bufferCount++;
        stream.setBuffer(byteBuffer);
        return byteBuffer;
    }

    @Override // from ByteBufferSink
    public ByteBuffer acceptBuffer(ByteBuffer b, int need) {
        assert b == buffers[bufferCount - 1];
        return nextBuffer(need);
    }

    @Override // from ByteBufferSink
    public void close(ByteBuffer b) throws IOException {
        // noinspection ConstantConditions
        throw Assert.statementNeverExecuted();
    }

    @Override // from LogOutput
    public LogOutput start() {
        clear();
        nextBuffer(START_SIZE_BYTES);
        return this;
    }

    @Override // from LogOutput
    public LogOutput close() {
        return this;
    }

    @Override // from LogOutput
    public int relativeSize() {
        return size();
    }

    @Override // from LogOutput
    public int size() {
        int byteCount = 0;
        for (int i = 0; i < bufferCount; ++i) {
            byteCount += buffers[i].position();
        }
        return byteCount;
    }

    @Override // from LogOutput
    public int getBufferCount() {
        return bufferCount;
    }

    @Override // from LogOutput
    public ByteBuffer getBuffer(int i) {
        return buffers[i];
    }

    @Override // from LogOutput
    public LogOutput clear() {
        for (int i = 0; i < bufferCount; ++i) {
            bufferPool.give(buffers[i]);
            buffers[i] = null;
        }
        bufferCount = 0;
        stream.setBuffer(null);
        return this;
    }
}

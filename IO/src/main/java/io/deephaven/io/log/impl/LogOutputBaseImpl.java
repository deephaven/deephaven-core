/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.io.log.impl;

import io.deephaven.base.ArrayUtil;
import io.deephaven.base.verify.Assert;
import io.deephaven.io.log.LogBufferPool;
import io.deephaven.base.log.LogOutput;
import io.deephaven.io.streams.ByteBufferOutputStream;
import io.deephaven.io.streams.ByteBufferSink;

import java.io.IOException;
import java.nio.ByteBuffer;

public abstract class LogOutputBaseImpl implements LogOutput, ByteBufferSink {
    // the stream in which we produce our output
    protected final ByteBufferOutputStream stream;

    // Marker for the end of the header position
    private int endOfHeaderPosition;

    // where the output buffers come from
    @SuppressWarnings("WeakerAccess")
    protected final LogBufferPool bufferPool;

    // the buffers in this entry
    protected ByteBuffer[] buffers;
    @SuppressWarnings("WeakerAccess")
    protected int bufferCount;

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

    private ByteBuffer nextBuffer() {
        ByteBuffer byteBuffer = bufferPool.take();
        buffers = ArrayUtil.put(buffers, bufferCount, byteBuffer, ByteBuffer.class);
        bufferCount++;
        stream.setBuffer(byteBuffer);
        return byteBuffer;
    }

    @Override // from ByteBufferSink
    public ByteBuffer acceptBuffer(ByteBuffer b, int need) throws IOException {
        return nextBuffer();
    }

    @Override // from ByteBufferSink
    public void close(ByteBuffer b) throws IOException {
        // noinspection ConstantConditions
        throw Assert.statementNeverExecuted();
    }

    @Override // from LogOutput
    public LogOutput start() {
        clear();
        nextBuffer();
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
        for (int nIndex = 0, nLength = bufferCount; nIndex < nLength; nIndex++) {
            byteCount += buffers[nIndex].position();
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
        for (int nIndex = 0, nLength = bufferCount; nIndex < nLength; nIndex++) {
            buffers[nIndex].clear();
            bufferPool.give(buffers[nIndex]);
            buffers[nIndex] = null;
        }
        bufferCount = 0;
        stream.setBuffer(null);
        return this;
    }
}

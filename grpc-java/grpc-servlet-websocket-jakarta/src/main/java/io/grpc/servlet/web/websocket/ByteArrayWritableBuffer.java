package io.grpc.servlet.web.websocket;

import io.grpc.internal.WritableBuffer;

import static java.lang.Math.max;
import static java.lang.Math.min;

final class ByteArrayWritableBuffer implements WritableBuffer {

    private final int capacity;
    final byte[] bytes;
    private int index;

    ByteArrayWritableBuffer(int capacityHint) {
        this.bytes = new byte[min(1024 * 1024, max(4096, capacityHint))];
        this.capacity = bytes.length;
    }

    @Override
    public void write(byte[] src, int srcIndex, int length) {
        System.arraycopy(src, srcIndex, bytes, index, length);
        index += length;
    }

    @Override
    public void write(byte b) {
        bytes[index++] = b;
    }

    @Override
    public int writableBytes() {
        return capacity - index;
    }

    @Override
    public int readableBytes() {
        return index;
    }

    @Override
    public void release() {}
}

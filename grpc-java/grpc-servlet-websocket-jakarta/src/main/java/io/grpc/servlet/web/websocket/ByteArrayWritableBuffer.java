/*
 * Copyright 2022 Deephaven Data Labs
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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

//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.util.mutable.MutableLong;
import io.deephaven.vector.Vector;

import java.lang.reflect.Array;

/** TODO: ADD JAVADOC TO KERNEL INTERFACES. **/
class UngroupSizeKernels {
    @FunctionalInterface
    interface SizeFunction {
        void size(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset);
    }

    @FunctionalInterface
    interface MaxSizeFunction {
        long maxSize(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset);
    }

    @FunctionalInterface
    interface CheckSizeFunction {
        int checkSize(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset,
                MutableLong encounteredSize);
    }

    @FunctionalInterface
    interface MaybeIncreaseSizeFunction {
        long maybeIncreaseSize(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset);
    }

    static void sizeArray(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset) {
        final int chunkSize = chunk.size();
        for (int ii = 0; ii < chunkSize; ++ii) {
            final Object array = chunk.get(ii);
            final int size = array == null ? 0 : Array.getLength(array);
            sizesOut[ii + offset] = size;
        }
    }

    static void sizeVector(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset) {
        final int chunkSize = chunk.size();
        for (int ii = 0; ii < chunkSize; ++ii) {
            final Vector<?> vector = (Vector<?>) chunk.get(ii);
            final long size = vector != null ? vector.size() : 0;
            sizesOut[ii + offset] = size;
        }
    }

    static long maxSizeArray(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset) {
        long maxSize = 0;
        final int chunkSize = chunk.size();
        for (int ii = 0; ii < chunkSize; ++ii) {
            final Object array = chunk.get(ii);
            final int size = array == null ? 0 : Array.getLength(array);
            sizesOut[ii + offset] = size;
            maxSize = Math.max(size, maxSize);
        }
        return maxSize;
    }

    static long maxSizeVector(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset) {
        long maxSize = 0;
        final int chunkSize = chunk.size();
        for (int ii = 0; ii < chunkSize; ++ii) {
            final Vector<?> vector = (Vector<?>) chunk.get(ii);
            final long size = vector != null ? vector.size() : 0;
            sizesOut[ii + offset] = size;
            maxSize = Math.max(size, maxSize);
        }
        return maxSize;
    }

    static int checkSizeArray(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset,
            final MutableLong encounteredSize) {
        final int chunkSize = chunk.size();
        for (int ii = 0; ii < chunkSize; ++ii) {
            final Object array = chunk.get(ii);
            final int size = array == null ? 0 : Array.getLength(array);
            if (sizesOut[ii + offset] != size) {
                encounteredSize.set(size);
                return ii;
            }
        }
        return -1;
    }

    static int checkSizeVector(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset,
            final MutableLong encounteredSize) {
        final int chunkSize = chunk.size();
        for (int ii = 0; ii < chunkSize; ++ii) {
            final Vector<?> vector = (Vector<?>) chunk.get(ii);
            final long size = vector != null ? vector.size() : 0;
            if (sizesOut[ii + offset] != size) {
                encounteredSize.set(size);
                return ii;
            }
        }
        return -1;
    }

    static long maybeIncreaseSizeArray(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset) {
        long maxSize = 0;
        final int chunkSize = chunk.size();
        for (int ii = 0; ii < chunkSize; ++ii) {
            final Object array = chunk.get(ii);
            final int size = array == null ? 0 : Array.getLength(array);
            final long existing = sizesOut[ii + offset];
            sizesOut[ii + offset] = Math.max(size, existing);
            maxSize = Math.max(sizesOut[ii + offset], maxSize);
        }
        return maxSize;
    }

    static long maybeIncreaseSizeVector(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset) {
        long maxSize = 0;
        final int chunkSize = chunk.size();
        for (int ii = 0; ii < chunkSize; ++ii) {
            final Vector<?> vector = (Vector<?>) chunk.get(ii);
            final long size = vector != null ? vector.size() : 0;
            final long existing = sizesOut[ii + offset];
            sizesOut[ii + offset] = Math.max(size, existing);
            maxSize = Math.max(sizesOut[ii + offset], maxSize);
        }
        return maxSize;
    }
}

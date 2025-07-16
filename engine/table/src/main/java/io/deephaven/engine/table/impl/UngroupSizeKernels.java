//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.util.mutable.MutableLong;
import io.deephaven.vector.Vector;

import java.lang.reflect.Array;

public class UngroupSizeKernels {
    @FunctionalInterface
    public interface SizeFunction {
        void size(final ObjectChunk<Object, ?> chunk, final long[] sizes, final int offset);
    }

    @FunctionalInterface
    public interface MaxSizeFunction {
        long maxSize(final ObjectChunk<Object, ?> chunk, final long[] sizes, final int offset);
    }

    @FunctionalInterface
    public interface CheckSizeFunction {
        int checkSize(final ObjectChunk<Object, ?> chunk, final long[] sizes, final int offset,
                MutableLong encounteredSize);
    }

    @FunctionalInterface
    public interface MaybeIncreaseSizeFunction {
        long maybeIncreaseSize(final ObjectChunk<Object, ?> chunk, final long[] sizes, final int offset);
    }

    static void sizeArray(final ObjectChunk<Object, ?> chunk, final long[] sizes, final int offset) {
        for (int ii = 0; ii < chunk.size(); ++ii) {
            final Object array = chunk.get(ii);
            final int size = array == null ? 0 : Array.getLength(array);
            sizes[ii + offset] = size;
        }
    }

    static void sizeVector(final ObjectChunk<Object, ?> chunk, final long[] sizes, final int offset) {
        for (int ii = 0; ii < chunk.size(); ++ii) {
            final Vector<?> vector = (Vector<?>) chunk.get(ii);
            final long size = vector != null ? vector.size() : 0;
            sizes[ii + offset] = size;
        }
    }

    static long maxSizeArray(final ObjectChunk<Object, ?> chunk, final long[] sizes, final int offset) {
        long maxSize = 0;
        for (int ii = 0; ii < chunk.size(); ++ii) {
            final Object array = chunk.get(ii);
            final int size = array == null ? 0 : Array.getLength(array);
            sizes[ii + offset] = size;
            maxSize = Math.max(size, maxSize);
        }
        return maxSize;
    }

    static long maxSizeVector(final ObjectChunk<Object, ?> chunk, final long[] sizes, final int offset) {
        long maxSize = 0;
        for (int ii = 0; ii < chunk.size(); ++ii) {
            final Vector<?> vector = (Vector<?>) chunk.get(ii);
            final long size = vector != null ? vector.size() : 0;
            sizes[ii + offset] = size;
            maxSize = Math.max(size, maxSize);
        }
        return maxSize;
    }

    static int checkSizeArray(final ObjectChunk<Object, ?> chunk, final long[] sizes, final int offset,
            final MutableLong encounteredSize) {
        for (int ii = 0; ii < chunk.size(); ++ii) {
            final Object array = chunk.get(ii);
            final int size = array == null ? 0 : Array.getLength(array);
            if (sizes[ii + offset] != size) {
                encounteredSize.set(size);
                return ii;
            }
        }
        return -1;
    }

    static int checkSizeVector(final ObjectChunk<Object, ?> chunk, final long[] sizes, final int offset,
            final MutableLong encounteredSize) {
        for (int ii = 0; ii < chunk.size(); ++ii) {
            final Vector<?> vector = (Vector<?>) chunk.get(ii);
            final long size = vector != null ? vector.size() : 0;
            if (sizes[ii + offset] != size) {
                encounteredSize.set(size);
                return ii;
            }
        }
        return -1;
    }

    static long maybeIncreaseSizeArray(final ObjectChunk<Object, ?> chunk, final long[] sizes, final int offset) {
        long maxSize = 0;
        for (int ii = 0; ii < chunk.size(); ++ii) {
            final Object array = chunk.get(ii);
            final int size = array == null ? 0 : Array.getLength(array);
            final long existing = sizes[ii + offset];
            sizes[ii + offset] = Math.max(size, existing);
            maxSize = Math.max(sizes[ii + offset], maxSize);
        }
        return maxSize;
    }

    static long maybeIncreaseSizeVector(final ObjectChunk<Object, ?> chunk, final long[] sizes, final int offset) {
        long maxSize = 0;
        for (int ii = 0; ii < chunk.size(); ++ii) {
            final Vector<?> vector = (Vector<?>) chunk.get(ii);
            final long size = vector != null ? vector.size() : 0;
            final long existing = sizes[ii + offset];
            sizes[ii + offset] = Math.max(size, existing);
            maxSize = Math.max(sizes[ii + offset], maxSize);
        }
        return maxSize;
    }
}

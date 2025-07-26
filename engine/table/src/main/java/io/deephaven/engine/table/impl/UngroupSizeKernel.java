//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.engine.table.impl;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.util.mutable.MutableLong;
import io.deephaven.vector.Vector;

import java.lang.reflect.Array;

/**
 * Kernels for determining the size of an array or vector.
 */
interface UngroupSizeKernel {
    /**
     * Determine the size of each element in {@code chunk}. Write results beginning at {@code offset} in
     * {@code sizesOut}. Additionally, returns the maximum size seen.
     *
     * @param chunk the input chunk
     * @param sizesOut the output array
     * @param offset the position in the output array of the first result
     *
     * @return the maximum size encountered
     */
    long maxSize(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset);


    /**
     * Compare the size of the size of each element in {@code chunk} to {@code sizesIn}.
     * 
     * @param chunk the input chunk
     * @param sizesIn the array of expected sizes
     * @param offset the position in the sizes array of the first element to check
     * @param encounteredSize the mismatched size encountered (unchanged if no mismatched size was encountered)
     * @return the position in the chunk of the first mismatched size, otherwise less than zero
     */
    int checkSize(final ObjectChunk<Object, ?> chunk, final long[] sizesIn, final int offset,
            MutableLong encounteredSize);

    /**
     * Determine the size of each element in {@code chunk}. Write the maximum of the existing size in
     * {@code sizesInAndOut} and the newly calculated size for each element into {@code sizesInAndOut}. Additionally,
     * returns the maximum size over all elements.
     *
     * @param chunk the input chunk
     * @param sizesInAndOut array containing existing sizes to be overwritten with new sizes
     * @param offset the position in sizesInAndOut of the first result
     *
     * @return the maximum increased size encountered in this region of sizesInAndOut and chunk
     */
    long maybeIncreaseSize(final ObjectChunk<Object, ?> chunk, final long[] sizesInAndOut, final int offset);

    class ArraySizeKernel implements UngroupSizeKernel {
        public static ArraySizeKernel INSTANCE = new ArraySizeKernel();

        @Override
        public long maxSize(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset) {
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

        @Override
        public int checkSize(final ObjectChunk<Object, ?> chunk, final long[] sizesIn, final int offset,
                final MutableLong encounteredSize) {
            final int chunkSize = chunk.size();
            for (int ii = 0; ii < chunkSize; ++ii) {
                final Object array = chunk.get(ii);
                final int size = array == null ? 0 : Array.getLength(array);
                if (sizesIn[ii + offset] != size) {
                    encounteredSize.set(size);
                    return ii;
                }
            }
            return -1;
        }

        @Override
        public long maybeIncreaseSize(final ObjectChunk<Object, ?> chunk, final long[] sizesInAndOut,
                final int offset) {
            long maxIncreasedSize = 0;
            final int chunkSize = chunk.size();
            for (int ii = 0; ii < chunkSize; ++ii) {
                final Object array = chunk.get(ii);
                final int size = array == null ? 0 : Array.getLength(array);
                final long existing = sizesInAndOut[ii + offset];
                if (size > existing) {
                    sizesInAndOut[ii + offset] = size;
                    maxIncreasedSize = Math.max(size, maxIncreasedSize);
                }
            }
            return maxIncreasedSize;
        }

    }

    class VectorSizeKernel implements UngroupSizeKernel {
        public static VectorSizeKernel INSTANCE = new VectorSizeKernel();

        @Override
        public long maxSize(final ObjectChunk<Object, ?> chunk, final long[] sizesOut, final int offset) {
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

        @Override
        public int checkSize(final ObjectChunk<Object, ?> chunk, final long[] sizesIn, final int offset,
                final MutableLong encounteredSize) {
            final int chunkSize = chunk.size();
            for (int ii = 0; ii < chunkSize; ++ii) {
                final Vector<?> vector = (Vector<?>) chunk.get(ii);
                final long size = vector != null ? vector.size() : 0;
                if (sizesIn[ii + offset] != size) {
                    encounteredSize.set(size);
                    return ii;
                }
            }
            return -1;
        }

        @Override
        public long maybeIncreaseSize(final ObjectChunk<Object, ?> chunk, final long[] sizesInAndOut,
                final int offset) {
            long maxIncreasedSize = 0;
            final int chunkSize = chunk.size();
            for (int ii = 0; ii < chunkSize; ++ii) {
                final Vector<?> vector = (Vector<?>) chunk.get(ii);
                final long size = vector != null ? vector.size() : 0;
                final long existing = sizesInAndOut[ii + offset];
                if (size > existing) {
                    sizesInAndOut[ii + offset] = size;
                    maxIncreasedSize = Math.max(size, maxIncreasedSize);
                }
            }
            return maxIncreasedSize;
        }
    }
}

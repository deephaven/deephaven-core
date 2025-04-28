//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.ChunkLengths;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

public class ListChunkReader<T> extends BaseChunkReader<WritableObjectChunk<T, Values>> {
    public enum Mode {
        FIXED, VARIABLE, VIEW
    }

    private static final String DEBUG_NAME = "ListChunkReader";

    private final Mode mode;
    private final int fixedSizeLength;
    private final ExpansionKernel<?> kernel;
    private final ChunkReader<? extends WritableChunk<Values>> componentReader;

    public ListChunkReader(
            final Mode mode,
            final int fixedSizeLength,
            final ExpansionKernel<?> kernel,
            final ChunkReader<? extends WritableChunk<Values>> componentReader) {
        this.mode = mode;
        this.fixedSizeLength = fixedSizeLength;
        this.componentReader = componentReader;
        this.kernel = kernel;
    }

    @Override
    public WritableObjectChunk<T, Values> readChunk(
            @NotNull final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull final PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull final DataInput is,
            @Nullable final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {
        final ChunkWriter.FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBufferLength = bufferInfoIter.nextLong();
        // have an offsets buffer if not every element is the same length
        final long offsetsBufferLength = mode == Mode.FIXED ? 0 : bufferInfoIter.nextLong();
        // have a lengths buffer if ListView instead of List
        final long lengthsBufferLength = mode != Mode.VIEW ? 0 : bufferInfoIter.nextLong();

        WritableObjectChunk<T, Values> chunk = BaseChunkReader.castOrCreateChunk(
                outChunk,
                outOffset,
                Math.max(totalRows, nodeInfo.numElements),
                WritableObjectChunk::makeWritableChunk,
                WritableChunk::asWritableObjectChunk);

        if (nodeInfo.numElements == 0) {
            // must consume any advertised inner payload even though there "aren't any rows"
            is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME,
                    validityBufferLength + offsetsBufferLength + lengthsBufferLength));
            try (final WritableChunk<Values> ignored =
                    componentReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {
                return chunk;
            }
        }

        final int numValidityLongs = (nodeInfo.numElements + 63) / 64;
        final int numOffsets = nodeInfo.numElements + (mode == Mode.VARIABLE ? 1 : 0);
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs);
                final WritableIntChunk<ChunkPositions> offsets = mode == Mode.FIXED
                        ? null
                        : WritableIntChunk.makeWritableChunk(numOffsets);
                final WritableIntChunk<ChunkLengths> lengths = mode != Mode.VIEW
                        ? null
                        : WritableIntChunk.makeWritableChunk(nodeInfo.numElements)) {

            readValidityBuffer(is, numValidityLongs, validityBufferLength, isValid, DEBUG_NAME);

            // Read offsets:
            if (offsets != null) {
                final long offBufRead = (long) numOffsets * Integer.BYTES;
                if (offsetsBufferLength < offBufRead) {
                    throw new IllegalStateException(
                            "list offset buffer is too short for the expected number of elements");
                }
                for (int ii = 0; ii < numOffsets; ++ii) {
                    offsets.set(ii, is.readInt());
                }
                if (offBufRead < offsetsBufferLength) {
                    is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, offsetsBufferLength - offBufRead));
                }
            }

            // Read lengths:
            if (lengths != null) {
                final long lenBufRead = ((long) nodeInfo.numElements) * Integer.BYTES;
                if (lengthsBufferLength < lenBufRead) {
                    throw new IllegalStateException(
                            "list sizes buffer is too short for the expected number of elements");
                }
                for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                    lengths.set(ii, is.readInt());
                }
                if (lenBufRead < lengthsBufferLength) {
                    is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, lengthsBufferLength - lenBufRead));
                }
            }

            try (final WritableChunk<Values> inner =
                    componentReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {
                // noinspection unchecked
                chunk = (WritableObjectChunk<T, Values>) kernel.contract(
                        inner, fixedSizeLength, offsets, lengths, chunk, outOffset, totalRows);
                if (outChunk != null) {
                    // expect our kernel to have returned the same chunk
                    Assert.eq(chunk, "chunk", outChunk, "outChunk");
                }

                long nextValid = 0;
                for (int ii = 0; ii < nodeInfo.numElements;) {
                    if ((ii % 64) == 0) {
                        nextValid = ~isValid.get(ii / 64);
                    }
                    if ((nextValid & 0x1) == 0x1) {
                        chunk.set(outOffset + ii, null);
                    }
                    final int numToSkip = Math.min(
                            Long.numberOfTrailingZeros(nextValid & (~0x1)),
                            64 - (ii % 64));
                    nextValid >>= numToSkip;
                    ii += numToSkip;
                }
            }
        }

        return chunk;
    }
}

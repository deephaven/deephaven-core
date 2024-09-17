//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.extensions.barrage.chunk.array.ArrayExpansionKernel;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.datastructures.LongSizedDataStructure;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;

import static io.deephaven.extensions.barrage.chunk.ChunkReader.typeInfo;

public class VarListChunkReader<T> implements ChunkReader {
    private static final String DEBUG_NAME = "VarListChunkReader";

    private final ArrayExpansionKernel kernel;
    private final ChunkReader componentReader;

    public VarListChunkReader(final StreamReaderOptions options, final TypeInfo typeInfo,
            Factory chunkReaderFactory) {
        final Class<?> componentType = typeInfo.type().getComponentType();
        final Class<?> innerComponentType = componentType != null ? componentType.getComponentType() : null;

        final ChunkType chunkType;
        if (componentType == boolean.class || componentType == Boolean.class) {
            // Note: Internally booleans are passed around as bytes, but the wire format is packed bits.
            chunkType = ChunkType.Byte;
        } else if (componentType != null && !componentType.isPrimitive()) {
            chunkType = ChunkType.Object;
        } else {
            chunkType = ChunkType.fromElementType(componentType);
        }
        kernel = ArrayExpansionKernel.makeExpansionKernel(chunkType, componentType);

        componentReader = chunkReaderFactory.getReader(options,
                typeInfo(chunkType, componentType, innerComponentType, typeInfo.componentArrowField()));
    }

    @Override
    public WritableObjectChunk<T, Values> readChunk(Iterator<ChunkInputStreamGenerator.FieldNodeInfo> fieldNodeIter,
            PrimitiveIterator.OfLong bufferInfoIter, DataInput is, WritableChunk<Values> outChunk, int outOffset,
            int totalRows) throws IOException {
        final ChunkInputStreamGenerator.FieldNodeInfo nodeInfo = fieldNodeIter.next();
        final long validityBuffer = bufferInfoIter.nextLong();
        final long offsetsBuffer = bufferInfoIter.nextLong();

        if (nodeInfo.numElements == 0) {
            try (final WritableChunk<Values> ignored =
                    componentReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {
                return WritableObjectChunk.makeWritableChunk(nodeInfo.numElements);
            }
        }

        final WritableObjectChunk<T, Values> chunk;
        final int numValidityLongs = (nodeInfo.numElements + 63) / 64;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs);
                final WritableIntChunk<ChunkPositions> offsets =
                        WritableIntChunk.makeWritableChunk(nodeInfo.numElements + 1)) {
            // Read validity buffer:
            int jj = 0;
            for (; jj < Math.min(numValidityLongs, validityBuffer / 8); ++jj) {
                isValid.set(jj, is.readLong());
            }
            final long valBufRead = jj * 8L;
            if (valBufRead < validityBuffer) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, validityBuffer - valBufRead));
            }
            // we support short validity buffers
            for (; jj < numValidityLongs; ++jj) {
                isValid.set(jj, -1); // -1 is bit-wise representation of all ones
            }
            // consumed entire validity buffer by here

            // Read offsets:
            final long offBufRead = (nodeInfo.numElements + 1L) * Integer.BYTES;
            if (offsetsBuffer < offBufRead) {
                throw new IllegalStateException("offset buffer is too short for the expected number of elements");
            }
            for (int i = 0; i < nodeInfo.numElements + 1; ++i) {
                offsets.set(i, is.readInt());
            }
            if (offBufRead < offsetsBuffer) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, offsetsBuffer - offBufRead));
            }

            try (final WritableChunk<Values> inner =
                    componentReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {
                chunk = kernel.contract(inner, offsets, outChunk, outOffset, totalRows);

                long nextValid = 0;
                for (int ii = 0; ii < nodeInfo.numElements; ++ii) {
                    if ((ii % 64) == 0) {
                        nextValid = isValid.get(ii / 64);
                    }
                    if ((nextValid & 0x1) == 0x0) {
                        chunk.set(outOffset + ii, null);
                    }
                    nextValid >>= 1;
                }
            }
        }

        return chunk;
    }
}

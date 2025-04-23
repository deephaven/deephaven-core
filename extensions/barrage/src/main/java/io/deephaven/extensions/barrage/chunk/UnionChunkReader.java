//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableByteChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.util.SafeCloseable;
import io.deephaven.util.SafeCloseableList;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.apache.arrow.vector.types.UnionMode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PrimitiveIterator;

public class UnionChunkReader<T> extends BaseChunkReader<WritableObjectChunk<T, Values>> {
    public enum Mode {
        Dense, Sparse
    }

    public static Mode mode(UnionMode mode) {
        return mode == UnionMode.Dense ? Mode.Dense : Mode.Sparse;
    }

    private static final String DEBUG_NAME = "UnionChunkReader";

    private final Mode mode;
    private final List<ChunkReader<? extends WritableChunk<Values>>> readers;
    private final Map<Byte, Integer> typeToIndex;

    public UnionChunkReader(
            final Mode mode,
            final List<ChunkReader<? extends WritableChunk<Values>>> readers,
            final Map<Byte, Integer> typeToIndex) {
        this.mode = mode;
        this.readers = readers;
        this.typeToIndex = typeToIndex;
        // the specification doesn't allow the union column to have more than signed byte number of types
        Assert.leq(readers.size(), "readers.size()", Byte.MAX_VALUE, "Byte.MAX_VALUE");
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
        // column of interest buffer
        final long coiBufferLength = bufferInfoIter.nextLong();
        // if Dense we also have an offset buffer
        final long offsetsBufferLength = mode == Mode.Dense ? bufferInfoIter.nextLong() : 0;

        final WritableObjectChunk<T, Values> result = castOrCreateChunk(
                outChunk,
                outOffset,
                Math.max(totalRows, nodeInfo.numElements),
                WritableObjectChunk::makeWritableChunk,
                WritableChunk::asWritableObjectChunk);

        int numRows = nodeInfo.numElements;
        if (numRows == 0) {
            // must consume any advertised inner payload even though there "aren't any rows"
            is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, coiBufferLength + offsetsBufferLength));
            for (final ChunkReader<? extends WritableChunk<Values>> reader : readers) {
                // noinspection EmptyTryBlock
                try (final SafeCloseable ignored = reader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {
                    // do nothing; we need each reader to consume fieldNodeIter and bufferInfoIter
                }
            }
            return result;
        }

        try (final WritableByteChunk<ChunkPositions> columnsOfInterest =
                WritableByteChunk.makeWritableChunk(numRows);
                final WritableIntChunk<ChunkPositions> offsets = mode == Mode.Sparse
                        ? null
                        : WritableIntChunk.makeWritableChunk(numRows);
                final SafeCloseableList closeableList = new SafeCloseableList()) {

            // Read columns of interest:
            final long coiBufRead = (long) numRows * Byte.BYTES;
            if (coiBufferLength < coiBufRead) {
                throw new IllegalStateException(
                        "column of interest buffer is too short for the expected number of elements");
            }
            for (int ii = 0; ii < numRows; ++ii) {
                columnsOfInterest.set(ii, is.readByte());
            }
            if (coiBufRead < coiBufferLength) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, coiBufferLength - coiBufRead));
            }


            // Read offsets:
            if (offsets != null) {
                final long offBufRead = (long) numRows * Integer.BYTES;
                if (offsetsBufferLength < offBufRead) {
                    throw new IllegalStateException(
                            "union offset buffer is too short for the expected number of elements");
                }
                for (int ii = 0; ii < numRows; ++ii) {
                    offsets.set(ii, is.readInt());
                }
                if (offBufRead < offsetsBufferLength) {
                    is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, offsetsBufferLength - offBufRead));
                }
            }

            // noinspection unchecked
            final ObjectChunk<T, Values>[] chunks = new ObjectChunk[readers.size()];

            for (int ii = 0; ii < readers.size(); ++ii) {
                final WritableChunk<Values> chunk =
                        readers.get(ii).readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0);
                closeableList.add(chunk);

                final ChunkBoxer.BoxerKernel boxer = ChunkBoxer.getBoxer(chunk.getChunkType(), chunk.size());
                closeableList.add(boxer);

                // noinspection unchecked
                chunks[ii] = (ObjectChunk<T, Values>) boxer.box(chunk);
            }

            for (int ii = 0; ii < columnsOfInterest.size(); ++ii) {
                final byte coi = columnsOfInterest.get(ii);
                final Integer mappedIndex = typeToIndex.get(coi);
                if (mappedIndex == null) {
                    throw new IllegalStateException("union column uses unexpected column of interest: " + coi);
                }
                final int offset;
                if (offsets != null) {
                    offset = offsets.get(ii);
                } else {
                    offset = ii;
                }

                result.set(outOffset + ii, chunks[mappedIndex].get(offset));
            }
            result.setSize(outOffset + columnsOfInterest.size());

            return result;
        }
    }
}

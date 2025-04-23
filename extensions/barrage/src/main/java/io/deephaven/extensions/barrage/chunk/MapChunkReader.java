//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.chunk.ObjectChunk;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.WritableIntChunk;
import io.deephaven.chunk.WritableLongChunk;
import io.deephaven.chunk.WritableObjectChunk;
import io.deephaven.chunk.attributes.ChunkPositions;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.table.impl.chunkboxer.ChunkBoxer;
import io.deephaven.util.datastructures.LongSizedDataStructure;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.PrimitiveIterator;

public class MapChunkReader<T> extends BaseChunkReader<WritableObjectChunk<T, Values>> {
    private static final String DEBUG_NAME = "MapChunkReader";

    private final ChunkReader<? extends WritableChunk<Values>> keyReader;
    private final ChunkReader<? extends WritableChunk<Values>> valueReader;

    public MapChunkReader(
            final ChunkReader<? extends WritableChunk<Values>> keyReader,
            final ChunkReader<? extends WritableChunk<Values>> valueReader) {
        this.keyReader = keyReader;
        this.valueReader = valueReader;
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
        // an arrow map is represented as a List<Struct<Pair<Key, Value>>>; the struct is superfluous, but we must
        // consume the field node anyway
        final ChunkWriter.FieldNodeInfo structInfo = fieldNodeIter.next();
        final long validityBufferLength = bufferInfoIter.nextLong();
        final long offsetsBufferLength = bufferInfoIter.nextLong();
        final long structValidityBufferLength = bufferInfoIter.nextLong();

        final WritableObjectChunk<T, Values> chunk = BaseChunkReader.castOrCreateChunk(
                outChunk,
                outOffset,
                Math.max(totalRows, nodeInfo.numElements),
                WritableObjectChunk::makeWritableChunk,
                WritableChunk::asWritableObjectChunk);

        if (nodeInfo.numElements == 0) {
            // must consume any advertised inner payload even though there "aren't any rows"
            is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME,
                    validityBufferLength + offsetsBufferLength + structValidityBufferLength));
            try (final WritableChunk<Values> ignored =
                    keyReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0);
                    final WritableChunk<Values> ignored2 =
                            valueReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0)) {
                return chunk;
            }
        }

        final int numValidityLongs = (nodeInfo.numElements + 63) / 64;
        final int numOffsets = nodeInfo.numElements + 1;
        try (final WritableLongChunk<Values> isValid = WritableLongChunk.makeWritableChunk(numValidityLongs);
                final WritableIntChunk<ChunkPositions> offsets = WritableIntChunk.makeWritableChunk(numOffsets)) {

            readValidityBuffer(is, numValidityLongs, validityBufferLength, isValid, DEBUG_NAME);

            // Read offsets:
            final long offBufRead = (long) numOffsets * Integer.BYTES;
            if (offsetsBufferLength < offBufRead) {
                throw new IllegalStateException(
                        "map offset buffer is too short for the expected number of elements");
            }
            for (int ii = 0; ii < numOffsets; ++ii) {
                offsets.set(ii, is.readInt());
            }
            if (offBufRead < offsetsBufferLength) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, offsetsBufferLength - offBufRead));
            }

            // it doesn't make sense to have a struct validity buffer for a map
            if (structValidityBufferLength > 0) {
                is.skipBytes(LongSizedDataStructure.intSize(DEBUG_NAME, structValidityBufferLength));
            }

            try (final WritableChunk<Values> keysPrim =
                    keyReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0);
                    final WritableChunk<Values> valuesPrim =
                            valueReader.readChunk(fieldNodeIter, bufferInfoIter, is, null, 0, 0);
                    final ChunkBoxer.BoxerKernel keyBoxer =
                            ChunkBoxer.getBoxer(keysPrim.getChunkType(), keysPrim.size());
                    final ChunkBoxer.BoxerKernel valueBoxer =
                            ChunkBoxer.getBoxer(valuesPrim.getChunkType(), valuesPrim.size())) {
                final ObjectChunk<Object, ? extends Values> keys = keyBoxer.box(keysPrim).asObjectChunk();
                final ObjectChunk<Object, ? extends Values> values = valueBoxer.box(valuesPrim).asObjectChunk();

                long nextValid = 0;
                for (int ii = 0; ii < nodeInfo.numElements; nextValid >>= 1, ++ii) {
                    if ((ii % 64) == 0) {
                        nextValid = ~isValid.get(ii / 64);
                    }
                    if ((nextValid & 0x1) == 0x1) {
                        chunk.set(outOffset + ii, null);
                    } else {
                        final Map<Object, Object> row = new LinkedHashMap<>();
                        for (int jj = offsets.get(ii); jj < offsets.get(ii + 1); ++jj) {
                            row.put(keys.get(jj), values.get(jj));
                        }
                        // noinspection unchecked
                        chunk.set(outOffset + ii, (T) row);
                    }
                }
            }
        }

        return chunk;
    }
}

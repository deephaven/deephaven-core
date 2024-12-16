//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.chunk;

import io.deephaven.base.verify.Assert;
import io.deephaven.chunk.WritableChunk;
import io.deephaven.chunk.attributes.Values;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.DataInput;
import java.io.IOException;
import java.util.Iterator;
import java.util.PrimitiveIterator;
import java.util.function.Function;
import java.util.function.IntFunction;

/**
 * A {@link ChunkReader} that reads a chunk of wire values and transforms them into a different chunk type.
 * 
 * @param <INPUT_CHUNK_TYPE> the input chunk type
 * @param <OUTPUT_CHUNK_TYPE> the output chunk type
 */
public class TransformingChunkReader<INPUT_CHUNK_TYPE extends WritableChunk<Values>, OUTPUT_CHUNK_TYPE extends WritableChunk<Values>>
        extends BaseChunkReader<OUTPUT_CHUNK_TYPE> {

    public interface TransformFunction<INPUT_CHUNK_TYPE extends WritableChunk<Values>, OUTPUT_CHUNK_TYPE extends WritableChunk<Values>> {
        void apply(INPUT_CHUNK_TYPE wireValues, OUTPUT_CHUNK_TYPE outChunk, int wireOffset, int outOffset);
    }

    private final ChunkReader<INPUT_CHUNK_TYPE> wireChunkReader;
    private final IntFunction<OUTPUT_CHUNK_TYPE> chunkFactory;
    private final Function<WritableChunk<Values>, OUTPUT_CHUNK_TYPE> castFunction;
    private final TransformFunction<INPUT_CHUNK_TYPE, OUTPUT_CHUNK_TYPE> transformFunction;

    public TransformingChunkReader(
            @NotNull final ChunkReader<INPUT_CHUNK_TYPE> wireChunkReader,
            final IntFunction<OUTPUT_CHUNK_TYPE> chunkFactory,
            final Function<WritableChunk<Values>, OUTPUT_CHUNK_TYPE> castFunction,
            final TransformFunction<INPUT_CHUNK_TYPE, OUTPUT_CHUNK_TYPE> transformFunction) {
        this.wireChunkReader = wireChunkReader;
        this.chunkFactory = chunkFactory;
        this.castFunction = castFunction;
        this.transformFunction = transformFunction;
    }

    @Override
    public OUTPUT_CHUNK_TYPE readChunk(
            @NotNull final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull final PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull final DataInput is,
            @Nullable final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {
        try (final INPUT_CHUNK_TYPE wireValues = wireChunkReader.readChunk(fieldNodeIter, bufferInfoIter, is)) {
            final OUTPUT_CHUNK_TYPE chunk = castOrCreateChunk(
                    outChunk, Math.max(totalRows, wireValues.size()), chunkFactory, castFunction);
            if (outChunk == null) {
                // if we're not given an output chunk then we better be writing at the front of the new one
                Assert.eqZero(outOffset, "outOffset");
            }
            for (int ii = 0; ii < wireValues.size(); ++ii) {
                transformFunction.apply(wireValues, chunk, ii, outOffset + ii);
            }
            chunk.setSize(outOffset + wireValues.size());
            return chunk;
        }
    }
}

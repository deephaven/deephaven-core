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
 * @param <InputChunkType> the input chunk type
 * @param <OutputChunkType> the output chunk type
 */
public class TransformingChunkReader<InputChunkType extends WritableChunk<Values>, OutputChunkType extends WritableChunk<Values>>
        extends BaseChunkReader<OutputChunkType> {

    public interface TransformFunction<InputChunkType extends WritableChunk<Values>, OutputChunkType extends WritableChunk<Values>> {
        void apply(InputChunkType wireValues, OutputChunkType outChunk, int wireOffset, int outOffset);
    }

    private final ChunkReader<InputChunkType> wireChunkReader;
    private final IntFunction<OutputChunkType> chunkFactory;
    private final Function<WritableChunk<Values>, OutputChunkType> castFunction;
    private final TransformFunction<InputChunkType, OutputChunkType> transformFunction;

    public TransformingChunkReader(
            @NotNull final ChunkReader<InputChunkType> wireChunkReader,
            final IntFunction<OutputChunkType> chunkFactory,
            final Function<WritableChunk<Values>, OutputChunkType> castFunction,
            final TransformFunction<InputChunkType, OutputChunkType> transformFunction) {
        this.wireChunkReader = wireChunkReader;
        this.chunkFactory = chunkFactory;
        this.castFunction = castFunction;
        this.transformFunction = transformFunction;
    }

    @Override
    public OutputChunkType readChunk(
            @NotNull final Iterator<ChunkWriter.FieldNodeInfo> fieldNodeIter,
            @NotNull final PrimitiveIterator.OfLong bufferInfoIter,
            @NotNull final DataInput is,
            @Nullable final WritableChunk<Values> outChunk,
            final int outOffset,
            final int totalRows) throws IOException {
        try (final InputChunkType wireValues = wireChunkReader.readChunk(fieldNodeIter, bufferInfoIter, is)) {
            final OutputChunkType chunk = castOrCreateChunk(
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

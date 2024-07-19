//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage;

import io.deephaven.chunk.Chunk;
import io.deephaven.chunk.ChunkType;
import io.deephaven.chunk.attributes.Values;
import io.deephaven.engine.rowset.RowSet;
import io.deephaven.extensions.barrage.chunk.ChunkInputStreamGenerator;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;
import io.deephaven.util.SafeCloseable;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ChunkListInputStreamGenerator implements SafeCloseable {
    private final List<ChunkInputStreamGenerator> generators;
    private final ChunkInputStreamGenerator emptyGenerator;

    public ChunkListInputStreamGenerator(ChunkInputStreamGenerator.Factory factory, Class<?> type,
            Class<?> componentType, List<Chunk<Values>> data,
            ChunkType chunkType) {
        // create an input stream generator for each chunk
        ChunkInputStreamGenerator[] generators = new ChunkInputStreamGenerator[data.size()];

        long rowOffset = 0;
        for (int i = 0; i < data.size(); ++i) {
            final Chunk<Values> valuesChunk = data.get(i);
            generators[i] = factory.makeInputStreamGenerator(chunkType, type, componentType,
                    valuesChunk, rowOffset);
            rowOffset += valuesChunk.size();
        }
        this.generators = Arrays.asList(generators);
        emptyGenerator = factory.makeInputStreamGenerator(
                chunkType, type, componentType, chunkType.getEmptyChunk(), 0);
    }

    public List<ChunkInputStreamGenerator> generators() {
        return generators;
    }

    public ChunkInputStreamGenerator.DrainableColumn empty(StreamReaderOptions options, RowSet rowSet)
            throws IOException {
        return emptyGenerator.getInputStream(options, rowSet);
    }

    @Override
    public void close() {
        for (ChunkInputStreamGenerator generator : generators) {
            generator.close();
        }
        emptyGenerator.close();
    }
}

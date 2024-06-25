//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.web.client.api.barrage;

import io.deephaven.extensions.barrage.chunk.ChunkReader;
import io.deephaven.extensions.barrage.chunk.ChunkReadingFactory;
import io.deephaven.extensions.barrage.util.StreamReaderOptions;

public class WebChunkReaderFactory implements ChunkReadingFactory {
    @Override
    public ChunkReader extractChunkFromInputStream(StreamReaderOptions options, int factor, ChunkTypeInfo typeInfo) {
        return null;
    }
}

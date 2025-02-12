//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.barrage.util;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.BarrageOptions;

import java.io.InputStream;

/**
 * A gRPC streaming reader that keeps stream specific context and converts {@link InputStream}s to
 * {@link BarrageMessage}s.
 */
public interface BarrageMessageReader {
    /**
     * Converts an {@link InputStream} to a {@link BarrageMessage} in the context of the provided parameters.
     *
     * @param options the options related to parsing this message
     * @param columnChunkTypes the types to use for each column chunk
     * @param columnTypes the actual type for the column
     * @param componentTypes the actual component type for the column
     * @param stream the input stream that holds the message to be parsed
     * @return a BarrageMessage filled out by the stream's payload
     */
    BarrageMessage safelyParseFrom(final BarrageOptions options,
            ChunkType[] columnChunkTypes,
            Class<?>[] columnTypes,
            Class<?>[] componentTypes,
            InputStream stream);
}

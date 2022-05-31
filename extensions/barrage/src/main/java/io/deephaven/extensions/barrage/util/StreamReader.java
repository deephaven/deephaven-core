package io.deephaven.extensions.barrage.util;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.impl.util.BarrageMessage;

import java.io.InputStream;
import java.util.BitSet;

/**
 * Thread safe re-usable reader that converts an InputStreams to BarrageMessages.
 *
 */
public interface StreamReader {
    /**
     * Converts an InputStream to a BarrageMessage in the context of the provided parameters.
     *
     * @param options the options related to parsing this message
     * @param expectedColumns the columns expected to appear in the stream, null implies all columns
     * @param columnChunkTypes the types to use for each column chunk
     * @param columnTypes the actual type for the column
     * @param componentTypes the actual component type for the column
     * @param stream the input stream that holds the message to be parsed
     * @return a BarrageMessage filled out by the stream's payload
     */
    BarrageMessage safelyParseFrom(final StreamReaderOptions options,
            BitSet expectedColumns,
            ChunkType[] columnChunkTypes,
            Class<?>[] columnTypes,
            Class<?>[] componentTypes,
            InputStream stream);

}

/*
 * Copyright (c) 2016-2021 Deephaven Data Labs and Patent Pending
 */

package io.deephaven.grpc_api.barrage;

import io.deephaven.db.v2.utils.BarrageMessage;
import io.deephaven.db.v2.sources.chunk.ChunkType;

import java.io.InputStream;

public class BarrageMessageConsumer {
    /**
     * Thread safe re-usable reader that converts an InputStreams to BarrageMessages.
     *
     * @param <Options> The options type this StreamReader needs to deserialize.
     */
    public interface StreamReader<Options> {
        /**
         * Converts an InputStream to a BarrageMessage in the context of the provided parameters.
         *
         * @param options the options related to parsing this message
         * @param columnChunkTypes the types to use for each column chunk
         * @param columnTypes the actual type for the column
         * @param componentTypes the actual component type for the column
         * @param stream the input stream that holds the message to be parsed
         * @return a BarrageMessage filled out by the stream's payload
         */
        BarrageMessage safelyParseFrom(final Options options,
            final ChunkType[] columnChunkTypes,
            final Class<?>[] columnTypes,
            final Class<?>[] componentTypes,
            final InputStream stream);
    }
}

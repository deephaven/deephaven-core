package io.deephaven.extensions.barrage.util;

import io.deephaven.chunk.ChunkType;
import io.deephaven.engine.table.impl.util.BarrageMessage;
import io.deephaven.extensions.barrage.BarrageSnapshotOptions;
import io.deephaven.extensions.barrage.BarrageSubscriptionOptions;
import io.deephaven.extensions.barrage.ColumnConversionMode;

import java.io.InputStream;

/**
 * Thread safe re-usable reader that converts an InputStreams to BarrageMessages.
 *
 */
public interface StreamReader {
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
    BarrageMessage safelyParseFrom(final StreamReaderOptions options,
            final ChunkType[] columnChunkTypes,
            final Class<?>[] columnTypes,
            final Class<?>[] componentTypes,
            final InputStream stream);

    class StreamReaderOptions {
        private final boolean useDeephavenNulls;
        private final ColumnConversionMode columnConversionMode;

        public StreamReaderOptions(BarrageSnapshotOptions options) {
            this.useDeephavenNulls = options.useDeephavenNulls();
            this.columnConversionMode = options.columnConversionMode();
        }

        public StreamReaderOptions(BarrageSubscriptionOptions options) {
            this.useDeephavenNulls = options.useDeephavenNulls();
            this.columnConversionMode = options.columnConversionMode();
        }

        public boolean useDeephavenNulls() {
            return useDeephavenNulls;
        }

        public ColumnConversionMode columnConversionMode() {
            return columnConversionMode;
        }
    }
}

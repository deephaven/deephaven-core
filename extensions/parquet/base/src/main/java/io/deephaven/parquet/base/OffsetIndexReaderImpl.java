//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.format.Util;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;

/**
 * Implementation of {@link OffsetIndexReader}, which reads the offset index for a column chunk on demand, and caches it
 * for future requests.
 */
final class OffsetIndexReaderImpl implements OffsetIndexReader {

    private final SeekableChannelsProvider channelsProvider;
    private final ColumnChunk columnChunk;
    private final URI columnChunkURI;
    private OffsetIndex offsetIndex;

    OffsetIndexReaderImpl(final SeekableChannelsProvider channelsProvider, final ColumnChunk columnChunk,
            final URI columnChunkURI) {
        this.channelsProvider = channelsProvider;
        this.columnChunk = columnChunk;
        this.columnChunkURI = columnChunkURI;
        this.offsetIndex = null;
    }

    @Override
    @Nullable
    public OffsetIndex getOffsetIndex(@NotNull final SeekableChannelContext context) {
        if (offsetIndex != null) {
            return offsetIndex;
        }
        if (!columnChunk.isSetOffset_index_offset()) {
            throw new UnsupportedOperationException("Cannot read offset index from this source.");
        }
        return readOffsetIndex(context);
    }

    private OffsetIndex readOffsetIndex(@NotNull final SeekableChannelContext channelContext) {
        try (
                final SeekableChannelContext.ContextHolder holder =
                        SeekableChannelContext.ensureContext(channelsProvider, channelContext);
                final SeekableByteChannel readChannel = channelsProvider.getReadChannel(holder.get(), columnChunkURI);
                final InputStream in =
                        channelsProvider.getInputStream(readChannel.position(columnChunk.getOffset_index_offset()),
                                columnChunk.getOffset_index_length())) {
            return (offsetIndex = ParquetMetadataConverter.fromParquetOffsetIndex(Util.readOffsetIndex(in)));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

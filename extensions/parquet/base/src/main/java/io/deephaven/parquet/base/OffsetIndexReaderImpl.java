//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.format.Util;
import org.apache.parquet.internal.hadoop.metadata.IndexReference;
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
    private final ColumnChunkMetaData columnChunk;
    private final URI columnChunkURI;
    private OffsetIndex offsetIndex;

    OffsetIndexReaderImpl(final SeekableChannelsProvider channelsProvider, final ColumnChunkMetaData columnChunk,
            final URI columnChunkURI) {
        if (columnChunk.getOffsetIndexReference() == null) {
            throw new IllegalArgumentException("Cannot read offset index from this source.");
        }
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
        try {
            return readOffsetIndex(context);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private OffsetIndex readOffsetIndex(@NotNull final SeekableChannelContext channelContext) throws IOException {
        final IndexReference ref = columnChunk.getOffsetIndexReference();
        try (
                final SeekableChannelContext.ContextHolder holder =
                        SeekableChannelContext.ensureContext(channelsProvider, channelContext);
                final SeekableByteChannel ch = channelsProvider.getReadChannel(holder.get(), columnChunkURI)) {
            ch.position(ref.getOffset());
            try (final InputStream in = channelsProvider.getInputStream(ch, ref.getLength())) {
                return offsetIndex = ParquetMetadataConverter.fromParquetOffsetIndex(Util.readOffsetIndex(in));
            }
        }
    }
}

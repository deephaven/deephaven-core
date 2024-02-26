package io.deephaven.parquet.base;

import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.parquet.format.ColumnChunk;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.internal.column.columnindex.OffsetIndex;
import org.apache.parquet.format.Util;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;

/**
 * Implementation of {@link OffsetIndexReader}, which reads the offset index for a column chunk on demand.
 */
final class OffsetIndexReaderImpl implements OffsetIndexReader {

    private final SeekableChannelsProvider channelsProvider;
    private final ColumnChunk chunk;
    private final URI columnChunkURI;
    private OffsetIndex offsetIndex;

    OffsetIndexReaderImpl(final SeekableChannelsProvider channelsProvider, final ColumnChunk chunk,
            final URI columnChunkURI) {
        this.channelsProvider = channelsProvider;
        this.chunk = chunk;
        this.columnChunkURI = columnChunkURI;
        this.offsetIndex = null;
    }

    @Override
    public OffsetIndex getOffsetIndex(@NotNull final SeekableChannelContext context) {
        if (offsetIndex != null) {
            return offsetIndex;
        }
        return readOffsetIndex(context);
    }

    private OffsetIndex readOffsetIndex(@NotNull final SeekableChannelContext channelContext) {
        try (
                final SeekableChannelContext.ContextHolder holder =
                        SeekableChannelContext.ensureContext(channelsProvider, channelContext);
                final SeekableByteChannel readChannel = channelsProvider.getReadChannel(holder.get(), columnChunkURI);
                final InputStream in =
                        channelsProvider.getInputStream(readChannel.position(chunk.getOffset_index_offset()))) {
            return (offsetIndex = ParquetMetadataConverter.fromParquetOffsetIndex(Util.readOffsetIndex(in)));
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

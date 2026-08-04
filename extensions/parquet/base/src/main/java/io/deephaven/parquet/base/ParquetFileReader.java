//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.parquet.base;

import io.deephaven.util.channel.CachedChannelProvider;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import org.apache.parquet.format.*;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.*;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.util.*;

import static io.deephaven.parquet.base.ParquetUtils.MAGIC;
import static io.deephaven.base.FileUtils.convertToURI;

/**
 * Top level accessor for a parquet file which can read from a CLI style file URI, ex."s3://bucket/key".
 */
public class ParquetFileReader {
    private static final int FOOTER_LENGTH_SIZE = 4;
    public static final String FILE_URI_SCHEME = "file";

    private static final ParquetMetadataConverter PARQUET_METADATA_CONVERTER = new ParquetMetadataConverter();

    public final FileMetaData fileMetaData;
    private final ParquetMetadata metadata;
    private final SeekableChannelsProvider channelsProvider;

    /**
     * If reading a single parquet file, root URI is the URI of the file, else the parent directory for a metadata file
     */
    private final URI rootURI;

    /**
     * Make a {@link ParquetFileReader} for the supplied {@link URI}. Wraps {@link IOException} as
     * {@link UncheckedIOException}.
     *
     * @param parquetFileURI The URI for the parquet file or the parquet metadata file
     * @param channelsProvider The {@link SeekableChannelsProvider} to use for reading the file
     * @return The new {@link ParquetFileReader}
     */
    public static ParquetFileReader create(
            @NotNull final URI parquetFileURI,
            @NotNull final SeekableChannelsProvider channelsProvider) {
        return createImpl(parquetFileURI, channelsProvider, -1);
    }

    /**
     * Make a {@link ParquetFileReader} for the supplied {@link URI}.
     *
     * @param parquetFileURI The URI for the parquet file or the parquet metadata file
     * @param channelsProvider The {@link SeekableChannelsProvider} to use for reading the file
     * @param fileSize The file size, must be positive
     * @return The new {@link ParquetFileReader}
     */
    public static ParquetFileReader create(
            @NotNull final URI parquetFileURI,
            @NotNull final SeekableChannelsProvider channelsProvider,
            final long fileSize) {
        if (fileSize <= 0) {
            // empty file is not legitimate for parquet files; possibly, we could have stricter min file size here in
            // the future.
            throw new IllegalArgumentException(String.format("fileSize must be positive: %s", parquetFileURI));
        }
        return createImpl(parquetFileURI, channelsProvider, fileSize);
    }

    private static ParquetFileReader createImpl(
            @NotNull final URI parquetFileURI,
            @NotNull final SeekableChannelsProvider provider,
            final long fileSize) {
        try {
            return new ParquetFileReader(parquetFileURI, provider, fileSize);
        } catch (final IOException e) {
            throw new UncheckedIOException(String.format("Failed to create Parquet file reader: %s", parquetFileURI),
                    e);
        }
    }

    private ParquetFileReader(
            @NotNull final URI parquetFileURI,
            @NotNull final SeekableChannelsProvider provider,
            final long fileSize) throws IOException {
        this.channelsProvider = CachedChannelProvider.create(provider, 1 << 7);
        if (!parquetFileURI.getRawPath().endsWith(".parquet") && FILE_URI_SCHEME.equals(parquetFileURI.getScheme())) {
            // Construct a new file URI for the parent directory
            rootURI = convertToURI(new File(parquetFileURI).getParentFile(), true);
        } else {
            rootURI = parquetFileURI;
        }
        try (
                final SeekableChannelContext context = channelsProvider.makeSingleUseReadContext();
                final SeekableByteChannel ch = fileSize >= 0
                        ? channelsProvider.getReadChannel(context, parquetFileURI, fileSize)
                        : channelsProvider.getReadChannel(context, parquetFileURI)) {
            final FooterInfo footerInfo = readAndComputeFooterInfo(parquetFileURI, ch);
            ch.position(footerInfo.pos);
            final int sizeHint = footerInfo.len;
            try (final InputStream in =
                    SeekableChannelsProvider.channelPositionInputStream(channelsProvider, ch, sizeHint)) {
                // Ideally, we would be able to get rid of our dependency on the underlying thrift structures, but there
                // is a non-trivial chain of usages stemming from fileMetaData. For now, we will create ParquetMetadata
                // in a two-step process that preserves the thrift structure.
                // metadata = PARQUET_METADATA_CONVERTER.readParquetMetadata(in, ParquetMetadataConverter.NO_FILTER);
                fileMetaData = Util.readFileMetaData(in);
            }
            {
                final long finalPos = ch.position();
                if (finalPos != footerInfo.pos + footerInfo.len) {
                    throw new InvalidParquetFileException(
                            "Footer parsing of '%s' resulted in unexpected channel position: ch.position()=%d, footerInfo=%s"
                                    .formatted(parquetFileURI, finalPos, footerInfo));
                }
            }
        }
        metadata = PARQUET_METADATA_CONVERTER.fromParquetMetadata(fileMetaData);
    }

    /**
     * Read the footer length and back out the footer position based on the channel {@link SeekableByteChannel#size()
     * size}.
     *
     * @return The footer info
     */
    private static FooterInfo readAndComputeFooterInfo(URI parquetFileURI, SeekableByteChannel readChannel)
            throws IOException {
        final long fileLen = readChannel.size();
        if (fileLen < MAGIC.length + FOOTER_LENGTH_SIZE + MAGIC.length) { // MAGIC + data + footer +
            // footerIndex + MAGIC
            throw new InvalidParquetFileException(
                    parquetFileURI + " is not a Parquet file (too small length: " + fileLen + ")");
        }
        final byte[] trailer = new byte[Integer.BYTES + MAGIC.length];
        final long footerLengthIndex = fileLen - FOOTER_LENGTH_SIZE - MAGIC.length;
        readChannel.position(footerLengthIndex);
        Helpers.readBytes(readChannel, trailer);
        if (!Arrays.equals(MAGIC, 0, MAGIC.length, trailer, Integer.BYTES, trailer.length)) {
            throw new InvalidParquetFileException(
                    parquetFileURI + " is not a Parquet file. expected magic number at tail " + Arrays.toString(MAGIC)
                            + " but found "
                            + Arrays.toString(Arrays.copyOfRange(trailer, Integer.BYTES, trailer.length)));
        }
        final int footerLength = makeLittleEndianInt(trailer[0], trailer[1], trailer[2], trailer[3]);
        final long footerIndex = footerLengthIndex - footerLength;
        if (footerIndex < MAGIC.length || footerIndex >= footerLengthIndex) {
            throw new InvalidParquetFileException(
                    "corrupted file: the footer index is not within the file: " + footerIndex);
        }
        return new FooterInfo(footerIndex, footerLength);
    }

    private static class FooterInfo {
        private final long pos;
        private final int len;

        FooterInfo(long pos, int len) {
            this.pos = pos;
            this.len = len;
        }
    }

    private static int makeLittleEndianInt(byte b0, byte b1, byte b2, byte b3) {
        return (b0 & 0xff) | ((b1 & 0xff) << 8) | ((b2 & 0xff) << 16) | ((b3 & 0xff) << 24);
    }

    /**
     * @return The {@link SeekableChannelsProvider} used for this reader, appropriate to use for related file access
     */
    public SeekableChannelsProvider getChannelsProvider() {
        return channelsProvider;
    }

    /**
     * Create a {@link RowGroupReader} object for provided row group number
     *
     * @param version The "version" string from deephaven specific parquet metadata, or null if it's not present.
     */
    public RowGroupReader getRowGroup(final int groupNumber, final String version) {
        return new RowGroupReaderImpl(
                fileMetaData.getRow_groups().get(groupNumber),
                channelsProvider,
                rootURI,
                getSchema(),
                getSchema(),
                version);
    }

    public ParquetMetadata getMetadata() {
        return metadata;
    }

    public MessageType getSchema() {
        return metadata.getFileMetaData().getSchema();
    }
}

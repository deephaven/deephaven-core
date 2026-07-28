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
        return impl(parquetFileURI, CachedChannelProvider.create(channelsProvider, 1 << 7), -1);
    }

    public static ParquetFileReader create(
            @NotNull final URI parquetFileURI,
            @NotNull final SeekableChannelsProvider channelsProvider,
            long fileSize) {
        return impl(parquetFileURI, CachedChannelProvider.create(channelsProvider, 1 << 7), fileSize);
    }

    private static URI rootUri(final URI parquetFileURI) {
        return !parquetFileURI.getRawPath().endsWith(".parquet") && FILE_URI_SCHEME.equals(parquetFileURI.getScheme())
                // Construct a new file URI for the parent directory
                ? convertToURI(new File(parquetFileURI).getParentFile(), true)
                : parquetFileURI;
    }

    private static ParquetFileReader impl(final URI parquetFileURI, final SeekableChannelsProvider provider,
            final long fileSize) {
        final FileMetaData fileMetaData;
        final ParquetMetadata metadata;
        try {
            try (
                    final SeekableChannelContext context = provider.makeSingleUseReadContext();
                    final SeekableByteChannel ch = fileSize > 0
                            ? provider.getReadChannel(context, parquetFileURI, fileSize)
                            : provider.getReadChannel(context, parquetFileURI)) {
                fileMetaData = footer(parquetFileURI, ch).readFrom(provider, ch);
            }
            metadata = PARQUET_METADATA_CONVERTER.fromParquetMetadata(fileMetaData);
        } catch (final IOException e) {
            throw new UncheckedIOException("Failed to create Parquet file reader: " + parquetFileURI, e);
        }
        return new ParquetFileReader(fileMetaData, metadata, provider, rootUri(parquetFileURI));
    }

    private record FooterOffset(long offset, int len) {

        public FileMetaData readFrom(final SeekableChannelsProvider provider, final SeekableByteChannel ch)
                throws IOException {
            ch.position(offset);
            final int sizeHint = len;
            final FileMetaData fileMetaData;
            try (final InputStream in = SeekableChannelsProvider.channelPositionInputStream(provider, ch, sizeHint)) {
                // Ideally, we would be able to get rid of our dependency on the underlying thrift structures, but there
                // is a non-trivial chain of usages stemming from fileMetaData. For now, we will create ParquetMetadata
                // in a two-step process that preserves the thrift structure.
                // metadata = PARQUET_METADATA_CONVERTER.readParquetMetadata(in, ParquetMetadataConverter.NO_FILTER);
                fileMetaData = Util.readFileMetaData(in);
            }
            if (ch.position() != offset + len) {
                throw new InvalidParquetFileException("FileMetaData size incorrect");
            }
            return fileMetaData;
        }
    }

    private ParquetFileReader(
            FileMetaData fileMetaData,
            ParquetMetadata metadata,
            SeekableChannelsProvider channelsProvider,
            URI rootURI) {
        this.fileMetaData = Objects.requireNonNull(fileMetaData);
        this.metadata = Objects.requireNonNull(metadata);
        this.channelsProvider = Objects.requireNonNull(channelsProvider);
        this.rootURI = Objects.requireNonNull(rootURI);
    }

    private static FooterOffset footer(final URI parquetFileURI, final SeekableByteChannel readChannel)
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
        return new FooterOffset(footerIndex, footerLength);
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

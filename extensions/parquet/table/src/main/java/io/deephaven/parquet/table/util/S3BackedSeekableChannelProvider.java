package io.deephaven.parquet.table.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.nio.spi.s3.FixedS3ClientProvider;
import software.amazon.nio.spi.s3.S3FileSystem;
import software.amazon.nio.spi.s3.S3FileSystemProvider;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Collections;

public final class S3BackedSeekableChannelProvider implements SeekableChannelsProvider {

    private final S3FileSystemProvider provider;
    private final S3FileSystem fileSystem;
    private final S3AsyncClient s3AsyncClient;
    private final URI uri;

    public S3BackedSeekableChannelProvider(final String awsRegionName, final String uriStr) throws IOException {
        if (awsRegionName == null || awsRegionName.isEmpty()) {
            throw new IllegalArgumentException("awsRegionName cannot be null or empty");
        }
        if (uriStr == null || uriStr.isEmpty()) {
            throw new IllegalArgumentException("uri cannot be null or empty");
        }
        try {
            uri = new URI(uriStr);
        } catch (final URISyntaxException e) {
            throw new UncheckedDeephavenException("Failed to parse URI " + uriStr, e);
        }
        final String bucket = uri.getHost();
        provider = new S3FileSystemProvider();
        S3FileSystem tempFileSystem;
        try {
            tempFileSystem = provider.newFileSystem(URI.create("s3://" + bucket));
        } catch (final FileSystemAlreadyExistsException e) {
            tempFileSystem = provider.getFileSystem(URI.create("s3://" + bucket));
        }
        fileSystem = tempFileSystem;
        s3AsyncClient = S3AsyncClient.builder()
                .region(Region.of(awsRegionName))
                .build();
        fileSystem.clientProvider(new FixedS3ClientProvider(s3AsyncClient));
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final Path path) throws IOException {
        return provider.newByteChannel(Paths.get(uri), Collections.singleton(StandardOpenOption.READ));
    }

    @Override
    public SeekableByteChannel getWriteChannel(@NotNull final Path path, final boolean append)
            throws UnsupportedEncodingException {
        throw new UnsupportedEncodingException("Don't support writing to S3 yet");
    }

    public void close() throws IOException {
        fileSystem.close();
        s3AsyncClient.close();
    }
}

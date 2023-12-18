package io.deephaven.parquet.table.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.configuration.Configuration;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;

public final class S3BackedSeekableChannelProvider implements SeekableChannelsProvider {

    private final S3AsyncClient s3AsyncClient;
    private final URI uri;
    private final String s3uri, bucket, key;

    public static final int MAX_FRAGMENT_SIZE =
            Configuration.getInstance().getIntegerWithDefault("s3.spi.read.max-fragment-size", 512 * 1024); // 512 KB
    private static final int MAX_FRAGMENT_NUMBER =
            Configuration.getInstance().getIntegerWithDefault("s3.spi.read.max-fragment-number", 2);

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
        this.s3uri = uriStr;
        this.bucket = uri.getHost();
        this.key = uri.getPath().substring(1);
        s3AsyncClient = S3AsyncClient.builder()
                .region(Region.of(awsRegionName))
                .build();
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final Path path) throws IOException {
        return new S3SeekableByteChannel(s3uri, bucket, key, s3AsyncClient, 0, MAX_FRAGMENT_SIZE, MAX_FRAGMENT_NUMBER,
                null, null);
    }

    @Override
    public SeekableByteChannel getWriteChannel(@NotNull final Path path, final boolean append)
            throws UnsupportedEncodingException {
        throw new UnsupportedEncodingException("Don't support writing to S3 yet");
    }

    public void close() throws IOException {
        s3AsyncClient.close();
    }
}

package io.deephaven.parquet.table.util;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.AmazonS3URI;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import io.deephaven.UncheckedDeephavenException;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;

import java.io.UnsupportedEncodingException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;

public final class S3BackedSeekableChannelProvider implements SeekableChannelsProvider {
    /**
     * The maximum number of open HTTP connections.
     */
    private static final int MAX_HTTP_CONNECTIONS = 50;

    /**
     * Shared s3Client object which maintains a pool of HTTP connections to S3 and can be used across threads.
     */
    private final AmazonS3 s3Client;
    private final AmazonS3URI s3URI;

    /**
     * The size of the associated object in bytes.
     */
    private final long size;

    public S3BackedSeekableChannelProvider(final String awsRegionName, final String uri) {
        if (awsRegionName == null || awsRegionName.isEmpty()) {
            throw new IllegalArgumentException("awsRegionName cannot be null or empty");
        }
        if (uri == null || uri.isEmpty()) {
            throw new IllegalArgumentException("uri cannot be null or empty");
        }
        // TODO There are a many other config options. Discuss and decide the values.
        final ClientConfiguration clientConfig = new ClientConfiguration().withMaxConnections(MAX_HTTP_CONNECTIONS);

        // Following will automatically read credentials from aws credentials file in "~/.aws/credentials"
        s3Client = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.fromName(awsRegionName))
                .withClientConfiguration(clientConfig)
                .build();
        s3URI = new AmazonS3URI(uri);
        try {
            // TODO THis will send a HEAD request to S3. Discuss and decide if we want to do this right now or later
            // when we make the first channel. I wanted to keep it here since channel creation can be parallel and
            // we are sure that a channel will be made for this URI when we fetch in the metadata.
            size = s3Client.getObjectMetadata(s3URI.getBucket(), s3URI.getKey()).getContentLength();
        } catch (final AmazonS3Exception e) {
            if (e.getStatusCode() == 404) {
                throw new UncheckedDeephavenException(
                        "Object " + uri + " in region " + awsRegionName + " does not exist", e);
            }
            throw new UncheckedDeephavenException("Failed to get object metadata for " + uri + " in region "
                    + awsRegionName + ". Please verify all inputs along with credentials are accurate. Refer the error "
                    + "status code for more details", e);
        }
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final Path path) {
        return new S3BackedSeekableByteChannel(s3Client, s3URI, size);
    }

    @Override
    public SeekableByteChannel getWriteChannel(@NotNull final Path path, final boolean append)
            throws UnsupportedEncodingException {
        throw new UnsupportedEncodingException("Don't support writing to S3 yet");
    }
}

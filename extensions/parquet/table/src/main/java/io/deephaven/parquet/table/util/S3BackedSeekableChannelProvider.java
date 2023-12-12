package io.deephaven.parquet.table.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Uri;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
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
    private final S3Client s3Client;
    private final S3Uri s3URI;

    /**
     * The size of the associated object in bytes.
     */
    private final long size;

    public S3BackedSeekableChannelProvider(final String awsRegionName, final String uriStr) {
        if (awsRegionName == null || awsRegionName.isEmpty()) {
            throw new IllegalArgumentException("awsRegionName cannot be null or empty");
        }
        if (uriStr == null || uriStr.isEmpty()) {
            throw new IllegalArgumentException("uri cannot be null or empty");
        }
        // TODO Number of other options also available, discuss with others and decide if we want to change any
        final SdkHttpClient httpClient = ApacheHttpClient.builder()
                .maxConnections(MAX_HTTP_CONNECTIONS)
                .build();

        // Following will automatically read credentials from aws credentials file in "~/.aws/credentials"
        s3Client = S3Client.builder()
                .region(Region.of(awsRegionName))
                .httpClient(httpClient)
                .build();
        final URI uri;
        try {
            uri = new URI(uriStr);
        } catch (final URISyntaxException e) {
            throw new UncheckedDeephavenException("Failed to parse URI " + uriStr, e);
        }
        s3URI = S3Uri.builder().uri(uri).bucket(uri.getHost()).key(uri.getPath().substring(1)).build();
        try {
            // Discuss and decide if we want to do this right now or later
            // when we make the first channel. I wanted to keep it here since channel creation can be done by multiple
            // threads but this object creation is only in single thread. And we are sure that a channel will be made
            // for this URI when we fetch in the metadata.
            // Also, I am using a synchronous client here, there is also an async client, should we use that instead?
            final HeadObjectRequest headObjectRequest = HeadObjectRequest.builder()
                    .bucket(s3URI.bucket().get())
                    .key(s3URI.key().get())
                    .build();
            size = s3Client.headObject(headObjectRequest).contentLength().longValue();
        } catch (final NoSuchKeyException e) {
            if (e.statusCode() == 404) {
                throw new UncheckedDeephavenException("Object " + uri + " in region " + awsRegionName +
                        " does not exist", e);
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

    public void close() throws IOException {
        s3Client.close();
    }
}

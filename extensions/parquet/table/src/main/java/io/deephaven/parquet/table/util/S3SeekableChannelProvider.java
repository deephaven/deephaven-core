package io.deephaven.parquet.table.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.configuration.Configuration;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class S3SeekableChannelProvider implements SeekableChannelsProvider {

    private final S3AsyncClient s3AsyncClient;
    private final URI uri;
    private final String s3uri, bucket, key;
    private final long size;
    private final Map<Long, ChannelContext> contextMap = new HashMap<>();

    private static final int MAX_AWS_CONCURRENT_REQUESTS =
            Configuration.getInstance().getIntegerWithDefault("s3.spi.read.max-concurrency", 20);

    public S3SeekableChannelProvider(final String awsRegionName, final String uriStr) throws IOException {
        if (awsRegionName == null || awsRegionName.isEmpty()) {
            throw new IllegalArgumentException("awsRegionName cannot be null or empty");
        }
        if (uriStr == null || uriStr.isEmpty()) {
            throw new IllegalArgumentException("uri cannot be null or empty");
        }
        if (MAX_AWS_CONCURRENT_REQUESTS < 1) {
            throw new IllegalArgumentException("maxConcurrency must be >= 1");
        }

        try {
            uri = new URI(uriStr);
        } catch (final URISyntaxException e) {
            throw new UncheckedDeephavenException("Failed to parse URI " + uriStr, e);
        }

        final SdkAsyncHttpClient asyncHttpClient = AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(MAX_AWS_CONCURRENT_REQUESTS)
                .connectionTimeout(Duration.ofSeconds(5))
                .build();
        s3AsyncClient = S3AsyncClient.builder()
                .region(Region.of(awsRegionName))
                .httpClient(asyncHttpClient)
                .build();

        this.s3uri = uriStr;
        this.bucket = uri.getHost();
        this.key = uri.getPath().substring(1);
        // Send HEAD request to S3 to get the size of the file
        {
            final long timeOut = 1L;
            final TimeUnit unit = TimeUnit.MINUTES;

            final HeadObjectResponse headObjectResponse;
            try {
                headObjectResponse = s3AsyncClient.headObject(builder -> builder
                        .bucket(bucket)
                        .key(key)).get(timeOut, unit);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            } catch (final ExecutionException | TimeoutException e) {
                throw new IOException(e);
            }
            this.size = headObjectResponse.contentLength();
        }
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final SeekableChannelsProvider.ChannelContext context,
            @NotNull final Path path) throws IOException {
        // Ignore the context provided here, will be set properly before reading
        return new S3SeekableByteChannel(context, s3uri, bucket, key, s3AsyncClient, 0, size);
    }

    @Override
    public ChannelContext makeContext() {
        final Long tid = Long.valueOf(Thread.currentThread().getId());
        if (contextMap.containsKey(tid)) {
            return contextMap.get(tid);
        } else {
            final ChannelContext context;
            synchronized (contextMap) {
                if (contextMap.containsKey(tid)) {
                    return contextMap.get(tid);
                }
                context = new S3SeekableByteChannel.ChannelContext(S3SeekableByteChannel.READ_AHEAD_COUNT,
                        S3SeekableByteChannel.MAX_CACHE_SIZE);
                contextMap.put(tid, context);
            }
            return context;
        }
    }

    @Override
    public SeekableByteChannel getWriteChannel(@NotNull final Path path, final boolean append)
            throws UnsupportedEncodingException {
        throw new UnsupportedEncodingException("Don't support writing to S3 yet");
    }

    public void close() throws IOException {
        s3AsyncClient.close();
        synchronized (contextMap) {
            for (final ChannelContext context : contextMap.values()) {
                context.close();
            }
            contextMap.clear();
        }
    }
}

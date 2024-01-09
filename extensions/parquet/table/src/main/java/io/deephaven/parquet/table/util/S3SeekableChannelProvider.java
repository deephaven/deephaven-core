package io.deephaven.parquet.table.util;

import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import io.deephaven.parquet.table.S3Instructions;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

/**
 * {@link SeekableChannelsProvider} implementation that is used to fetch objects from AWS S3 instances.
 */
public final class S3SeekableChannelProvider implements SeekableChannelsProvider {

    private final S3AsyncClient s3AsyncClient;
    private final int fragmentSize;
    private final int maxCacheSize;
    private final int readAheadCount;
    private final Duration readTimeout;
    private final Map<Long, ChannelContext> contextMap = new HashMap<>(); // TODO Remove this

    public S3SeekableChannelProvider(final S3Instructions s3Instructions) {
        final SdkAsyncHttpClient asyncHttpClient = AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(Integer.valueOf(s3Instructions.maxConcurrentRequests()))
                .connectionTimeout(s3Instructions.connectionTimeout())
                .build();
        // TODO Should we cache and reuse the client object?
        this.s3AsyncClient = S3AsyncClient.builder()
                .region(Region.of(s3Instructions.awsRegionName()))
                .httpClient(asyncHttpClient)
                .build();
        this.fragmentSize = s3Instructions.fragmentSize();
        this.maxCacheSize = s3Instructions.maxCacheSize();
        this.readAheadCount = s3Instructions.readAheadCount();
        this.readTimeout = s3Instructions.readTimeout();
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final SeekableChannelsProvider.ChannelContext context,
            @NotNull final URI uri) {
        return new S3SeekableByteChannel(context, uri, s3AsyncClient, fragmentSize, readAheadCount, readTimeout);
    }

    @Override
    public ChannelContext makeContext() {
        final Long tid = Long.valueOf(Thread.currentThread().getId());
        if (contextMap.containsKey(tid)) {
            return contextMap.get(tid);
        } else {
            final ChannelContext context;
            // TODO Remove this part
            synchronized (contextMap) {
                if (contextMap.containsKey(tid)) {
                    return contextMap.get(tid);
                }
                context = new S3SeekableByteChannel.S3ChannelContext(maxCacheSize);
                contextMap.put(tid, context);
            }
            return context;
        }
    }

    @Override
    public SeekableByteChannel getWriteChannel(@NotNull final Path path, final boolean append) {
        throw new UnsupportedOperationException("Don't support writing to S3 yet");
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

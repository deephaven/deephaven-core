package io.deephaven.parquet.table.util;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.base.verify.Assert;
import io.deephaven.parquet.base.util.SeekableChannelsProvider;
import io.deephaven.parquet.table.ParquetInstructions;
import io.deephaven.parquet.table.S3ParquetInstructions;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.http.async.SdkAsyncHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.http.crt.AwsCrtAsyncHttpClient;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static io.deephaven.parquet.table.util.S3SeekableByteChannel.CONNECTION_TIMEOUT_MINUTES;
import static io.deephaven.parquet.table.util.S3SeekableByteChannel.CONNECTION_TIMEOUT_UNIT;

/**
 * {@link SeekableChannelsProvider} implementation that is used to fetch objects from AWS S3 instances.
 */
public final class S3SeekableChannelProvider implements SeekableChannelsProvider {

    private final S3AsyncClient s3AsyncClient;

    /**
     * Parquet file {@link URI} stored as a {@link Path} to save on parsing time at the time of constructing new read
     * channels. Note that this conversion is lossy, as the {@link URI} will contain characters "s3://" but {@link Path}
     * will have "s3:/".
     */
    private final Path parquetFilePath;
    private final String bucket, key;
    private final long size;
    private final int fragmentSize;
    private final int maxCacheSize;
    private final int readAheadCount;
    private final Map<Long, ChannelContext> contextMap = new HashMap<>(); // TODO Remove this


    public S3SeekableChannelProvider(final URI parquetFileURI, final ParquetInstructions readInstructions) {
        if (!(readInstructions.getSpecialInstructions() instanceof S3ParquetInstructions)) {
            throw new IllegalArgumentException("Must provide S3ParquetInstructions to read files from S3");
        }
        final S3ParquetInstructions s3Instructions = (S3ParquetInstructions) readInstructions.getSpecialInstructions();
        final String awsRegionName = s3Instructions.awsRegionName();
        final int maxConcurrentRequests = s3Instructions.maxConcurrentRequests();
        final SdkAsyncHttpClient asyncHttpClient = AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(maxConcurrentRequests)
                .connectionTimeout(Duration.ofMinutes(CONNECTION_TIMEOUT_MINUTES))
                .build();
        this.s3AsyncClient = S3AsyncClient.builder()
                .region(Region.of(awsRegionName))
                .httpClient(asyncHttpClient)
                .build();

        this.fragmentSize = s3Instructions.fragmentSize();
        this.maxCacheSize = s3Instructions.maxCacheSize();
        this.readAheadCount = s3Instructions.readAheadCount();

        this.parquetFilePath = Path.of(parquetFileURI.toString());
        this.bucket = parquetFileURI.getHost();
        this.key = parquetFileURI.getPath().substring(1);
        // Send a blocking HEAD request to S3 to get the size of the file
        final HeadObjectResponse headObjectResponse;
        try {
            headObjectResponse = s3AsyncClient.headObject(
                    builder -> builder.bucket(bucket).key(key))
                    .get(CONNECTION_TIMEOUT_MINUTES, CONNECTION_TIMEOUT_UNIT);
        } catch (final InterruptedException | ExecutionException | TimeoutException | RuntimeException e) {
            throw new UncheckedDeephavenException("Failed to fetch HEAD for file " + parquetFileURI, e);
        }
        this.size = headObjectResponse.contentLength();
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull final SeekableChannelsProvider.ChannelContext context,
            @NotNull final Path path) {
        Assert.equals(parquetFilePath, "parquetFilePath", path, "path");
        return new S3SeekableByteChannel(context, bucket, key, s3AsyncClient, size, fragmentSize, readAheadCount);
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
                context = new S3SeekableByteChannel.ChannelContext(readAheadCount, maxCacheSize);
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

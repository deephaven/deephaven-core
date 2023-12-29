package io.deephaven.parquet.table.util;

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
import java.io.UnsupportedEncodingException;
import java.net.URI;
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
    private final String bucket, key;
    private final long size;
    private final int maxConcurrentRequests;
    private final int fragmentSize;
    private final int maxCacheSize;
    private final int readAheadCount;
    private final Map<Long, ChannelContext> contextMap = new HashMap<>();


    public S3SeekableChannelProvider(final URI parquetFileURI, final ParquetInstructions readInstructions)
            throws IOException {
        if (!(readInstructions.getSpecialInstructions() instanceof S3ParquetInstructions)) {
            throw new IllegalArgumentException("Must provide S3ParquetInstructions to read files from S3");
        }
        final S3ParquetInstructions s3Instructions = (S3ParquetInstructions) readInstructions.getSpecialInstructions();
        final String awsRegionName = s3Instructions.awsRegionName();
        maxConcurrentRequests = s3Instructions.maxConcurrentRequests();
        final SdkAsyncHttpClient asyncHttpClient = AwsCrtAsyncHttpClient.builder()
                .maxConcurrency(maxConcurrentRequests)
                .connectionTimeout(Duration.ofSeconds(5))
                .build();
        s3AsyncClient = S3AsyncClient.builder()
                .region(Region.of(awsRegionName))
                .httpClient(asyncHttpClient)
                .build();

        fragmentSize = s3Instructions.fragmentSize();
        maxCacheSize = s3Instructions.maxCacheSize();
        readAheadCount = s3Instructions.readAheadCount();

        this.bucket = parquetFileURI.getHost();
        this.key = parquetFileURI.getPath().substring(1);
        // Send HEAD request to S3 to get the size of the file
        {
            final long timeOut = 1L;
            final TimeUnit unit = TimeUnit.MINUTES;

            final HeadObjectResponse headObjectResponse;
            try {
                headObjectResponse = s3AsyncClient.headObject(
                        builder -> builder.bucket(bucket).key(key)).get(timeOut, unit);
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
            @NotNull final Path path) {
        // Ignore the context provided here, will be set properly before reading
        return new S3SeekableByteChannel(context, bucket, key, s3AsyncClient, size, fragmentSize, readAheadCount);
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
                context = new S3SeekableByteChannel.ChannelContext(readAheadCount, maxCacheSize);
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

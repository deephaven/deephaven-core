//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.UncheckedDeephavenException;
import io.deephaven.test.types.OutOfBandTest;
import io.deephaven.util.channel.CachedChannelProvider;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderPlugin;
import org.junit.*;

import org.junit.experimental.categories.Category;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteBucketRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.stream.Stream;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@Category(OutOfBandTest.class)
abstract class S3SeekableChannelTestBase {

    public abstract S3Client s3Client();

    public abstract S3Instructions.Builder s3Instructions(S3Instructions.Builder builder);

    private S3Client client;

    private String bucket;

    private final Collection<String> keys = new ArrayList<>();

    void setUp() {
        bucket = UUID.randomUUID().toString();
        client = s3Client();
        client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
    }

    void tearDown() {
        for (final String key : keys) {
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
        }
        keys.clear();
        client.deleteBucket(DeleteBucketRequest.builder().bucket(bucket).build());
        client.close();
    }

    @Test
    public void readEmptyFile() throws IOException {
        putObject("empty.txt", RequestBody.empty());
        final URI uri = uri("empty.txt");
        final ByteBuffer buffer = ByteBuffer.allocate(1);
        try (
                final SeekableChannelsProvider providerImpl = providerImpl(uri);
                final SeekableChannelsProvider provider = new CachedChannelProvider(providerImpl, 32);
                final SeekableChannelContext context = provider.makeContext();
                final SeekableByteChannel readChannel = provider.getReadChannel(context, uri)) {
            assertThat(readChannel.read(buffer)).isEqualTo(-1);
        }
    }

    @Test
    public void read32MiB() throws IOException {
        final int numBytes = 33554432;
        putObject("32MiB.bin", RequestBody.fromInputStream(new InputStream() {
            @Override
            public int read() {
                return 42;
            }
        }, numBytes));
        final URI uri = uri("32MiB.bin");
        final ByteBuffer buffer = ByteBuffer.allocate(1);
        try (
                final SeekableChannelsProvider providerImpl = providerImpl(uri);
                final SeekableChannelsProvider provider = new CachedChannelProvider(providerImpl, 32);
                final SeekableChannelContext context = provider.makeContext();
                final SeekableByteChannel readChannel = provider.getReadChannel(context, uri)) {
            for (long p = 0; p < numBytes; ++p) {
                assertThat(readChannel.read(buffer)).isEqualTo(1);
                assertThat(buffer.get(0)).isEqualTo((byte) 42);
                buffer.clear();
            }
            assertThat(readChannel.read(buffer)).isEqualTo(-1);
        }
    }

    URI uri(String key) {
        return URI.create(String.format("s3://%s/%s", bucket, key));
    }

    void putObject(String key, RequestBody body) {
        client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).build(), body);
        keys.add(key);
    }

    void putDirectory(final File directory) {
        final String directoryName = directory.getName();
        final Path directoryPath = directory.toPath();
        try (final Stream<Path> paths = Files.walk(directoryPath)) {
            paths.filter(Files::isRegularFile)
                    .forEach(file -> {
                        final String key = directoryName + "/" + directoryPath.relativize(file);
                        putObject(key, RequestBody.fromFile(file.toFile()));
                    });
        } catch (final IOException e) {
            throw new UncheckedDeephavenException("Failed to walk directory: " + directory, e);
        }
    }

    private SeekableChannelsProvider providerImpl(URI uri) {
        final SeekableChannelsProviderPlugin plugin = new S3SeekableChannelProviderPlugin();
        final S3Instructions instructions = s3Instructions(S3Instructions.builder()).build();
        return plugin.createProvider(uri, instructions);
    }
}

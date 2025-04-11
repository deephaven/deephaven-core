//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.util.channel.CompletableOutputStream;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProvider;
import io.deephaven.util.channel.SeekableChannelsProviderDelegate;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;
import java.util.stream.Stream;

import static io.deephaven.base.FileUtils.convertToURI;
import static io.deephaven.extensions.s3.S3Constants.S3A_URI_SCHEME;
import static io.deephaven.extensions.s3.S3Constants.S3N_URI_SCHEME;
import static io.deephaven.extensions.s3.S3Constants.S3_URI_SCHEME;

@SuppressWarnings("resource")
final class UniversalS3Provider extends SeekableChannelsProviderDelegate {

    private final SeekableChannelsProvider s3;
    private final SeekableChannelsProvider s3a;
    private final SeekableChannelsProvider s3n;

    UniversalS3Provider(
            S3SeekableChannelProvider delegate,
            S3SeekableChannelProvider s3,
            S3DelegateProvider s3a,
            S3DelegateProvider s3n) {
        super(Objects.requireNonNull(delegate));
        this.s3 = s3;
        this.s3a = s3a;
        this.s3n = s3n;
    }

    private SeekableChannelsProvider of(final String scheme) {
        switch (scheme) {
            case S3_URI_SCHEME:
                if (s3 == null) {
                    break;
                }
                return s3;
            case S3A_URI_SCHEME:
                if (s3a == null) {
                    break;
                }
                return s3a;
            case S3N_URI_SCHEME:
                if (s3n == null) {
                    break;
                }
                return s3n;
        }
        throw new IllegalArgumentException("Unexpected scheme: " + scheme);
    }

    @Override
    public boolean exists(@NotNull URI uri) {
        return of(uri.getScheme()).exists(uri);
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull URI uri)
            throws IOException {
        return of(uri.getScheme()).getReadChannel(channelContext, uri);
    }

    @Override
    public CompletableOutputStream getOutputStream(
            @NotNull final WriteContext channelContext,
            @NotNull URI uri,
            int bufferSizeHint) throws IOException {
        return of(uri.getScheme()).getOutputStream(channelContext, uri, bufferSizeHint);
    }

    @Override
    public Stream<URI> list(@NotNull URI directory) throws IOException {
        return of(directory.getScheme()).list(directory);
    }

    @Override
    public Stream<URI> walk(@NotNull URI directory) throws IOException {
        return of(directory.getScheme()).walk(directory);
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull String uriStr)
            throws IOException {
        final URI uri = convertToURI(uriStr, false);
        // Note: delegating to the URI version instead of the uriStr version
        return of(uri.getScheme()).getReadChannel(channelContext, uri);
    }

    @Override
    public String toString() {
        return "UniversalS3Provider{" +
                "delegate=" + delegate +
                ",s3=" + (s3 != null) +
                ",s3a=" + (s3a != null) +
                ",s3n=" + (s3n != null) +
                '}';
    }
}

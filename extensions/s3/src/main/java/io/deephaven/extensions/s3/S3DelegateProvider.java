//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.util.channel.CompletableOutputStream;
import io.deephaven.util.channel.SeekableChannelContext;
import io.deephaven.util.channel.SeekableChannelsProviderDelegate;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.util.Objects;
import java.util.stream.Stream;

import static io.deephaven.extensions.s3.S3Constants.S3_URI_SCHEME;

final class S3DelegateProvider extends SeekableChannelsProviderDelegate {
    private final String scheme;

    S3DelegateProvider(String scheme, S3SeekableChannelProvider delegate) {
        super(delegate);
        if (S3_URI_SCHEME.equals(scheme)) {
            throw new IllegalArgumentException(
                    String.format("Should not be delegating, use %s directly", S3SeekableChannelProvider.class));
        }
        this.scheme = Objects.requireNonNull(scheme);
    }

    @Override
    public boolean exists(@NotNull URI uri) {
        return delegate.exists(toS3Uri(uri));
    }

    @Override
    public SeekableByteChannel getReadChannel(@NotNull SeekableChannelContext channelContext, @NotNull URI uri)
            throws IOException {
        return delegate.getReadChannel(channelContext, toS3Uri(uri));
    }

    @Override
    public CompletableOutputStream getOutputStream(
            @NotNull final WriteContext channelContext,
            @NotNull URI uri,
            int bufferSizeHint) throws IOException {
        return delegate.getOutputStream(channelContext, toS3Uri(uri), bufferSizeHint);
    }

    @Override
    public Stream<URI> list(@NotNull URI directory) {
        return ((S3SeekableChannelProvider) delegate).createStream(scheme, toS3Uri(directory), false);
    }

    @Override
    public Stream<URI> walk(@NotNull URI directory) {
        return ((S3SeekableChannelProvider) delegate).createStream(scheme, toS3Uri(directory), true);
    }

    @Override
    public String toString() {
        return "S3DelegateProvider{" +
                "scheme='" + scheme + '\'' +
                ", delegate=" + delegate +
                '}';
    }

    private URI toS3Uri(@NotNull final URI uri) {
        if (!scheme.equals(uri.getScheme())) {
            throw new IllegalArgumentException(
                    String.format("Expected uri scheme `%s`, got `%s`", scheme, uri.getScheme()));
        }
        try {
            return new URI(S3_URI_SCHEME, uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(),
                    uri.getQuery(), uri.getFragment());
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Failed to convert URI " + uri + " to s3 URI", e);
        }
    }
}

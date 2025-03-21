//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import io.deephaven.util.channel.CompletableOutputStream;
import io.deephaven.util.channel.SeekableChannelContext;
import org.jetbrains.annotations.NotNull;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.SeekableByteChannel;
import java.util.stream.Stream;

import static io.deephaven.extensions.s3.S3SeekableChannelProviderPlugin.S3_URI_SCHEME;

/**
 * Base class for all seekable channel providers with URI schemes that can be derived from S3, like S3A, S3N, etc.
 */
class UriToS3SeekableChannelProvider extends S3SeekableChannelProvider {

    private static final Logger log = LoggerFactory.getLogger(UriToS3SeekableChannelProvider.class);

    UriToS3SeekableChannelProvider(@NotNull final S3Instructions s3Instructions,
            @NotNull final String childScheme) {
        super(s3Instructions, childScheme);
    }

    @Override
    public boolean exists(@NotNull final URI uri) {
        return super.exists(toS3Uri(uri));
    }

    @Override
    public SeekableByteChannel getReadChannel(
            @NotNull final SeekableChannelContext channelContext,
            @NotNull final URI uri) {
        return super.getReadChannel(channelContext, toS3Uri(uri));
    }

    @Override
    public CompletableOutputStream getOutputStream(
            @NotNull final SeekableChannelContext channelContext,
            @NotNull final URI uri,
            final int bufferSizeHint) {
        return super.getOutputStream(channelContext, toS3Uri(uri), bufferSizeHint);
    }

    @Override
    public Stream<URI> list(@NotNull final URI directory) {
        if (log.isDebugEnabled()) {
            log.debug().append("Fetching child URIs for directory: ").append(directory.toString()).endl();
        }
        return createStream(toS3Uri(directory), false);
    }

    @Override
    public Stream<URI> walk(@NotNull final URI directory) {
        if (log.isDebugEnabled()) {
            log.debug().append("Performing recursive traversal from directory: ").append(directory.toString()).endl();
        }
        return createStream(toS3Uri(directory), true);
    }

    private static URI toS3Uri(@NotNull final URI uri) {
        try {
            if (S3_URI_SCHEME.equals(uri.getScheme())) {
                return uri;
            }
            return new URI(S3_URI_SCHEME, uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(),
                    uri.getQuery(), uri.getFragment());
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Failed to convert URI " + uri + " to s3 URI", e);
        }
    }
}

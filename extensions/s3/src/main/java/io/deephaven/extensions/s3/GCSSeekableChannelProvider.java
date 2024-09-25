//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
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

import static io.deephaven.extensions.s3.GCSSeekableChannelProviderPlugin.GCS_URI_SCHEME;
import static io.deephaven.extensions.s3.S3SeekableChannelProviderPlugin.S3_URI_SCHEME;

final class GCSSeekableChannelProvider extends S3SeekableChannelProvider {

    private static final Logger log = LoggerFactory.getLogger(GCSSeekableChannelProvider.class);

    GCSSeekableChannelProvider(@NotNull final S3Instructions s3Instructions) {
        super(s3Instructions);
    }

    @Override
    public boolean exists(@NotNull final URI uri) {
        return super.exists(gcsToS3Uri(uri));
    }

    @Override
    public SeekableByteChannel getReadChannel(
            @NotNull final SeekableChannelContext channelContext,
            @NotNull final URI uri) {
        return super.getReadChannel(channelContext, gcsToS3Uri(uri));
    }

    @Override
    public CompletableOutputStream getOutputStream(@NotNull final URI uri, final int bufferSizeHint) {
        return super.getOutputStream(gcsToS3Uri(uri), bufferSizeHint);
    }

    @Override
    public Stream<URI> list(@NotNull final URI directory) {
        if (log.isDebugEnabled()) {
            log.debug().append("Fetching child URIs for directory: ").append(directory.toString()).endl();
        }
        return createStream(gcsToS3Uri(directory), false, GCS_URI_SCHEME);
    }

    @Override
    public Stream<URI> walk(@NotNull final URI directory) {
        if (log.isDebugEnabled()) {
            log.debug().append("Performing recursive traversal from directory: ").append(directory.toString()).endl();
        }
        return createStream(gcsToS3Uri(directory), true, GCS_URI_SCHEME);
    }

    private static URI gcsToS3Uri(@NotNull final URI uri) {
        try {
            if (S3_URI_SCHEME.equals(uri.getScheme())) {
                return uri;
            }
            return new URI(S3_URI_SCHEME, uri.getUserInfo(), uri.getHost(), uri.getPort(), uri.getPath(),
                    uri.getQuery(), uri.getFragment());
        } catch (final URISyntaxException e) {
            throw new IllegalArgumentException("Failed to convert GCS URI " + uri + " to s3 URI", e);
        }
    }
}

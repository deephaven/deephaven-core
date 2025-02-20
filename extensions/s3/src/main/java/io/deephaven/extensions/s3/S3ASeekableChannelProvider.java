//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.jetbrains.annotations.NotNull;

import static io.deephaven.extensions.s3.S3ASeekableChannelProviderPlugin.S3A_URI_SCHEME;

final class S3ASeekableChannelProvider extends UriToS3SeekableChannelProvider {

    S3ASeekableChannelProvider(@NotNull final S3Instructions s3Instructions) {
        super(s3Instructions, S3A_URI_SCHEME);
    }
}

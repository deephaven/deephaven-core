//
// Copyright (c) 2016-2025 Deephaven Data Labs and Patent Pending
//
// ****** AUTO-GENERATED CLASS - DO NOT EDIT MANUALLY
// ****** Edit S3ASeekableChannelProvider and run "./gradlew replicateChannelProviders" to regenerate
//
// @formatter:off
package io.deephaven.extensions.s3;

import org.jetbrains.annotations.NotNull;

import static io.deephaven.extensions.s3.GCSSeekableChannelProviderPlugin.GCS_URI_SCHEME;

final class GCSSeekableChannelProvider extends UriToS3SeekableChannelProvider {

    GCSSeekableChannelProvider(@NotNull final S3Instructions s3Instructions) {
        super(s3Instructions, GCS_URI_SCHEME);
    }
}

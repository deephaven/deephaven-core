//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.util.annotations.InternalUseOnly;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;

/**
 * Gives async S3 clients built outside {@link S3ClientFactory} the executors it shares across all of its clients, so
 * that each client does not start its own thread pools.
 */
@InternalUseOnly
public final class SharedS3AsyncExecutors {

    private SharedS3AsyncExecutors() {}

    /**
     * Sets the shared scheduled and future-completion executors on {@code builder}, keeping its other overrides. The
     * SDK does not shut these executors down when the client is closed.
     */
    public static void apply(@NotNull final S3AsyncClientBuilder builder) {
        S3ClientFactory.applySharedAsyncExecutors(builder);
    }
}

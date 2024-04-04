//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.s3.S3Uri;

/**
 * A cache for S3 requests, which can be used concurrently.
 */
interface S3RequestCache {
    /**
     * Get the request for the given URI and fragment index if it exists
     *
     * @param uri the URI
     * @param fragmentIndex the fragment index
     * @return the request, or {@code null} if not found
     */
    @Nullable
    S3ChannelContext.Request getRequest(@NotNull final S3Uri uri, final long fragmentIndex);

    /**
     * Get the request for the given URI and fragment index, creating it if it does not exist.
     *
     * @param uri the URI
     * @param fragmentIndex the fragment index
     * @param context the S3 channel context to use for creating the request, if needed
     * @return the request
     */
    @NotNull
    S3ChannelContext.Request getOrCreateRequest(@NotNull final S3Uri uri, final long fragmentIndex,
            @NotNull final S3ChannelContext context);

    /**
     * The maximum number of {@link S3ChannelContext.Request} objects that can be cached.
     */
    int maxSize();

    /**
     * Cancel all outstanding requests and release the objects.
     */
    void cancelAllAndRelease();
}

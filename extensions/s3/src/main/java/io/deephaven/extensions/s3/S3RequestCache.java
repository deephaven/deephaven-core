//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.s3.S3Uri;
import io.deephaven.extensions.s3.S3ChannelContext.Request;

/**
 * A cache for S3 requests, which can be used concurrently.
 */
interface S3RequestCache {
    /**
     * {@link Request#acquire() Acquire} a request for the given URI and fragment index, if it exists in the cache.
     *
     * @param uri the URI
     * @param fragmentIndex the fragment index
     * @return the request, or {@code null} if not found
     */
    @Nullable
    Request getRequest(@NotNull final S3Uri uri, final long fragmentIndex);

    /**
     * {@link Request#acquire() Acquire} a request for the given URI and fragment index, creating it if it does not
     * exist in the cache.
     *
     * @param uri the URI
     * @param fragmentIndex the fragment index
     * @param context the S3 channel context to use for creating the request, if needed
     * @return the request
     */
    @NotNull
    Request getOrCreateRequest(@NotNull final S3Uri uri, final long fragmentIndex,
            @NotNull final S3ChannelContext context);

    /**
     * Remove this request from the cache, if present.
     */
    void remove(@NotNull final Request request);
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.S3Uri;
import io.deephaven.extensions.s3.S3ChannelContext.Request;

/**
 * This class uses a {@link KeyedObjectHashMap} to cache {@link Request} objects based on their URI and fragment index.
 * This cache can be used concurrently.
 */
final class S3RequestCache {

    private static final Logger log = LoggerFactory.getLogger(S3RequestCache.class);

    private final int fragmentSize;
    private final KeyedObjectHashMap<Request.ID, Request> requests;

    /**
     * Create a new cache to hold fragments of the given size.
     */
    S3RequestCache(final int fragmentSize) {
        this.fragmentSize = fragmentSize;
        this.requests = new KeyedObjectHashMap<>(RequestKey.INSTANCE);
    }

    private static final class RequestKey extends KeyedObjectKey.Basic<Request.ID, Request> {

        private static final KeyedObjectKey<Request.ID, Request> INSTANCE = new RequestKey();

        @Override
        public Request.ID getKey(@NotNull final S3ChannelContext.Request request) {
            return request.getId();
        }
    }

    /**
     * @return the size of fragments stored in this cache.
     */
    int getFragmentSize() {
        return fragmentSize;
    }

    /**
     * Acquire a request for the given URI and fragment index, creating and sending a new request it if it does not
     * exist in the cache.
     *
     * @param uri the URI
     * @param fragmentIndex the fragment index
     * @param context the S3 channel context to use for creating the request, if needed
     * @return the request
     */
    @NotNull
    S3ChannelContext.Request.AcquiredRequest getOrCreateRequest(@NotNull final S3Uri uri, final long fragmentIndex,
            @NotNull final S3ChannelContext context) {
        final Request.ID key = new Request.ID(uri, fragmentIndex);
        Request.AcquiredRequest newAcquiredRequest = null;
        for (int retryCount = 0; retryCount < Integer.MAX_VALUE; retryCount++) {
            final Request existingRequest = requests.get(key);
            if (existingRequest != null) {
                final Request.AcquiredRequest acquired = existingRequest.tryAcquire();
                if (acquired != null) {
                    return acquired;
                } else {
                    remove(existingRequest);
                }
            }
            if (newAcquiredRequest == null) {
                newAcquiredRequest = Request.createAndAcquire(fragmentIndex, context);
            }
            final boolean added = requests.putIfAbsent(key, newAcquiredRequest.request) == null;
            if (added && log.isDebugEnabled()) {
                log.debug().append("Adding new request to cache: ").append(String.format("ctx=%d ",
                        System.identityHashCode(context))).append(newAcquiredRequest.request.requestStr()).endl();
            }
        }
        // We have tried to add the request to the cache too many times
        throw new IllegalStateException(
                String.format("Failed to add request to cache: ctx=%d, uri=%s, fragmentIndex=%d",
                        System.identityHashCode(context), uri, fragmentIndex));
    }

    /**
     * Remove this request from the cache, if present.
     */
    void remove(@NotNull final Request request) {
        if (log.isDebugEnabled()) {
            log.debug().append("Clearing request from cache: ").append(request.requestStr()).endl();
        }
        requests.remove(request.getId(), request);
    }

    /**
     * Clear the cache.
     */
    void clear() {
        requests.clear();
    }
}

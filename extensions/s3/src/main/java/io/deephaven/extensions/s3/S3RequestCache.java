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

/**
 * This class uses a {@link KeyedObjectHashMap} to cache {@link S3Request} objects based on their URI and fragment
 * index. This cache can be used concurrently.
 */
final class S3RequestCache {

    private static final Logger log = LoggerFactory.getLogger(S3RequestCache.class);

    private final int fragmentSize;
    private final KeyedObjectHashMap<S3Request.ID, S3Request> requests;

    /**
     * Create a new cache to hold fragments of the given size.
     */
    S3RequestCache(final int fragmentSize) {
        this.fragmentSize = fragmentSize;
        this.requests = new KeyedObjectHashMap<>(RequestKey.INSTANCE);
    }

    private static final class RequestKey extends KeyedObjectKey.Basic<S3Request.ID, S3Request> {

        private static final KeyedObjectKey<S3Request.ID, S3Request> INSTANCE = new RequestKey();

        @Override
        public S3Request.ID getKey(@NotNull final S3Request request) {
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
    S3Request.AcquiredRequest getOrCreateRequest(@NotNull final S3Uri uri, final long fragmentIndex,
            @NotNull final S3ChannelContext context) {
        final S3Request.ID key = new S3Request.ID(uri, fragmentIndex);
        S3Request.AcquiredRequest newAcquiredRequest = null;
        S3Request existingRequest = requests.get(key);
        while (true) {
            if (existingRequest != null) {
                final S3Request.AcquiredRequest acquired = existingRequest.tryAcquire();
                if (acquired != null) {
                    return acquired;
                } else {
                    remove(existingRequest);
                }
            }
            if (newAcquiredRequest == null) {
                newAcquiredRequest = S3Request.createAndAcquire(fragmentIndex, context);
            }
            if ((existingRequest = requests.putIfAbsent(key, newAcquiredRequest.request)) == null) {
                if (log.isDebugEnabled()) {
                    log.debug().append("Added new request to cache: ").append(String.format("ctx=%d ",
                            System.identityHashCode(context))).append(newAcquiredRequest.request.requestStr()).endl();
                }
                return newAcquiredRequest;
            }
            // TODO(deephaven-core#5486): Instead of remove + putIfAbsent pattern, we could have used replace + get
            // pattern, but KeyedObjectHashMap potentially has a bug in replace method.
        }
    }

    /**
     * Remove this request from the cache, if present.
     */
    void remove(@NotNull final S3Request request) {
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

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import io.deephaven.internal.log.LoggerFactory;
import io.deephaven.io.logger.Logger;
import org.apache.commons.lang3.mutable.Mutable;
import org.apache.commons.lang3.mutable.MutableObject;
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
        final Mutable<Request.AcquiredRequest> ret = new MutableObject<>();
        // TODO Need to unwrap the compute to avoid putting() the same result in the map.
        requests.compute(new Request.ID(uri, fragmentIndex), (key, existingRequest) -> {
            if (existingRequest != null) {
                final Request.AcquiredRequest acquiredExisting = existingRequest.tryAcquire();
                if (acquiredExisting != null) {
                    ret.setValue(acquiredExisting);
                    return existingRequest;
                }
            }
            final Request.AcquiredRequest newAcquiredRequest = Request.createAndAcquire(fragmentIndex, context);
            ret.setValue(newAcquiredRequest);
            if (log.isDebugEnabled()) {
                log.debug().append("Adding new request to cache: ").append(String.format("ctx=%d ",
                        System.identityHashCode(context))).append(newAcquiredRequest.request.requestStr()).endl();
            }
            return newAcquiredRequest.request;
        });
        return ret.getValue();
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

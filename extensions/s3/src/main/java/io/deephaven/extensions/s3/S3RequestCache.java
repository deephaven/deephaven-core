//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;
import software.amazon.awssdk.services.s3.S3Uri;
import io.deephaven.extensions.s3.S3ChannelContext.Request;

/**
 * This class uses a ({@link KeyedObjectHashMap}) to cache {@link Request} objects based on their URI and fragment
 * index. This cache can be used concurrently.
 */
final class S3RequestCache {

    private final int fragmentSize;
    private final KeyedObjectHashMap<Request.ID, Request> requests;

    /**
     * Create a new cache to hold fragments of the given size.
     */
    S3RequestCache(final int fragmentSize) {
        this.fragmentSize = fragmentSize;
        requests = new KeyedObjectHashMap<>(new REQUEST_KEY());
    }

    private static final class REQUEST_KEY extends KeyedObjectKey.Basic<Request.ID, Request> {
        @Override
        public Request.ID getKey(@NotNull final S3ChannelContext.Request request) {
            return request.getId();
        }
    }

    /**
     * Return the size of fragments stored in this cache.
     */
    int getFragmentSize() {
        return fragmentSize;
    }

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
            @NotNull final S3ChannelContext context) {
        // TODO Do you think the acquiring part should be done by the caller or here?
        // I kept it here because acquire could potentially fail and the caller would have to call again in a loop,
        // which felt unnecessary compared to a guaranteed acquire.
        return requests.compute(new Request.ID(uri, fragmentIndex), (key, existingRequest) -> {
            if (existingRequest != null) {
                final Request acquired = existingRequest.acquire();
                if (acquired != null) {
                    return acquired;
                }
            }
            final Request newRequest = Request.createAndAcquire(fragmentIndex, context);
            // TODO Do you think the init part should be done by the context, inside createAndAcquire or here?
            // Kept it here for now because caller doesn't know whether request is new or not. So should call init or
            // not. Maybe we can make init more idempotent and the context would call it always.
            newRequest.init();
            return newRequest;
        });
    }

    /**
     * Remove this request from the cache, if present.
     */
    void remove(@NotNull final Request request) {
        requests.remove(request.getId(), request);
    }

    /**
     * Clear the cache.
     */
    void clear() {
        requests.clear();
    }
}

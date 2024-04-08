//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.hash.KeyedLongObjectHashMap;
import io.deephaven.hash.KeyedLongObjectKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.s3.S3Uri;
import io.deephaven.extensions.s3.S3ChannelContext.Request;

import static java.lang.Math.abs;

/**
 * This class uses a modulo-based mapping function with a {@link KeyedLongObjectHashMap} to cache {@link Request}
 * objects.
 */
final class ModuloBasedRequestCache implements S3RequestCache {

    private final class RequestIdKey extends KeyedLongObjectKey.BasicStrict<Request> {
        @Override
        public long getLongKey(@NotNull final Request request) {
            return cacheIndex(request.getUri(), request.getFragmentIndex());
        }
    }

    private final int maxSize;
    private final KeyedLongObjectHashMap<Request> requests;

    ModuloBasedRequestCache(final int maxSize) {
        this.maxSize = maxSize;
        this.requests = new KeyedLongObjectHashMap<>(new RequestIdKey());
    }

    @Override
    @Nullable
    public Request getRequest(@NotNull final S3Uri uri, final long fragmentIndex) {
        final long cacheIdx = cacheIndex(uri, fragmentIndex);
        final Request request = requests.get(cacheIdx);
        return request == null || !request.isFragment(uri, fragmentIndex) ? null : request.acquire();
    }

    @Override
    @NotNull
    public Request getOrCreateRequest(@NotNull final S3Uri uri, final long fragmentIndex,
            @NotNull final S3ChannelContext context) {
        // TODO Do you think the acquiring part should be done by the caller or here?
        final long cacheIdx = cacheIndex(uri, fragmentIndex);
        return requests.compute(cacheIdx, (key, existingRequest) -> {
            if (existingRequest != null && existingRequest.isFragment(uri, fragmentIndex)) {
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

    @Override
    public void remove(@NotNull final Request request) {
        requests.remove(cacheIndex(request.getUri(), request.getFragmentIndex()), request);
    }

    private long cacheIndex(final S3Uri uri, final long fragmentIndex) {
        // TODO(deephaven-core#5061): Experiment with LRU caching
        return (abs(uri.hashCode()) + fragmentIndex) % maxSize;
    }
}

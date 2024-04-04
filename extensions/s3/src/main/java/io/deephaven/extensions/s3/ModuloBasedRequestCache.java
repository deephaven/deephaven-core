//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.s3.S3Uri;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.Math.abs;

/**
 * This class uses a modulo-based mapping function with a {@link ConcurrentHashMap} to cache requests.
 */
final class ModuloBasedRequestCache implements S3RequestCache {

    private final int maxSize;
    private final Map<Integer, S3ChannelContext.Request> requests;

    ModuloBasedRequestCache(final int maxSize) {
        this.maxSize = maxSize;
        this.requests = new ConcurrentHashMap<>(maxSize);
    }

    @Override
    @Nullable
    public S3ChannelContext.Request getRequest(@NotNull final S3Uri uri, final long fragmentIndex) {
        final int cacheIdx = cacheIndex(uri, fragmentIndex);
        final S3ChannelContext.Request request = requests.get(cacheIdx);
        return request == null || !request.isFragment(uri, fragmentIndex) ? null : request;
    }

    @Override
    @NotNull
    public S3ChannelContext.Request getOrCreateRequest(@NotNull final S3Uri uri, final long fragmentIndex,
            @NotNull final S3ChannelContext context) {
        final int cacheIdx = cacheIndex(uri, fragmentIndex);
        return requests.compute(cacheIdx, (key, existingRequest) -> {
            if (existingRequest != null && existingRequest.isFragment(uri, fragmentIndex)) {
                return existingRequest;
            } else {
                // TODO Discuss with Ryan and Devin where this should happen
                // if (existingRequest != null) {
                // existingRequest.release();
                // }
                final S3ChannelContext.Request newRequest = new S3ChannelContext.Request(fragmentIndex, context);
                newRequest.init();
                return newRequest;
            }
        });
    }

    public int maxSize() {
        return maxSize;
    }

    @Override
    public void cancelAllAndRelease() {
        requests.values().forEach(S3ChannelContext.Request::release);
        requests.clear();
    }

    private int cacheIndex(final S3Uri uri, final long fragmentIndex) {
        // TODO(deephaven-core#5061): Experiment with LRU caching
        return (int) ((abs(uri.hashCode()) + fragmentIndex) % maxSize);
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.extensions.s3;

import io.deephaven.hash.KeyedObjectHashMap;
import io.deephaven.hash.KeyedObjectKey;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import software.amazon.awssdk.services.s3.S3Uri;
import io.deephaven.extensions.s3.S3ChannelContext.Request;

/**
 * This class uses a ({@link KeyedObjectHashMap}) to cache {@link Request} objects based on their URI and fragment
 * index.
 */
final class KeyedHashMapBasedRequestCache implements S3RequestCache {

    private final KeyedObjectHashMap<Request.ID, Request> requests;

    KeyedHashMapBasedRequestCache() {
        requests = new KeyedObjectHashMap<>(new KeyedObjectKey.Basic<>() {
            @Override
            public Request.ID getKey(@NotNull final Request request) {
                return request.getId();
            }
        });
    }

    @Override
    @Nullable
    public Request getRequest(@NotNull final S3Uri uri, final long fragmentIndex) {
        final Request request = requests.get(new Request.ID(uri, fragmentIndex));
        return request == null ? null : request.acquire();
    }

    @Override
    @NotNull
    public Request getOrCreateRequest(@NotNull final S3Uri uri, final long fragmentIndex,
            @NotNull final S3ChannelContext context) {
        // TODO Do you think the acquiring part should be done by the caller or here?
        // I kept it here because acquire could potentially fail and the caller would have to call again in a loop,
        // which
        // felt unnecessary compared to a guaranteed acquire.
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

    @Override
    public void remove(@NotNull final Request request) {
        requests.removeValue(request);
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

public class BaseSeekableChannelContext implements SeekableChannelContext {

    /**
     * A sentinel value to indicate that a resource is {@code null}.
     */
    private static final SafeCloseable NULL_SENTINEL = () -> {
    };

    /**
     * An empty cache of resources.
     */
    private static final Map<String, SafeCloseable> EMPTY_CACHE = Map.of();

    /**
     * A cache of opaque resource objects hosted by this context.
     */
    private Map<String, SafeCloseable> resourceCache = EMPTY_CACHE;

    @Override
    @Nullable
    public final <T extends SafeCloseable> T getCachedResource(
            final String key,
            @NotNull final Supplier<T> resourceFactory) {
        SafeCloseable resource;
        if (resourceCache == EMPTY_CACHE) {
            resourceCache = new HashMap<>(1);
            resource = null;
        } else {
            resource = resourceCache.get(key);
            if (resource == NULL_SENTINEL) {
                return null;
            }
        }
        if (resource == null) {
            resourceCache.put(key, (resource = resourceFactory.get()) == null ? NULL_SENTINEL : resource);
        }
        // noinspection unchecked
        return (T) resource;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() {
        SafeCloseable.closeAll(resourceCache.values().iterator());
        resourceCache = EMPTY_CACHE;
    }
}

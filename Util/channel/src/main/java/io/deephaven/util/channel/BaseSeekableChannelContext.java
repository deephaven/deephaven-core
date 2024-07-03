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
     * A cache of opaque resource objects hosted by this context.
     */
    private final Map<String, SafeCloseable> resourceCache = new HashMap<>();

    @Override
    @Nullable
    public final SafeCloseable apply(final String key, @NotNull final Supplier<SafeCloseable> resourceFactory) {
        SafeCloseable resource = resourceCache.get(key);
        if (resource == null) {
            resource = resourceFactory.get();
            if (resource != null) {
                resourceCache.put(key, resource);
            }
        }
        return resource;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() {
        for (final SafeCloseable resource : resourceCache.values()) {
            if (resource != null) {
                resource.close();
            }
        }
    }
}

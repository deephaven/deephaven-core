//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import javax.annotation.OverridingMethodsMustInvokeSuper;
import java.util.function.Supplier;

public class BaseSeekableChannelContext implements SeekableChannelContext {
    /**
     * An opaque resource object hosted by this context.
     */
    private SafeCloseable resource;

    @Override
    @Nullable
    public final SafeCloseable apply(@NotNull final Supplier<SafeCloseable> resourceFactory) {
        if (resource == null) {
            resource = resourceFactory.get();
        }
        return resource;
    }

    @Override
    @OverridingMethodsMustInvokeSuper
    public void close() {
        if (resource != null) {
            resource.close();
            resource = null;
        }
    }
}

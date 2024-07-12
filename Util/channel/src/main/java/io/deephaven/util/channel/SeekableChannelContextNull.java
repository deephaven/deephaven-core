//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import io.deephaven.util.SafeCloseable;
import org.jetbrains.annotations.Nullable;

import java.util.function.Supplier;

enum SeekableChannelContextNull implements SeekableChannelContext {
    NULL_CONTEXT_INSTANCE;

    @Override
    @Nullable
    public <T extends SafeCloseable> T getCachedResource(final String key, final Supplier<T> resourceFactory) {
        return null;
    }
}

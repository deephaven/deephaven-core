/**
 * Copyright (c) 2016-2023 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.util.channel;

import io.deephaven.util.channel.SeekableChannelContext.Provider;

import java.util.Objects;

final class ProviderImpl implements Provider {
    private final SeekableChannelContext context;

    public ProviderImpl(SeekableChannelContext context) {
        this.context = Objects.requireNonNull(context);
    }

    @Override
    public SeekableChannelContext get() {
        return context;
    }

    @Override
    public void close() {
        context.close();
    }
}

//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import io.deephaven.util.channel.SeekableChannelContext.ContextHolder;

import java.util.Objects;

final class ContextHolderImpl implements ContextHolder {
    private final SeekableChannelContext context;

    public ContextHolderImpl(SeekableChannelContext context) {
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

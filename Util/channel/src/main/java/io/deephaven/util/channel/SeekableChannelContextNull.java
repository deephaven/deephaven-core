//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import org.jetbrains.annotations.Nullable;

enum SeekableChannelContextNull implements SeekableChannelContext {
    NULL_CONTEXT_INSTANCE;

    @Override
    public void setResource(final Resource resource) {}

    @Override
    @Nullable
    public <RESOURCE_TYPE extends Resource> RESOURCE_TYPE getResource() {
        return null;
    }
}

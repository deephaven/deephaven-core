//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.util.channel;

import org.jetbrains.annotations.Nullable;

public class SeekableChannelContextDefaultImpl implements SeekableChannelContext {
    /**
     * An opaque resource object hosted by this context.
     */
    private Resource resource;

    @Override
    public void setResource(@Nullable final Resource resource) {
        this.resource = resource;
    }

    @Override
    @Nullable
    final public <RESOURCE_TYPE extends Resource> RESOURCE_TYPE getResource() {
        // noinspection unchecked
        return (RESOURCE_TYPE) resource;
    }
}

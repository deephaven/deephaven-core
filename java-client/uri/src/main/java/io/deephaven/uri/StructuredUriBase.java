/**
 * Copyright (c) 2016-2022 Deephaven Data Labs and Patent Pending
 */
package io.deephaven.uri;

public abstract class StructuredUriBase implements StructuredUri {

    @Override
    public final RemoteUri target(DeephavenTarget target) {
        return RemoteUri.of(target, this);
    }
}

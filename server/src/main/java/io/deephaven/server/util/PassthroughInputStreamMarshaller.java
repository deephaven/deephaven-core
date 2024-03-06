//
// Copyright (c) 2016-2024 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.util;

import io.grpc.MethodDescriptor;

import java.io.InputStream;

public enum PassthroughInputStreamMarshaller implements MethodDescriptor.Marshaller<InputStream> {
    INSTANCE;

    @Override
    public InputStream stream(final InputStream inputStream) {
        return inputStream;
    }

    @Override
    public InputStream parse(final InputStream inputStream) {
        return inputStream;
    }
}

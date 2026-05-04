package io.deephaven.web.client.api.grpc;

import io.grpc.MethodDescriptor;

import java.io.InputStream;

public enum InputStreamMarshaller implements MethodDescriptor.Marshaller<InputStream> {
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


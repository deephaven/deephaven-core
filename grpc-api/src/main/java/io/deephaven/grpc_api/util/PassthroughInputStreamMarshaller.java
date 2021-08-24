package io.deephaven.grpc_api.util;

import io.grpc.MethodDescriptor;

import java.io.InputStream;

public class PassthroughInputStreamMarshaller implements MethodDescriptor.Marshaller<InputStream> {
    public static final PassthroughInputStreamMarshaller INSTANCE =
        new PassthroughInputStreamMarshaller();

    @Override
    public InputStream stream(final InputStream inputStream) {
        return inputStream;
    }

    @Override
    public InputStream parse(final InputStream inputStream) {
        return inputStream;
    }
}

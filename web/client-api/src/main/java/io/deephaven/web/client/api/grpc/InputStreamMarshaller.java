package io.deephaven.web.client.api.grpc;

import io.deephaven.extensions.barrage.DrainableByteArrayInputStream;
import io.grpc.MethodDescriptor;

import java.io.InputStream;

public class InputStreamMarshaller implements MethodDescriptor.Marshaller<InputStream> {
    public static final DrainableByteArrayInputStream empty = new DrainableByteArrayInputStream(new byte[0], 0, 0);

    @Override
    public InputStream stream(final InputStream inputStream) {
        if (inputStream == null) {
            return empty;
        }
        return inputStream;
    }

    @Override
    public InputStream parse(final InputStream inputStream) {
        if (inputStream == null) {
            return empty;
        }
        return inputStream;
    }
}


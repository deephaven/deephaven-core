package io.deephaven.grpc_api.util;

import io.deephaven.grpc_api_client.util.BarrageProtoUtil;
import io.grpc.MethodDescriptor;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class UnaryInputStreamMarshaller implements MethodDescriptor.Marshaller<InputStream> {
    public static UnaryInputStreamMarshaller INSTANCE = new UnaryInputStreamMarshaller();

    @Override
    public InputStream stream(InputStream value) {
        return value;
    }

    @Override
    public InputStream parse(InputStream stream) {
        try (final BarrageProtoUtil.ExposedByteArrayOutputStream baos = new BarrageProtoUtil.ExposedByteArrayOutputStream()) {
            final byte[] buffer = new byte[4096];
            while (stream.available() > 0) {
                int len = stream.read(buffer);
                if (len <= 0) {
                    throw new IOException("failed to read from stream");
                }
                baos.write(buffer, 0, len);
            }
            return new ByteArrayInputStream(baos.peekBuffer(), 0, baos.size());
        } catch (final IOException e) {
            throw new IllegalStateException("Failed to parse barrage message: ", e);
        }
    }
}

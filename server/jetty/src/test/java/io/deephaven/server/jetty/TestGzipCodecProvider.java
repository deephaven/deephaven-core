//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import com.google.auto.service.AutoService;
import io.deephaven.grpc.compression.CompressionCodecProvider;
import io.grpc.Codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Adds a test-only {@code test-gzip} compression type, gRPC's gzip codec under another name, registered as a service as
 * a deployment's own codec would be, so that {@link JettyFlightRoundTripTest} compresses responses and decodes requests
 * with a codec that a {@link CompressionCodecProvider} adds.
 */
@AutoService(CompressionCodecProvider.class)
public final class TestGzipCodecProvider implements CompressionCodecProvider {

    @Override
    public Codec codec() {
        final Codec gzip = new Codec.Gzip();
        return new Codec() {
            @Override
            public String getMessageEncoding() {
                return "test-gzip";
            }

            @Override
            public OutputStream compress(final OutputStream os) throws IOException {
                return gzip.compress(os);
            }

            @Override
            public InputStream decompress(final InputStream is) throws IOException {
                return gzip.decompress(is);
            }
        };
    }
}

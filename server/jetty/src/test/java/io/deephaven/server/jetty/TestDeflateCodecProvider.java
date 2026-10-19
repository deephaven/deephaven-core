//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.server.jetty;

import com.google.auto.service.AutoService;
import io.deephaven.grpc.compression.CompressionCodecProvider;
import io.grpc.Codec;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * Adds a test-only {@code test-deflate} compression type, registered as a service as a deployment's own codec would be,
 * so that {@link JettyFlightRoundTripTest} compresses responses and decodes requests with a codec that a
 * {@link CompressionCodecProvider} adds.
 */
@AutoService(CompressionCodecProvider.class)
public final class TestDeflateCodecProvider implements CompressionCodecProvider {

    @Override
    public Codec codec() {
        return new Codec() {
            @Override
            public String getMessageEncoding() {
                return "test-deflate";
            }

            @Override
            public OutputStream compress(final OutputStream os) {
                return new DeflaterOutputStream(os);
            }

            @Override
            public InputStream decompress(final InputStream is) {
                // like the GZIPInputStream behind gzip, this reports available data until the end of the stream, which
                // Arrow Flight's ArrowMessage needs in order to parse
                return new InflaterInputStream(is);
            }
        };
    }
}

//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import com.google.auto.service.AutoService;
import io.grpc.Codec;

/**
 * Supplies gRPC's own {@link Codec.Gzip gzip} codec as a supported compression type, so that {@link CompressionCodecs}
 * finds it with {@link java.util.ServiceLoader} like every other type.
 */
@AutoService(CompressionCodecProvider.class)
public final class GzipCodecProvider implements DeephavenCompressionCodecProvider {

    @Override
    public Codec codec() {
        return new Codec.Gzip();
    }
}

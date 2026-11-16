//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import com.google.auto.service.AutoService;
import io.grpc.Codec;

import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

/**
 * A gRPC {@link Codec} for the {@code deflate} message encoding: the zlib format, as gRPC Core uses it, from
 * {@code java.util.zip} at its default compression level, the same level as gRPC's {@link Codec.Gzip gzip} codec.
 */
public final class DeflateCodec implements Codec {

    /**
     * The gRPC message encoding name, as it appears in {@code grpc-encoding} and {@code grpc-accept-encoding}.
     */
    public static final String ENCODING = "deflate";

    public static final DeflateCodec INSTANCE = new DeflateCodec();

    private DeflateCodec() {}

    @Override
    public String getMessageEncoding() {
        return ENCODING;
    }

    @Override
    public OutputStream compress(final OutputStream os) {
        return new DeflaterOutputStream(os);
    }

    @Override
    public InputStream decompress(final InputStream is) {
        // reports available data until the end of the stream, which Arrow Flight's ArrowMessage needs in order to parse
        return new InflaterInputStream(is);
    }

    /**
     * Supplies {@link DeflateCodec#INSTANCE} as a supported compression type, so that {@link CompressionCodecs} finds
     * it with {@link java.util.ServiceLoader} like every other type.
     */
    @AutoService(CompressionCodecProvider.class)
    public static final class Provider implements DeephavenCompressionCodecProvider {
        @Override
        public Codec codec() {
            return INSTANCE;
        }
    }
}

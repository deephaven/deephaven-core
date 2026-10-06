//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import com.github.luben.zstd.ZstdInputStream;
import com.github.luben.zstd.ZstdOutputStream;
import io.grpc.Codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A gRPC {@link Codec} for the {@code zstd} message encoding, backed by zstd-jni streams at zstd's default compression
 * level.
 */
public final class ZstdCodec implements Codec {

    /**
     * The gRPC message encoding name, as it appears in {@code grpc-encoding} and {@code grpc-accept-encoding}.
     */
    public static final String ENCODING = "zstd";

    public static final ZstdCodec INSTANCE = new ZstdCodec();

    /**
     * zstd's default compression level.
     */
    private static final int LEVEL = 3;

    private ZstdCodec() {}

    @Override
    public String getMessageEncoding() {
        return ENCODING;
    }

    @Override
    public OutputStream compress(final OutputStream os) throws IOException {
        return new ZstdOutputStream(os, LEVEL);
    }

    @Override
    public InputStream decompress(final InputStream is) throws IOException {
        return new ZstdInputStream(is);
    }
}

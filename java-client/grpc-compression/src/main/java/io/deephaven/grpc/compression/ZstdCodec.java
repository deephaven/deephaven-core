//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
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
        // gRPC creates and closes each compressing stream within one write, so it needs no finalizer
        return new ZstdOutputStreamNoFinalizer(os, LEVEL);
    }

    @Override
    public InputStream decompress(final InputStream is) throws IOException {
        // gRPC closes each message stream after parsing it, including when the call is cancelled, so it needs no
        // finalizer
        return new ZstdInputStreamNoFinalizer(is);
    }
}

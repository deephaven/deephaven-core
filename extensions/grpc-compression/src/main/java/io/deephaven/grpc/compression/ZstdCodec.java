//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import com.google.auto.service.AutoService;
import io.grpc.Codec;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * A gRPC {@link Codec} for the {@code zstd} message encoding, backed by zstd-jni streams at zstd's default compression
 * level. gRPC opens a stream for every message, so the streams take their buffers from zstd-jni's shared
 * {@link RecyclingBufferPool} rather than allocating new ones each time.
 */
public final class ZstdCodec implements Codec {

    /**
     * The gRPC message encoding name, as it appears in {@code grpc-encoding} and {@code grpc-accept-encoding}.
     */
    public static final String ENCODING = "zstd";

    /**
     * zstd-jni's shared buffer pool. Initializing the pool loads zstd-jni's native library, so a library that cannot
     * load fails when {@link CompressionCodecs} loads this codec, which then leaves zstd out, rather than at the first
     * compressed message. Declared before {@link #INSTANCE}, so that no instance exists without the library.
     */
    private static final BufferPool POOL = RecyclingBufferPool.INSTANCE;

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
        return new ZstdOutputStreamNoFinalizer(os, POOL, LEVEL);
    }

    @Override
    public InputStream decompress(final InputStream is) throws IOException {
        // gRPC closes each message stream after parsing it, including when the call is cancelled, so it needs no
        // finalizer
        return new ZstdInputStreamNoFinalizer(is, POOL);
    }

    /**
     * Supplies {@link ZstdCodec#INSTANCE} as a supported compression type, so that {@link CompressionCodecs} finds it
     * with {@link java.util.ServiceLoader} like every other type.
     */
    @AutoService(CompressionCodecProvider.class)
    public static final class Provider implements CompressionCodecProvider {
        @Override
        public Codec codec() {
            return INSTANCE;
        }
    }
}

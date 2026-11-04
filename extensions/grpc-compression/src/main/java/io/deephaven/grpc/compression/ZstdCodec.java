//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import com.github.luben.zstd.BufferPool;
import com.github.luben.zstd.RecyclingBufferPool;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.github.luben.zstd.ZstdOutputStreamNoFinalizer;
import com.github.luben.zstd.util.Native;
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
     * zstd-jni's shared buffer pool.
     */
    private static final BufferPool POOL = RecyclingBufferPool.INSTANCE;

    public static final ZstdCodec INSTANCE = new ZstdCodec();

    /**
     * zstd's default compression level.
     */
    private static final int LEVEL = 3;

    /**
     * The base-2 logarithm of the largest window that a frame may declare for decompression: 8 MiB, the largest window
     * that zstd's compression levels 1 through 19 use. {@link #LEVEL} uses a window of at most 2 MiB.
     */
    static final int WINDOW_LOG_MAX = 23;

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
        return new ZstdInputStreamNoFinalizer(is, POOL).setLongMax(WINDOW_LOG_MAX);
    }

    /**
     * Supplies {@link ZstdCodec#INSTANCE} as a supported compression type, so that {@link CompressionCodecs} finds it
     * with {@link java.util.ServiceLoader} like every other type.
     */
    @AutoService(CompressionCodecProvider.class)
    public static final class Provider implements DeephavenCompressionCodecProvider {
        @Override
        public Codec codec() {
            // loads zstd-jni's native library now, before ZstdCodec is initialized, so that a library that cannot load
            // fails here with zstd-jni's own error, where CompressionCodecs leaves zstd out, rather than at the first
            // compressed message
            Native.load();
            return INSTANCE;
        }
    }
}

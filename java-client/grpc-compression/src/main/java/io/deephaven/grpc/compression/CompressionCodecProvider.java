//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import io.grpc.Codec;

import java.util.ServiceLoader;

/**
 * Adds a compression type to the ones Deephaven supports for gRPC messages, beyond the built-in {@code zstd},
 * {@code gzip} and {@code snappy}. {@link CompressionCodecs} finds implementations with {@link ServiceLoader} the first
 * time it is used: list the implementation in
 * {@code META-INF/services/io.deephaven.grpc.compression.CompressionCodecProvider}, or annotate it with
 * {@code @AutoService(CompressionCodecProvider.class)}, and put its jar on the classpath of the server and of each Java
 * client that should accept the type. The Java client then accepts it by default.
 * <p>
 * The codec's {@link Codec#getMessageEncoding() encoding name} names the type in a table's {@code BarrageCompression}
 * attribute, in {@code withCompression}, and in gRPC's {@code grpc-encoding} and {@code grpc-accept-encoding} headers.
 * It must be lower-case letters, digits, {@code .}, {@code _}, {@code +} and {@code -}, starting with a letter or
 * digit; must not be {@code identity}; and must differ from every other type's name. Otherwise
 * {@link CompressionCodecs} fails to initialize, and so does the server.
 * <p>
 * The Jetty server both compresses responses and decodes client requests with every codec, so a decompressor must
 * handle malformed input safely. The Netty server registers only {@code gzip}, and never uses a provided codec.
 */
public interface CompressionCodecProvider {

    /**
     * @return the codec for this compression type; called once per process
     */
    Codec codec();
}

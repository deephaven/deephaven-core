//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.grpc.compression;

import io.grpc.Codec;
import org.jetbrains.annotations.NotNull;

import java.util.Locale;

/**
 * A compression type that Deephaven supports for gRPC messages, with the {@link Codec} that implements it.
 * <p>
 * This enum is the only list of supported compression types. {@link CompressionCodecs} registers every constant, so
 * adding a compression type means adding a constant and its codec here.
 */
public enum CompressionType {
    /**
     * Zstandard: compresses nearly as well as {@link #GZIP gzip}, and many times faster.
     */
    ZSTD(ZstdCodec.INSTANCE),
    /**
     * gzip: compresses slightly smaller than {@link #ZSTD zstd} but much more slowly. Most gRPC clients can decode it.
     */
    GZIP(new Codec.Gzip()),
    /**
     * Snappy: the fastest compression type, but the one that saves the least.
     */
    SNAPPY(SnappyCodec.INSTANCE);

    private final Codec codec;

    CompressionType(final Codec codec) {
        this.codec = codec;
    }

    /**
     * @return the name of this compression type in gRPC's {@code grpc-encoding} and {@code grpc-accept-encoding}
     *         headers
     */
    public String encodingName() {
        return codec.getMessageEncoding();
    }

    /**
     * @return the gRPC codec that implements this compression type
     */
    public Codec codec() {
        return codec;
    }

    /**
     * @param encodingName a name from gRPC's {@code grpc-encoding} or {@code grpc-accept-encoding} headers; case and
     *        surrounding whitespace are ignored
     * @return the compression type with that name
     * @throws IllegalArgumentException if no compression type has that name
     */
    public static CompressionType fromEncodingName(@NotNull final String encodingName) {
        final String lower = encodingName.trim().toLowerCase(Locale.ROOT);
        for (final CompressionType type : values()) {
            if (type.encodingName().equals(lower)) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported compression '" + encodingName + "'; supported values are "
                + String.join(", ", CompressionCodecs.SUPPORTED));
    }
}

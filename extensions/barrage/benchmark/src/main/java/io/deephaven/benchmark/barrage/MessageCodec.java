//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.barrage;

import io.deephaven.grpc.compression.SnappyCodec;
import io.deephaven.grpc.compression.ZstdCodec;
import io.grpc.Codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;

/**
 * gRPC message compression, applied to each serialized {@code FlightData} message as a whole, using the same
 * {@link Codec} implementations the Deephaven server and Java client register. The 5-byte gRPC frame prefix is the same
 * size for every configuration and is not counted.
 */
public enum MessageCodec {
    IDENTITY(null),
    /** grpc-java's built-in gzip codec (deflate level 6). */
    GZIP(new Codec.Gzip()),
    /** zstd at its default level 3. */
    ZSTD(ZstdCodec.INSTANCE),
    /** Snappy, framed. */
    SNAPPY(SnappyCodec.INSTANCE);

    private final Codec codec;

    MessageCodec(final Codec codec) {
        this.codec = codec;
    }

    byte[] compress(final byte[] message) {
        if (codec == null) {
            return message;
        }
        final ByteArrayOutputStream out = new ByteArrayOutputStream(message.length / 2 + 64);
        try (final OutputStream compressing = codec.compress(out)) {
            compressing.write(message);
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
        return out.toByteArray();
    }

    byte[] decompress(final byte[] message) {
        if (codec == null) {
            return message;
        }
        try (final InputStream decompressing = codec.decompress(new ByteArrayInputStream(message))) {
            return decompressing.readAllBytes();
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}

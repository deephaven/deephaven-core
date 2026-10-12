//
// Copyright (c) 2016-2026 Deephaven Data Labs and Patent Pending
//
package io.deephaven.benchmark.barrage;

import io.deephaven.grpc.compression.CompressionType;
import io.grpc.Codec;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;

/**
 * gRPC message compression, applied to each serialized {@code FlightData} message as a whole, using the codec of a
 * {@link CompressionType}: the same codec the Deephaven server and Java client register. The 5-byte gRPC frame prefix
 * is the same size for every configuration and is not counted.
 */
final class MessageCodec {
    /** no compression; the baseline */
    static final MessageCodec IDENTITY = new MessageCodec("IDENTITY", null);

    private final String name;
    private final Codec codec;

    private MessageCodec(final String name, final Codec codec) {
        this.name = name;
        this.codec = codec;
    }

    static MessageCodec of(final CompressionType type) {
        return new MessageCodec(type.name(), type.codec());
    }

    /**
     * @return {@link #IDENTITY} followed by every {@link CompressionType}
     */
    static List<MessageCodec> all() {
        final List<MessageCodec> codecs = new ArrayList<>();
        codecs.add(IDENTITY);
        for (final CompressionType type : CompressionType.values()) {
            codecs.add(of(type));
        }
        return codecs;
    }

    @Override
    public String toString() {
        return name;
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
